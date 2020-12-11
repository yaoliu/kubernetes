/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package app implements a server that runs a set of active
// components.  This includes replication controllers, service endpoints and
// nodes.
//
package app

import (
	"net/http"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/scale"
	"k8s.io/kubernetes/pkg/controller/podautoscaler"
	"k8s.io/kubernetes/pkg/controller/podautoscaler/metrics"

	resourceclient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/custom_metrics"
	"k8s.io/metrics/pkg/client/external_metrics"
)

func startHPAController(ctx ControllerContext) (http.Handler, bool, error) {
	if !ctx.AvailableResources[schema.GroupVersionResource{Group: "autoscaling", Version: "v1", Resource: "horizontalpodautoscalers"}] {
		return nil, false, nil
	}
	// 新版本使用Restful Metric Api收集度量指标数据进行伸缩
	if ctx.ComponentConfig.HPAController.HorizontalPodAutoscalerUseRESTClients {
		// use the new-style clients if support for custom metrics is enabled
		return startHPAControllerWithRESTClient(ctx)
	}
	// 老版本使用Heapster收集度量指标数据进行伸缩
	return startHPAControllerWithLegacyClient(ctx)
}

func startHPAControllerWithRESTClient(ctx ControllerContext) (http.Handler, bool, error) {
	clientConfig := ctx.ClientBuilder.ConfigOrDie("horizontal-pod-autoscaler")
	hpaClient := ctx.ClientBuilder.ClientOrDie("horizontal-pod-autoscaler")

	apiVersionsGetter := custom_metrics.NewAvailableAPIsGetter(hpaClient.Discovery())
	// invalidate the discovery information roughly once per resync interval our API
	// information is *at most* two resync intervals old.
	go custom_metrics.PeriodicallyInvalidate(
		apiVersionsGetter,
		ctx.ComponentConfig.HPAController.HorizontalPodAutoscalerSyncPeriod.Duration,
		ctx.Stop)
	// 创建访问MetricServer的Client
	metricsClient := metrics.NewRESTMetricsClient(
		resourceclient.NewForConfigOrDie(clientConfig),
		custom_metrics.NewForConfig(clientConfig, ctx.RESTMapper, apiVersionsGetter),
		external_metrics.NewForConfigOrDie(clientConfig),
	)
	return startHPAControllerWithMetricsClient(ctx, metricsClient)
}

func startHPAControllerWithLegacyClient(ctx ControllerContext) (http.Handler, bool, error) {
	// 获取访问APIServer的client
	hpaClient := ctx.ClientBuilder.ClientOrDie("horizontal-pod-autoscaler")
	// 创建访问HeapsterServer的Client
	metricsClient := metrics.NewHeapsterMetricsClient(
		hpaClient,
		metrics.DefaultHeapsterNamespace,
		metrics.DefaultHeapsterScheme,
		metrics.DefaultHeapsterService,
		metrics.DefaultHeapsterPort,
	)
	return startHPAControllerWithMetricsClient(ctx, metricsClient)
}

func startHPAControllerWithMetricsClient(ctx ControllerContext, metricsClient metrics.MetricsClient) (http.Handler, bool, error) {
	// 无论新版本/旧版本都会调用此方法
	hpaClient := ctx.ClientBuilder.ClientOrDie("horizontal-pod-autoscaler")
	hpaClientConfig := ctx.ClientBuilder.ConfigOrDie("horizontal-pod-autoscaler")

	// we don't use cached discovery because DiscoveryScaleKindResolver does its own caching,
	// so we want to re-fetch every time when we actually ask for it
	scaleKindResolver := scale.NewDiscoveryScaleKindResolver(hpaClient.Discovery())
	scaleClient, err := scale.NewForConfig(hpaClientConfig, ctx.RESTMapper, dynamic.LegacyAPIPathResolverFunc, scaleKindResolver)
	if err != nil {
		return nil, false, err
	}
	// 创建 hpa controller并且运行
	go podautoscaler.NewHorizontalController(
		hpaClient.CoreV1(),
		scaleClient,
		hpaClient.AutoscalingV1(),
		ctx.RESTMapper,
		metricsClient,
		ctx.InformerFactory.Autoscaling().V1().HorizontalPodAutoscalers(),
		ctx.InformerFactory.Core().V1().Pods(),
		// 周期性检测 默认为30秒
		ctx.ComponentConfig.HPAController.HorizontalPodAutoscalerSyncPeriod.Duration,
		// 缩容冷却时间 表示从上一次缩容结束后，多久以后可以再次执行扩容，默认时间为5分钟
		ctx.ComponentConfig.HPAController.HorizontalPodAutoscalerDownscaleStabilizationWindow.Duration,
		// 全局配置的容忍值
		ctx.ComponentConfig.HPAController.HorizontalPodAutoscalerTolerance,
		// 默认为5分钟 用于设置Pod的初始化时间，在此时间内的Pod，CPU资源度量值将不会被采纳
		ctx.ComponentConfig.HPAController.HorizontalPodAutoscalerCPUInitializationPeriod.Duration,
		// 等待Pod到达Reday状态的延时 或者说用于设置Pod的准备时间，在此时间内的Pod都被认为未就绪状态，默认30秒
		ctx.ComponentConfig.HPAController.HorizontalPodAutoscalerInitialReadinessDelay.Duration,
	).Run(ctx.Stop)
	return nil, true, nil
}
