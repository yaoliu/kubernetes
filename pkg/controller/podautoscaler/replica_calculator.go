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

package podautoscaler

import (
	"fmt"
	"math"
	"time"

	autoscaling "k8s.io/api/autoscaling/v2beta2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	corelisters "k8s.io/client-go/listers/core/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	metricsclient "k8s.io/kubernetes/pkg/controller/podautoscaler/metrics"
)

const (
	// defaultTestingTolerance is default value for calculating when to
	// scale up/scale down.
	defaultTestingTolerance                     = 0.1
	defaultTestingCPUInitializationPeriod       = 2 * time.Minute
	defaultTestingDelayOfInitialReadinessStatus = 10 * time.Second
)

// ReplicaCalculator bundles all needed information to calculate the target amount of replicas
// 副本计算器
type ReplicaCalculator struct {
	metricsClient metricsclient.MetricsClient
	podLister     corelisters.PodLister
	// 全局配置的容忍值
	tolerance float64
	// 默认为5分钟 用于设置Pod的初始化时间，在此时间内的Pod，CPU资源度量值将不会被采纳
	cpuInitializationPeriod time.Duration
	// 等待Pod到达Reday状态的延时 或者说用于设置Pod的准备时间，在此时间内的Pod都被认为未就绪状态，默认30秒
	delayOfInitialReadinessStatus time.Duration
}

// NewReplicaCalculator creates a new ReplicaCalculator and passes all necessary information to the new instance
func NewReplicaCalculator(metricsClient metricsclient.MetricsClient, podLister corelisters.PodLister, tolerance float64, cpuInitializationPeriod, delayOfInitialReadinessStatus time.Duration) *ReplicaCalculator {
	return &ReplicaCalculator{
		metricsClient:                 metricsClient,
		podLister:                     podLister,
		tolerance:                     tolerance,
		cpuInitializationPeriod:       cpuInitializationPeriod,
		delayOfInitialReadinessStatus: delayOfInitialReadinessStatus,
	}
}

// GetResourceReplicas calculates the desired replica count based on a target resource utilization percentage
// of the given resource for pods matching the given selector in the given namespace, and the current replica count
func (c *ReplicaCalculator) GetResourceReplicas(currentReplicas int32, targetUtilization int32, resource v1.ResourceName, namespace string, selector labels.Selector) (replicaCount int32, utilization int32, rawUtilization int64, timestamp time.Time, err error) {
	// 获取pod metrics指标数据
	metrics, timestamp, err := c.metricsClient.GetResourceMetric(resource, namespace, selector)
	if err != nil {
		return 0, 0, 0, time.Time{}, fmt.Errorf("unable to get metrics for resource %s: %v", resource, err)
	}
	// 根据选择器获取pod列表
	podList, err := c.podLister.Pods(namespace).List(selector)
	if err != nil {
		return 0, 0, 0, time.Time{}, fmt.Errorf("unable to get pods while calculating replica count: %v", err)
	}

	itemsLen := len(podList)
	if itemsLen == 0 {
		return 0, 0, 0, time.Time{}, fmt.Errorf("no pods returned by selector while calculating replica count")
	}
	// 对pod进行分组 返回就绪pod数量
	// missingPods为不存在度量指标的pod集合
	// ignoredPods为存储未就绪pod集合
	//
	readyPodCount, ignoredPods, missingPods := groupPods(podList, metrics, resource, c.cpuInitializationPeriod, c.delayOfInitialReadinessStatus)
	// 从mertics指标数据里移除ignoredPods(未就绪及不在范围内的pod集合)
	// 所有被标记了删除时间戳（Pod 正在关闭过程中）的 Pod 和失败的 Pod 都会被忽略。
	removeMetricsForPods(metrics, ignoredPods)
	// resource获取pod.spec.containers.resource.request值 request值为该pod请求某项资源
	// 获取pod设置的resource累加值
	// 比如一个deployment.spec.template.spec.containers[0].resources.requests[cpu] = 200m 如果replicas = 2 那么requests
	// 计算pod中container request 设置的资源之和
	requests, err := calculatePodRequests(podList, resource)
	if err != nil {
		return 0, 0, 0, time.Time{}, err
	}

	if len(metrics) == 0 {
		return 0, 0, 0, time.Time{}, fmt.Errorf("did not receive metrics for any ready pods")
	}
	// usageRatio为当前资源使用率/扩容值 utilization当前资源使用率
	usageRatio, utilization, rawUtilization, err := metricsclient.GetResourceUtilizationRatio(metrics, requests, targetUtilization)
	if err != nil {
		return 0, 0, 0, time.Time{}, err
	}
	// usageRatio > 1.0 代表需要扩容
	// ignoredPods > 0 代表有未就绪pod
	rebalanceIgnored := len(ignoredPods) > 0 && usageRatio > 1.0
	// 如果不需要扩容/pod都已经ready/pod都有mertic
	if !rebalanceIgnored && len(missingPods) == 0 {
		if math.Abs(1.0-usageRatio) <= c.tolerance {
			// return the current replicas if the change would be too small
			// 计算出的扩缩比例接近 1.0 （根据--horizontal-pod-autoscaler-tolerance 参数全局配置的容忍值，默认为 0.1）， 将会放弃本次扩缩
			// 如果更改较小 比如小于容忍度 那么不进行更改 返回当前副本数
			return currentReplicas, utilization, rawUtilization, timestamp, nil
		}

		// if we don't have any unready or missing pods, we can calculate the new replica count now
		// math.Ceil(usageRatio * float64(readyPodCount)) 需要扩容的副本数
		// math.Ceil((当前指标/期望指标) * 当前副本数)
		return int32(math.Ceil(usageRatio * float64(readyPodCount))), utilization, rawUtilization, timestamp, nil
	}
	// 如果有的pod没有metric 根据扩容还是缩容 将这些pod对应的metric进行填充
	// 对missingPods所对应对metric进行设置
	// 如果缺失任何的度量值，我们会更保守地重新计算平均值， 在需要缩小时假设这些 Pod 消耗了目标值的 100%， 在需要放大时假设这些 Pod 消耗了 0% 目标值。 这可以在一定程度上抑制扩缩的幅度
	if len(missingPods) > 0 {
		if usageRatio < 1.0 {
			// on a scale-down, treat missing pods as using 100% of the resource request
			for podName := range missingPods {
				metrics[podName] = metricsclient.PodMetric{Value: requests[podName]}
			}
		} else if usageRatio > 1.0 {
			// on a scale-up, treat missing pods as using 0% of the resource request
			for podName := range missingPods {
				metrics[podName] = metricsclient.PodMetric{Value: 0}
			}
		}
	}
	// 对ignoredPods所对应对metric进行设置
	// 如果需要扩容。将那些未就绪的pod对应的mertic设置未空
	if rebalanceIgnored {
		// on a scale-up, treat unready pods as using 0% of the resource request
		for podName := range ignoredPods {
			metrics[podName] = metricsclient.PodMetric{Value: 0}
		}
	}
	// 把未就绪的 Pod 和缺少指标的 Pod 考虑进来再次计算使用率。 如果新的比率与扩缩方向相反，或者在容忍范围内，则跳过扩缩。 否则，我们使用新的扩缩比例。
	// re-run the utilization calculation with our new numbers
	// 重新计算新的资源使用率
	newUsageRatio, _, _, err := metricsclient.GetResourceUtilizationRatio(metrics, requests, targetUtilization)
	if err != nil {
		return 0, utilization, rawUtilization, time.Time{}, err
	}
	// 伸缩方向不一致 返回当前副本数
	if math.Abs(1.0-newUsageRatio) <= c.tolerance || (usageRatio < 1.0 && newUsageRatio > 1.0) || (usageRatio > 1.0 && newUsageRatio < 1.0) {
		// return the current replicas if the change would be too small,
		// or if the new usage ratio would cause a change in scale direction
		return currentReplicas, utilization, rawUtilization, timestamp, nil
	}
	// metrics 为 pod数量 newUsageRatio为当前资源使用率  相乘就是需要扩容的副本数
	newReplicas := int32(math.Ceil(newUsageRatio * float64(len(metrics))))
	if (newUsageRatio < 1.0 && newReplicas > currentReplicas) || (newUsageRatio > 1.0 && newReplicas < currentReplicas) {
		// return the current replicas if the change of metrics length would cause a change in scale direction
		return currentReplicas, utilization, rawUtilization, timestamp, nil
	}

	// return the result, where the number of replicas considered is
	// however many replicas factored into our calculation
	return newReplicas, utilization, rawUtilization, timestamp, nil
}

// GetRawResourceReplicas calculates the desired replica count based on a target resource utilization (as a raw milli-value)
// for pods matching the given selector in the given namespace, and the current replica count
func (c *ReplicaCalculator) GetRawResourceReplicas(currentReplicas int32, targetUtilization int64, resource v1.ResourceName, namespace string, selector labels.Selector) (replicaCount int32, utilization int64, timestamp time.Time, err error) {
	// 根据resourceName、namespace、selector选择器 , 获取pod metrics指标数据
	metrics, timestamp, err := c.metricsClient.GetResourceMetric(resource, namespace, selector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get metrics for resource %s: %v", resource, err)
	}
	// 核心计算副本数实现
	replicaCount, utilization, err = c.calcPlainMetricReplicas(metrics, currentReplicas, targetUtilization, namespace, selector, resource)
	return replicaCount, utilization, timestamp, err
}

// GetMetricReplicas calculates the desired replica count based on a target metric utilization
// (as a milli-value) for pods matching the given selector in the given namespace, and the
// current replica count
func (c *ReplicaCalculator) GetMetricReplicas(currentReplicas int32, targetUtilization int64, metricName string, namespace string, selector labels.Selector, metricSelector labels.Selector) (replicaCount int32, utilization int64, timestamp time.Time, err error) {
	// GetRawMetric
	metrics, timestamp, err := c.metricsClient.GetRawMetric(metricName, namespace, selector, metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get metric %s: %v", metricName, err)
	}
	// 核心计算副本数实现
	replicaCount, utilization, err = c.calcPlainMetricReplicas(metrics, currentReplicas, targetUtilization, namespace, selector, v1.ResourceName(""))
	return replicaCount, utilization, timestamp, err
}

// calcPlainMetricReplicas calculates the desired replicas for plain (i.e. non-utilization percentage) metrics.
// 核心计算副本数实现
func (c *ReplicaCalculator) calcPlainMetricReplicas(metrics metricsclient.PodMetricsInfo, currentReplicas int32, targetUtilization int64, namespace string, selector labels.Selector, resource v1.ResourceName) (replicaCount int32, utilization int64, err error) {
	// 根据选择获取pod列表
	podList, err := c.podLister.Pods(namespace).List(selector)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to get pods while calculating replica count: %v", err)
	}

	if len(podList) == 0 {
		return 0, 0, fmt.Errorf("no pods returned by selector while calculating replica count")
	}
	// 将pod进行分组
	// readyPodCount 已就绪pod数量，
	// ignoredPods 未就绪及不在范围内的pod集合
	// missingPods 不存在mertic指标数据的pod集合
	readyPodCount, ignoredPods, missingPods := groupPods(podList, metrics, resource, c.cpuInitializationPeriod, c.delayOfInitialReadinessStatus)
	// 从mertics指标数据里移除ignoredPods(未就绪及不在范围内的pod集合)
	removeMetricsForPods(metrics, ignoredPods)

	if len(metrics) == 0 {
		return 0, 0, fmt.Errorf("did not receive metrics for any ready pods")
	}
	// 获取资源使用率 当前利用率
	usageRatio, utilization := metricsclient.GetMetricUtilizationRatio(metrics, targetUtilization)

	rebalanceIgnored := len(ignoredPods) > 0 && usageRatio > 1.0

	if !rebalanceIgnored && len(missingPods) == 0 {
		if math.Abs(1.0-usageRatio) <= c.tolerance {
			// return the current replicas if the change would be too small
			return currentReplicas, utilization, nil
		}

		// if we don't have any unready or missing pods, we can calculate the new replica count now
		return int32(math.Ceil(usageRatio * float64(readyPodCount))), utilization, nil
	}

	if len(missingPods) > 0 {
		if usageRatio < 1.0 {
			// on a scale-down, treat missing pods as using 100% of the resource request
			for podName := range missingPods {
				metrics[podName] = metricsclient.PodMetric{Value: targetUtilization}
			}
		} else {
			// on a scale-up, treat missing pods as using 0% of the resource request
			for podName := range missingPods {
				metrics[podName] = metricsclient.PodMetric{Value: 0}
			}
		}
	}

	if rebalanceIgnored {
		// on a scale-up, treat unready pods as using 0% of the resource request
		for podName := range ignoredPods {
			metrics[podName] = metricsclient.PodMetric{Value: 0}
		}
	}

	// re-run the utilization calculation with our new numbers
	//重新进行计算资源利用率
	newUsageRatio, _ := metricsclient.GetMetricUtilizationRatio(metrics, targetUtilization)

	if math.Abs(1.0-newUsageRatio) <= c.tolerance || (usageRatio < 1.0 && newUsageRatio > 1.0) || (usageRatio > 1.0 && newUsageRatio < 1.0) {
		// return the current replicas if the change would be too small,
		// or if the new usage ratio would cause a change in scale direction
		return currentReplicas, utilization, nil
	}

	// return the result, where the number of replicas considered is
	// however many replicas factored into our calculation
	return int32(math.Ceil(newUsageRatio * float64(len(metrics)))), utilization, nil
}

// GetObjectMetricReplicas calculates the desired replica count based on a target metric utilization (as a milli-value)
// for the given object in the given namespace, and the current replica count.
func (c *ReplicaCalculator) GetObjectMetricReplicas(currentReplicas int32, targetUtilization int64, metricName string, namespace string, objectRef *autoscaling.CrossVersionObjectReference, selector labels.Selector, metricSelector labels.Selector) (replicaCount int32, utilization int64, timestamp time.Time, err error) {
	utilization, timestamp, err = c.metricsClient.GetObjectMetric(metricName, namespace, objectRef, metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get metric %s: %v on %s %s/%s", metricName, objectRef.Kind, namespace, objectRef.Name, err)
	}
	// 当前使用率/目标使用率
	usageRatio := float64(utilization) / float64(targetUtilization)
	// 计算副本数
	replicaCount, timestamp, err = c.getUsageRatioReplicaCount(currentReplicas, usageRatio, namespace, selector)
	return replicaCount, utilization, timestamp, err
}

// getUsageRatioReplicaCount calculates the desired replica count based on usageRatio and ready pods count.
// For currentReplicas=0 doesn't take into account ready pods count and tolerance to support scaling to zero pods.
// 根据当前利用率获取扩容的副本数量
func (c *ReplicaCalculator) getUsageRatioReplicaCount(currentReplicas int32, usageRatio float64, namespace string, selector labels.Selector) (replicaCount int32, timestamp time.Time, err error) {
	if currentReplicas != 0 {
		// 如果小于tolerance 放弃本次扩容
		if math.Abs(1.0-usageRatio) <= c.tolerance {
			// return the current replicas if the change would be too small
			return currentReplicas, timestamp, nil
		}
		readyPodCount := int64(0)
		// 已经就绪的pod数量
		readyPodCount, err = c.getReadyPodsCount(namespace, selector)
		if err != nil {
			return 0, time.Time{}, fmt.Errorf("unable to calculate ready pods: %s", err)
		}
		// 计算副本数
		replicaCount = int32(math.Ceil(usageRatio * float64(readyPodCount)))
	} else {
		// Scale to zero or n pods depending on usageRatio
		replicaCount = int32(math.Ceil(usageRatio))
	}

	return replicaCount, timestamp, err
}

// GetObjectPerPodMetricReplicas calculates the desired replica count based on a target metric utilization (as a milli-value)
// for the given object in the given namespace, and the current replica count.
func (c *ReplicaCalculator) GetObjectPerPodMetricReplicas(statusReplicas int32, targetAverageUtilization int64, metricName string, namespace string, objectRef *autoscaling.CrossVersionObjectReference, metricSelector labels.Selector) (replicaCount int32, utilization int64, timestamp time.Time, err error) {
	utilization, timestamp, err = c.metricsClient.GetObjectMetric(metricName, namespace, objectRef, metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get metric %s: %v on %s %s/%s", metricName, objectRef.Kind, namespace, objectRef.Name, err)
	}

	replicaCount = statusReplicas
	usageRatio := float64(utilization) / (float64(targetAverageUtilization) * float64(replicaCount))
	if math.Abs(1.0-usageRatio) > c.tolerance {
		// update number of replicas if change is large enough
		replicaCount = int32(math.Ceil(float64(utilization) / float64(targetAverageUtilization)))
	}
	utilization = int64(math.Ceil(float64(utilization) / float64(statusReplicas)))
	return replicaCount, utilization, timestamp, nil
}

// @TODO(mattjmcnaughton) Many different functions in this module use variations
// of this function. Make this function generic, so we don't repeat the same
// logic in multiple places.
func (c *ReplicaCalculator) getReadyPodsCount(namespace string, selector labels.Selector) (int64, error) {
	podList, err := c.podLister.Pods(namespace).List(selector)
	if err != nil {
		return 0, fmt.Errorf("unable to get pods while calculating replica count: %v", err)
	}

	if len(podList) == 0 {
		return 0, fmt.Errorf("no pods returned by selector while calculating replica count")
	}

	readyPodCount := 0

	for _, pod := range podList {
		if pod.Status.Phase == v1.PodRunning && podutil.IsPodReady(pod) {
			readyPodCount++
		}
	}

	return int64(readyPodCount), nil
}

// GetExternalMetricReplicas calculates the desired replica count based on a
// target metric value (as a milli-value) for the external metric in the given
// namespace, and the current replica count.
func (c *ReplicaCalculator) GetExternalMetricReplicas(currentReplicas int32, targetUtilization int64, metricName, namespace string, metricSelector *metav1.LabelSelector, podSelector labels.Selector) (replicaCount int32, utilization int64, timestamp time.Time, err error) {
	metricLabelSelector, err := metav1.LabelSelectorAsSelector(metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	metrics, timestamp, err := c.metricsClient.GetExternalMetric(metricName, namespace, metricLabelSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get external metric %s/%s/%+v: %s", namespace, metricName, metricSelector, err)
	}
	utilization = 0
	for _, val := range metrics {
		utilization = utilization + val
	}

	usageRatio := float64(utilization) / float64(targetUtilization)
	replicaCount, timestamp, err = c.getUsageRatioReplicaCount(currentReplicas, usageRatio, namespace, podSelector)
	return replicaCount, utilization, timestamp, err
}

// GetExternalPerPodMetricReplicas calculates the desired replica count based on a
// target metric value per pod (as a milli-value) for the external metric in the
// given namespace, and the current replica count.
func (c *ReplicaCalculator) GetExternalPerPodMetricReplicas(statusReplicas int32, targetUtilizationPerPod int64, metricName, namespace string, metricSelector *metav1.LabelSelector) (replicaCount int32, utilization int64, timestamp time.Time, err error) {
	metricLabelSelector, err := metav1.LabelSelectorAsSelector(metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	metrics, timestamp, err := c.metricsClient.GetExternalMetric(metricName, namespace, metricLabelSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get external metric %s/%s/%+v: %s", namespace, metricName, metricSelector, err)
	}
	utilization = 0
	for _, val := range metrics {
		utilization = utilization + val
	}

	replicaCount = statusReplicas
	usageRatio := float64(utilization) / (float64(targetUtilizationPerPod) * float64(replicaCount))
	if math.Abs(1.0-usageRatio) > c.tolerance {
		// update number of replicas if the change is large enough
		replicaCount = int32(math.Ceil(float64(utilization) / float64(targetUtilizationPerPod)))
	}
	utilization = int64(math.Ceil(float64(utilization) / float64(statusReplicas)))
	return replicaCount, utilization, timestamp, nil
}

func groupPods(pods []*v1.Pod, metrics metricsclient.PodMetricsInfo, resource v1.ResourceName, cpuInitializationPeriod, delayOfInitialReadinessStatus time.Duration) (readyPodCount int, ignoredPods sets.String, missingPods sets.String) {
	// 对pod进行分组 返回就绪pod数量
	// missingPods为不存在度量指标的pod集合
	// ignoredPods为存储未就绪pod集合
	missingPods = sets.NewString()
	ignoredPods = sets.NewString()
	for _, pod := range pods {
		// 标记删除时间戳(处于删除中)、失败的Pod都会被忽略
		if pod.DeletionTimestamp != nil || pod.Status.Phase == v1.PodFailed {
			continue
		}
		// Pending pods are ignored.
		// 未就绪状态的pod加入ignoredPods
		if pod.Status.Phase == v1.PodPending {

			ignoredPods.Insert(pod.Name)
			continue
		}
		// Pods missing metrics.
		// 不存在度量指标的pod加入missingPods
		metric, found := metrics[pod.Name]
		if !found {
			missingPods.Insert(pod.Name)
			continue
		}
		// Unready pods are ignored.
		// 判断如果resource == cpu 检查pod是否已经处于reday状态 如果没有处于ready状态 并且pod还没创建出来  这个pod需要忽略 加入ignoredPod里
		// 当使用 CPU 指标来扩缩时，任何还未就绪（例如还在初始化）状态的 Pod 或 最近的指标 度量值采集于就绪状态前的 Pod，该 Pod 也会被搁置
		if resource == v1.ResourceCPU {
			var ignorePod bool
			_, condition := podutil.GetPodCondition(&pod.Status, v1.PodReady)
			if condition == nil || pod.Status.StartTime == nil {
				ignorePod = true
			} else {
				// Pod still within possible initialisation period.
				if pod.Status.StartTime.Add(cpuInitializationPeriod).After(time.Now()) {
					// Ignore sample if pod is unready or one window of metric wasn't collected since last state transition.
					ignorePod = condition.Status == v1.ConditionFalse || metric.Timestamp.Before(condition.LastTransitionTime.Time.Add(metric.Window))
				} else {
					// Ignore metric if pod is unready and it has never been ready.
					ignorePod = condition.Status == v1.ConditionFalse && pod.Status.StartTime.Add(delayOfInitialReadinessStatus).After(condition.LastTransitionTime.Time)
				}
			}
			if ignorePod {
				ignoredPods.Insert(pod.Name)
				continue
			}
		}
		readyPodCount++
	}
	return
}

func calculatePodRequests(pods []*v1.Pod, resource v1.ResourceName) (map[string]int64, error) {
	requests := make(map[string]int64, len(pods))
	// 遍历所有pod及pod.spec.Containers 将匹配的resource对应的值进行累加
	for _, pod := range pods {
		podSum := int64(0)
		for _, container := range pod.Spec.Containers {
			if containerRequest, ok := container.Resources.Requests[resource]; ok {
				podSum += containerRequest.MilliValue()
			} else {
				return nil, fmt.Errorf("missing request for %s", resource)
			}
		}
		requests[pod.Name] = podSum
	}
	return requests, nil
}

func removeMetricsForPods(metrics metricsclient.PodMetricsInfo, pods sets.String) {
	for _, pod := range pods.UnsortedList() {
		delete(metrics, pod)
	}
}
