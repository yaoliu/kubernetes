/*
Copyright 2017 The Kubernetes Authors.

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

// The Controller sets tainted annotations on nodes.
// Tainted nodes should not be used for new work loads and
// some effort should be given to getting existing work
// loads off of tainted nodes.

package nodelifecycle

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	coordv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	appsv1informers "k8s.io/client-go/informers/apps/v1"
	coordinformers "k8s.io/client-go/informers/coordination/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	coordlisters "k8s.io/client-go/listers/coordination/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/metrics/prometheus/ratelimiter"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/nodelifecycle/scheduler"
	nodeutil "k8s.io/kubernetes/pkg/controller/util/node"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	kubeletapis "k8s.io/kubernetes/pkg/kubelet/apis"
	utilnode "k8s.io/kubernetes/pkg/util/node"
	taintutils "k8s.io/kubernetes/pkg/util/taints"
)

func init() {
	// Register prometheus metrics
	// 注册上报metrics
	Register()
}

var (
	// UnreachableTaintTemplate is the taint for when a node becomes unreachable.
	UnreachableTaintTemplate = &v1.Taint{
		Key:    v1.TaintNodeUnreachable,
		Effect: v1.TaintEffectNoExecute,
	}

	// NotReadyTaintTemplate is the taint for when a node is not ready for
	// executing pods
	// 未就绪状态污点模版
	NotReadyTaintTemplate = &v1.Taint{
		Key:    v1.TaintNodeNotReady,
		Effect: v1.TaintEffectNoExecute,
	}

	// map {NodeConditionType: {ConditionStatus: TaintKey}}
	// represents which NodeConditionType under which ConditionStatus should be
	// tainted with which TaintKey
	// for certain NodeConditionType, there are multiple {ConditionStatus,TaintKey} pairs
	// 根据node.condition.type和node.condition.status 来确定添加那个污点
	nodeConditionToTaintKeyStatusMap = map[v1.NodeConditionType]map[v1.ConditionStatus]string{
		v1.NodeReady: {
			v1.ConditionFalse:   v1.TaintNodeNotReady,
			v1.ConditionUnknown: v1.TaintNodeUnreachable,
		},
		// 内存
		v1.NodeMemoryPressure: {
			v1.ConditionTrue: v1.TaintNodeMemoryPressure,
		},
		// 磁盘
		v1.NodeDiskPressure: {
			v1.ConditionTrue: v1.TaintNodeDiskPressure,
		},
		// 网络
		v1.NodeNetworkUnavailable: {
			v1.ConditionTrue: v1.TaintNodeNetworkUnavailable,
		},
		// PID
		v1.NodePIDPressure: {
			v1.ConditionTrue: v1.TaintNodePIDPressure,
		},
	}

	taintKeyToNodeConditionMap = map[string]v1.NodeConditionType{
		v1.TaintNodeNotReady:           v1.NodeReady,
		v1.TaintNodeUnreachable:        v1.NodeReady,
		v1.TaintNodeNetworkUnavailable: v1.NodeNetworkUnavailable,
		v1.TaintNodeMemoryPressure:     v1.NodeMemoryPressure,
		v1.TaintNodeDiskPressure:       v1.NodeDiskPressure,
		v1.TaintNodePIDPressure:        v1.NodePIDPressure,
	}
)

// ZoneState is the state of a given zone.
type ZoneState string

const (
	// Zone分类
	stateInitial           = ZoneState("Initial")           // 刚完成初始化 加入集群
	stateNormal            = ZoneState("Normal")            // 正常状态
	stateFullDisruption    = ZoneState("FullDisruption")    // 将处于notReady的加入该zone
	statePartialDisruption = ZoneState("PartialDisruption") // 该zone中部分node notReady，此时已经超过了unhealthyZoneThreshold设置的阈值
)

const (
	// The amount of time the nodecontroller should sleep between retrying node health updates
	retrySleepTime   = 20 * time.Millisecond
	nodeNameKeyIndex = "spec.nodeName"
	// podUpdateWorkerSizes assumes that in most cases pod will be handled by monitorNodeHealth pass.
	// Pod update workers will only handle lagging cache pods. 4 workers should be enough.
	podUpdateWorkerSize = 4
)

// labelReconcileInfo lists Node labels to reconcile, and how to reconcile them.
// primaryKey and secondaryKey are keys of labels to reconcile.
//   - If both keys exist, but their values don't match. Use the value from the
//   primaryKey as the source of truth to reconcile.
//   - If ensureSecondaryExists is true, and the secondaryKey does not
//   exist, secondaryKey will be added with the value of the primaryKey.
var labelReconcileInfo = []struct {
	primaryKey            string
	secondaryKey          string
	ensureSecondaryExists bool
}{
	{
		// Reconcile the beta and the stable OS label using the stable label as the source of truth.
		// TODO(#89477): no earlier than 1.22: drop the beta labels if they differ from the GA labels
		primaryKey:            v1.LabelOSStable,
		secondaryKey:          kubeletapis.LabelOS,
		ensureSecondaryExists: true,
	},
	{
		// Reconcile the beta and the stable arch label using the stable label as the source of truth.
		// TODO(#89477): no earlier than 1.22: drop the beta labels if they differ from the GA labels
		primaryKey:            v1.LabelArchStable,
		secondaryKey:          kubeletapis.LabelArch,
		ensureSecondaryExists: true,
	},
}

// node心跳数据 由每个node(kubelet)进行上报
type nodeHealthData struct {
	// 上次心跳探测时间
	probeTimestamp           metav1.Time
	readyTransitionTimestamp metav1.Time
	// node status
	status *v1.NodeStatus
	// node 租约对象
	lease *coordv1.Lease
}

func (n *nodeHealthData) deepCopy() *nodeHealthData {
	if n == nil {
		return nil
	}
	return &nodeHealthData{
		probeTimestamp:           n.probeTimestamp,
		readyTransitionTimestamp: n.readyTransitionTimestamp,
		status:                   n.status.DeepCopy(),
		lease:                    n.lease.DeepCopy(),
	}
}

// 用来存每个node心跳数据(node status && node lease) 线程安全
type nodeHealthMap struct {
	lock        sync.RWMutex
	nodeHealths map[string]*nodeHealthData
}

func newNodeHealthMap() *nodeHealthMap {
	return &nodeHealthMap{
		nodeHealths: make(map[string]*nodeHealthData),
	}
}

// getDeepCopy - returns copy of node health data.
// It prevents data being changed after retrieving it from the map.
func (n *nodeHealthMap) getDeepCopy(name string) *nodeHealthData {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.nodeHealths[name].deepCopy()
}

func (n *nodeHealthMap) set(name string, data *nodeHealthData) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.nodeHealths[name] = data
}

type podUpdateItem struct {
	namespace string
	name      string
}

type evictionStatus int

const (
	unmarked = iota
	toBeEvicted
	evicted
)

// nodeEvictionMap stores evictionStatus data for each node.
type nodeEvictionMap struct {
	lock          sync.Mutex
	nodeEvictions map[string]evictionStatus
}

func newNodeEvictionMap() *nodeEvictionMap {
	return &nodeEvictionMap{
		nodeEvictions: make(map[string]evictionStatus),
	}
}

func (n *nodeEvictionMap) registerNode(nodeName string) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.nodeEvictions[nodeName] = unmarked
}

func (n *nodeEvictionMap) unregisterNode(nodeName string) {
	n.lock.Lock()
	defer n.lock.Unlock()
	delete(n.nodeEvictions, nodeName)
}

func (n *nodeEvictionMap) setStatus(nodeName string, status evictionStatus) bool {
	n.lock.Lock()
	defer n.lock.Unlock()
	if _, exists := n.nodeEvictions[nodeName]; !exists {
		return false
	}
	n.nodeEvictions[nodeName] = status
	return true
}

func (n *nodeEvictionMap) getStatus(nodeName string) (evictionStatus, bool) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if _, exists := n.nodeEvictions[nodeName]; !exists {
		return unmarked, false
	}
	return n.nodeEvictions[nodeName], true
}

// Controller is the controller that manages node's life cycle.
type Controller struct {
	// 用于驱逐node上的Pod 完成pod驱逐任务
	// 运行在node上的pod 如果没有配置容忍(NoExecute) 那么如果这个node设置了污点(taint) 那么这个Pod会被驱逐
	// 如果未开启taintManager 已经运行的Pod不会被驱逐 新创建的Pod如果配置了容忍 才能调度到这个node上
	// runTaintManager用来标记是否运行
	taintManager *scheduler.NoExecuteTaintManager
	// 用于获取pod元数据
	podLister corelisters.PodLister
	// 用于判断pod是否同步过到cache
	podInformerSynced cache.InformerSynced
	// 用于访问apiserver的client
	kubeClient clientset.Interface

	// This timestamp is to be used instead of LastProbeTime stored in Condition. We do this
	// to avoid the problem with time skew across the cluster.
	now func() metav1.Time
	// 计算PartialDisruption zone下node驱逐速率
	enterPartialDisruptionFunc func(nodeNum int) float32
	// 计算FullDisruption zone下node驱逐速率
	enterFullDisruptionFunc func(nodeNum int) float32
	// 计算node所属zone及未就绪node数量
	computeZoneStateFunc func(nodeConditions []*v1.NodeCondition) (int, ZoneState)
	// 存node
	knownNodeSet map[string]*v1.Node
	// per Node map storing last observed health together with a local time when it was observed.
	// 记录node最近一次心跳信息
	nodeHealthMap *nodeHealthMap

	// evictorLock protects zonePodEvictor and zoneNoExecuteTainter.
	// TODO(#83954): API calls shouldn't be executed under the lock.
	evictorLock     sync.Mutex
	nodeEvictionMap *nodeEvictionMap
	// workers that evicts pods from unresponsive nodes.
	// 用来存放有异常的node 如果未开启taintManager monitorNodeHealth()作为生产者将node加入到此队列
	zonePodEvictor map[string]*scheduler.RateLimitedTimedQueue
	// workers that are responsible for tainting nodes.
	// 用来存放有异常的node 如果开启taintManager monitorNodeHealth()作为生产者将node加入到此队列
	zoneNoExecuteTainter map[string]*scheduler.RateLimitedTimedQueue

	nodesToRetry sync.Map
	// 将node划分到不同的Zone里
	zoneStates map[string]ZoneState
	// 用于获取daemonSet元数据
	daemonSetStore appsv1listers.DaemonSetLister
	// 记录daemonset同步cache状态
	daemonSetInformerSynced cache.InformerSynced
	// 获取node租约元数据
	leaseLister         coordlisters.LeaseLister
	leaseInformerSynced cache.InformerSynced
	// 用于获取node元数据
	nodeLister         corelisters.NodeLister
	nodeInformerSynced cache.InformerSynced
	// 根据nodename获取pod
	getPodsAssignedToNode func(nodeName string) ([]*v1.Pod, error)
	// 事件记录器
	recorder record.EventRecorder

	// Value controlling Controller monitoring period, i.e. how often does Controller
	// check node health signal posted from kubelet. This value should be lower than
	// nodeMonitorGracePeriod.
	// TODO: Change node health monitor to watch based.
	// 初始化传进来的参数
	nodeMonitorPeriod time.Duration

	// When node is just created, e.g. cluster bootstrap or node creation, we give
	// a longer grace period.
	nodeStartupGracePeriod time.Duration

	// Controller will not proactively sync node health, but will monitor node
	// health signal updated from kubelet. There are 2 kinds of node healthiness
	// signals: NodeStatus and NodeLease. NodeLease signal is generated only when
	// NodeLease feature is enabled. If it doesn't receive update for this amount
	// of time, it will start posting "NodeReady==ConditionUnknown". The amount of
	// time before which Controller start evicting pods is controlled via flag
	// 'pod-eviction-timeout'.
	// Note: be cautious when changing the constant, it must work with
	// nodeStatusUpdateFrequency in kubelet and renewInterval in NodeLease
	// controller. The node health signal update frequency is the minimal of the
	// two.
	// There are several constraints:
	// 1. nodeMonitorGracePeriod must be N times more than  the node health signal
	//    update frequency, where N means number of retries allowed for kubelet to
	//    post node status/lease. It is pointless to make nodeMonitorGracePeriod
	//    be less than the node health signal update frequency, since there will
	//    only be fresh values from Kubelet at an interval of node health signal
	//    update frequency. The constant must be less than podEvictionTimeout.
	// 2. nodeMonitorGracePeriod can't be too large for user experience - larger
	//    value takes longer for user to see up-to-date node health.
	nodeMonitorGracePeriod time.Duration

	podEvictionTimeout time.Duration
	// zone state = normal 的驱逐速率 默认为0.1 即每个10s清除一个node
	evictionLimiterQPS          float32
	secondaryEvictionLimiterQPS float32
	largeClusterThreshold       int32
	unhealthyZoneThreshold      float32

	// if set to true Controller will start TaintManager that will evict Pods from
	// tainted nodes, if they're not tolerated.
	runTaintManager bool

	nodeUpdateQueue workqueue.Interface
	podUpdateQueue  workqueue.RateLimitingInterface
}

// NewNodeLifecycleController returns a new taint controller.
func NewNodeLifecycleController(
	leaseInformer coordinformers.LeaseInformer,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
	daemonSetInformer appsv1informers.DaemonSetInformer,
	kubeClient clientset.Interface,
	nodeMonitorPeriod time.Duration,
	nodeStartupGracePeriod time.Duration,
	nodeMonitorGracePeriod time.Duration,
	podEvictionTimeout time.Duration,
	evictionLimiterQPS float32,
	secondaryEvictionLimiterQPS float32,
	largeClusterThreshold int32,
	unhealthyZoneThreshold float32,
	runTaintManager bool,
) (*Controller, error) {

	if kubeClient == nil {
		klog.Fatalf("kubeClient is nil when starting Controller")
	}
	// 创建事件管理器
	eventBroadcaster := record.NewBroadcaster()
	// 创建事件收集器
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "node-controller"})
	// 设置事件上报到klog
	eventBroadcaster.StartStructuredLogging(0)
	klog.Infof("Sending events to api server.")
	// 设置事件上报到Api Server
	eventBroadcaster.StartRecordingToSink(
		&v1core.EventSinkImpl{
			Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
		})

	if kubeClient.CoreV1().RESTClient().GetRateLimiter() != nil {
		ratelimiter.RegisterMetricAndTrackRateLimiterUsage("node_lifecycle_controller", kubeClient.CoreV1().RESTClient().GetRateLimiter())
	}
	// 初始化controller
	nc := &Controller{
		kubeClient:                  kubeClient,
		now:                         metav1.Now,
		knownNodeSet:                make(map[string]*v1.Node),
		nodeHealthMap:               newNodeHealthMap(),
		nodeEvictionMap:             newNodeEvictionMap(),
		recorder:                    recorder,
		nodeMonitorPeriod:           nodeMonitorPeriod,
		nodeStartupGracePeriod:      nodeStartupGracePeriod,
		nodeMonitorGracePeriod:      nodeMonitorGracePeriod,
		zonePodEvictor:              make(map[string]*scheduler.RateLimitedTimedQueue),
		zoneNoExecuteTainter:        make(map[string]*scheduler.RateLimitedTimedQueue),
		nodesToRetry:                sync.Map{},
		zoneStates:                  make(map[string]ZoneState),
		podEvictionTimeout:          podEvictionTimeout,
		evictionLimiterQPS:          evictionLimiterQPS,
		secondaryEvictionLimiterQPS: secondaryEvictionLimiterQPS,
		largeClusterThreshold:       largeClusterThreshold,
		unhealthyZoneThreshold:      unhealthyZoneThreshold,
		runTaintManager:             runTaintManager,
		nodeUpdateQueue:             workqueue.NewNamed("node_lifecycle_controller"),
		podUpdateQueue:              workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "node_lifecycle_controller_pods"),
	}
	// 计算PartialDisruption zone下node驱逐速率
	nc.enterPartialDisruptionFunc = nc.ReducedQPSFunc
	// 计算FullDisruption zone下node驱逐速率
	nc.enterFullDisruptionFunc = nc.HealthyQPSFunc
	// 计算node所属zone及未就绪zone的数量
	nc.computeZoneStateFunc = nc.ComputeZoneState

	// 监听watch pod add/update/delete等事件 并且调用对应事件注册的函数
	// 如果开启了TaintManager Pod也要添加到对应的TaintManager.podUpdateQueue队列里
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			nc.podUpdated(nil, pod)
			if nc.taintManager != nil {
				nc.taintManager.PodUpdated(nil, pod)
			}
		},
		UpdateFunc: func(prev, obj interface{}) {
			prevPod := prev.(*v1.Pod)
			newPod := obj.(*v1.Pod)
			nc.podUpdated(prevPod, newPod)
			//
			if nc.taintManager != nil {
				nc.taintManager.PodUpdated(prevPod, newPod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod, isPod := obj.(*v1.Pod)
			// We can get DeletedFinalStateUnknown instead of *v1.Pod here and we need to handle that correctly.
			if !isPod {
				deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					klog.Errorf("Received unexpected object: %v", obj)
					return
				}
				pod, ok = deletedState.Obj.(*v1.Pod)
				if !ok {
					klog.Errorf("DeletedFinalStateUnknown contained non-Pod object: %v", deletedState.Obj)
					return
				}
			}
			nc.podUpdated(pod, nil)
			if nc.taintManager != nil {
				nc.taintManager.PodUpdated(pod, nil)
			}
		},
	})
	nc.podInformerSynced = podInformer.Informer().HasSynced
	podInformer.Informer().AddIndexers(cache.Indexers{
		nodeNameKeyIndex: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return []string{}, nil
			}
			if len(pod.Spec.NodeName) == 0 {
				return []string{}, nil
			}
			return []string{pod.Spec.NodeName}, nil
		},
	})

	podIndexer := podInformer.Informer().GetIndexer()
	// 根据nodename 来获取所有在这个node上的pod
	nc.getPodsAssignedToNode = func(nodeName string) ([]*v1.Pod, error) {
		objs, err := podIndexer.ByIndex(nodeNameKeyIndex, nodeName)
		if err != nil {
			return nil, err
		}
		pods := make([]*v1.Pod, 0, len(objs))
		for _, obj := range objs {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				continue
			}
			pods = append(pods, pod)
		}
		return pods, nil
	}
	nc.podLister = podInformer.Lister()
	// 如果开启了TaintManager 则需要将TaintManager有关的函数注册到nodeInformer里
	if nc.runTaintManager {
		podGetter := func(name, namespace string) (*v1.Pod, error) { return nc.podLister.Pods(namespace).Get(name) }
		nodeLister := nodeInformer.Lister()
		nodeGetter := func(name string) (*v1.Node, error) { return nodeLister.Get(name) }
		// 初始化taintManager
		nc.taintManager = scheduler.NewNoExecuteTaintManager(kubeClient, podGetter, nodeGetter, nc.getPodsAssignedToNode)
		// 监听watch node add/update/delete等事件 并且注册TaintManager的相关函数
		nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: nodeutil.CreateAddNodeHandler(func(node *v1.Node) error {
				nc.taintManager.NodeUpdated(nil, node)
				return nil
			}),
			UpdateFunc: nodeutil.CreateUpdateNodeHandler(func(oldNode, newNode *v1.Node) error {
				nc.taintManager.NodeUpdated(oldNode, newNode)
				return nil
			}),
			DeleteFunc: nodeutil.CreateDeleteNodeHandler(func(node *v1.Node) error {
				nc.taintManager.NodeUpdated(node, nil)
				return nil
			}),
		})
	}

	klog.Infof("Controller will reconcile labels.")
	// 监听watch node add/update/delete等事件 并且注册对应事件所需要的函数
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: nodeutil.CreateAddNodeHandler(func(node *v1.Node) error {
			nc.nodeUpdateQueue.Add(node.Name)
			nc.nodeEvictionMap.registerNode(node.Name)
			return nil
		}),
		UpdateFunc: nodeutil.CreateUpdateNodeHandler(func(_, newNode *v1.Node) error {
			nc.nodeUpdateQueue.Add(newNode.Name)
			return nil
		}),
		DeleteFunc: nodeutil.CreateDeleteNodeHandler(func(node *v1.Node) error {
			nc.nodesToRetry.Delete(node.Name)
			nc.nodeEvictionMap.unregisterNode(node.Name)
			return nil
		}),
	})
	// 用于获取lease元数据
	nc.leaseLister = leaseInformer.Lister()
	nc.leaseInformerSynced = leaseInformer.Informer().HasSynced
	// 用于获取node元数据
	nc.nodeLister = nodeInformer.Lister()
	nc.nodeInformerSynced = nodeInformer.Informer().HasSynced
	// 用于获取daemonset元数据
	nc.daemonSetStore = daemonSetInformer.Lister()
	nc.daemonSetInformerSynced = daemonSetInformer.Informer().HasSynced

	return nc, nil
}

// Run starts an asynchronous loop that monitors the status of cluster nodes.
func (nc *Controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	klog.Infof("Starting node controller")
	defer klog.Infof("Shutting down node controller")
	// 等待lease,node,pod,daemonset 的缓存同步完成
	if !cache.WaitForNamedCacheSync("taint", stopCh, nc.leaseInformerSynced, nc.nodeInformerSynced, nc.podInformerSynced, nc.daemonSetInformerSynced) {
		return
	}
	// 运行TaintManager 根据容忍和污点等特性完成对Pod的驱逐任务
	if nc.runTaintManager {
		go nc.taintManager.Run(stopCh)
	}

	// Close node update queue to cleanup go routine.
	defer nc.nodeUpdateQueue.ShutDown()
	defer nc.podUpdateQueue.ShutDown()

	// Start workers to reconcile labels and/or update NoSchedule taint for nodes.
	// 启动多个goroutine 来运行doNodeProcessingPassWorker 处理nodeUpdateQueue队列中的数据
	// doNodeProcessingPassWorker为每个node对象完成NoSchedule的污点更新及Label的更新
	for i := 0; i < scheduler.UpdateWorkerSize; i++ {
		// Thanks to "workqueue", each worker just need to get item from queue, because
		// the item is flagged when got from queue: if new event come, the new item will
		// be re-queued until "Done", so no more than one worker handle the same item and
		// no event missed.
		go wait.Until(nc.doNodeProcessingPassWorker, time.Second, stopCh)
	}
	// 启动多个goroutine 来运行doPodProcessingWorker 处理podUpdateQueue队列中的数据
	for i := 0; i < podUpdateWorkerSize; i++ {
		go wait.Until(nc.doPodProcessingWorker, time.Second, stopCh)
	}

	if nc.runTaintManager {
		// Handling taint based evictions. Because we don't want a dedicated logic in TaintManager for NC-originated
		// taints and we normally don't rate limit evictions caused by taints, we need to rate limit adding taints.
		// 如果开启TaintManager 则运行doNoExecuteTaintingPass
		// 处理nc.zoneNoExecuteTainter队列中的数据
		go wait.Until(nc.doNoExecuteTaintingPass, scheduler.NodeEvictionPeriod, stopCh)
	} else {
		// Managing eviction of nodes:
		// When we delete pods off a node, if the node was not empty at the time we then
		// queue an eviction watcher. If we hit an error, retry deletion.
		// 如果没有开启TaintManager 则运行doEvictionPass
		go wait.Until(nc.doEvictionPass, scheduler.NodeEvictionPeriod, stopCh)
	}

	// Incorporate the results of node health signal pushed from kubelet to master.
	// 运行monitorNodeHealth周期性监控node状态
	go wait.Until(func() {
		if err := nc.monitorNodeHealth(); err != nil {
			klog.Errorf("Error monitoring node health: %v", err)
		}
	}, nc.nodeMonitorPeriod, stopCh)

	<-stopCh
}

func (nc *Controller) doNodeProcessingPassWorker() {
	// 此函数的功能主要是不断的从nodeUpdateQueue队列中取出数据 为每个对象完成NoSchedule的污点更新及Label的更新
	for {
		obj, shutdown := nc.nodeUpdateQueue.Get()
		// "nodeUpdateQueue" will be shutdown when "stopCh" closed;
		// we do not need to re-check "stopCh" again.
		if shutdown {
			return
		}
		nodeName := obj.(string)
		// 完成每个node的NoSchedule更新
		if err := nc.doNoScheduleTaintingPass(nodeName); err != nil {
			klog.Errorf("Failed to taint NoSchedule on node <%s>, requeue it: %v", nodeName, err)
			// TODO(k82cn): Add nodeName back to the queue
		}
		// TODO: re-evaluate whether there are any labels that need to be
		// reconcile in 1.19. Remove this function if it's no longer necessary.
		if err := nc.reconcileNodeLabels(nodeName); err != nil {
			klog.Errorf("Failed to reconcile labels for node <%s>, requeue it: %v", nodeName, err)
			// TODO(yujuhong): Add nodeName back to the queue
		}
		nc.nodeUpdateQueue.Done(nodeName)
	}
}

func (nc *Controller) doNoScheduleTaintingPass(nodeName string) error {
	// 获取node对象
	node, err := nc.nodeLister.Get(nodeName)
	if err != nil {
		// If node not found, just ignore it.
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// Map node's condition to Taints.
	var taints []v1.Taint
	// 遍历node.status.Conditions
	for _, condition := range node.Status.Conditions {
		// 判断Conditions.Type是否在nodeConditionToTaintKeyStatusMap里 如果在 需要添加对应的污点
		// 根据Conditions来创建对应的污点
		if taintMap, found := nodeConditionToTaintKeyStatusMap[condition.Type]; found {
			if taintKey, found := taintMap[condition.Status]; found {
				taints = append(taints, v1.Taint{
					Key:    taintKey,
					Effect: v1.TaintEffectNoSchedule,
				})
			}
		}
	}
	// 如果处于Unschedulable状态 也添加对应的污点
	if node.Spec.Unschedulable {
		// If unschedulable, append related taint.
		taints = append(taints, v1.Taint{
			Key:    v1.TaintNodeUnschedulable,
			Effect: v1.TaintEffectNoSchedule,
		})
	}

	// Get exist taints of node.
	// 获取当前node已经存在的taints
	nodeTaints := taintutils.TaintSetFilter(node.Spec.Taints, func(t *v1.Taint) bool {
		// only NoSchedule taints are candidates to be compared with "taints" later
		if t.Effect != v1.TaintEffectNoSchedule {
			return false
		}
		// Find unschedulable taint of node.
		if t.Key == v1.TaintNodeUnschedulable {
			return true
		}
		// Find node condition taints of node.
		_, found := taintKeyToNodeConditionMap[t.Key]
		return found
	})
	// 对比当前和新的 得到需要添加及需要删除的
	taintsToAdd, taintsToDel := taintutils.TaintSetDiff(taints, nodeTaints)
	// If nothing to add not delete, return true directly.
	if len(taintsToAdd) == 0 && len(taintsToDel) == 0 {
		return nil
	}
	// 更新当前node taints
	if !nodeutil.SwapNodeControllerTaint(nc.kubeClient, taintsToAdd, taintsToDel, node) {
		return fmt.Errorf("failed to swap taints of node %+v", node)
	}
	return nil
}

func (nc *Controller) doNoExecuteTaintingPass() {
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()
	for k := range nc.zoneNoExecuteTainter {
		// Function should return 'false' and a time after which it should be retried, or 'true' if it shouldn't (it succeeded).
		nc.zoneNoExecuteTainter[k].Try(func(value scheduler.TimedValue) (bool, time.Duration) {
			// 获取node对象
			node, err := nc.nodeLister.Get(value.Value)
			if apierrors.IsNotFound(err) {
				klog.Warningf("Node %v no longer present in nodeLister!", value.Value)
				return true, 0
			} else if err != nil {
				klog.Warningf("Failed to get Node %v from the nodeLister: %v", value.Value, err)
				// retry in 50 millisecond
				return false, 50 * time.Millisecond
			}
			// 获取node的NodeReadyConditions
			_, condition := nodeutil.GetNodeCondition(&node.Status, v1.NodeReady)
			// Because we want to mimic NodeStatus.Condition["Ready"] we make "unreachable" and "not ready" taints mutually exclusive.
			taintToAdd := v1.Taint{}
			oppositeTaint := v1.Taint{}
			switch condition.Status {
			case v1.ConditionFalse:
				// 如果status为fasle 则为node添加
				taintToAdd = *NotReadyTaintTemplate
				// 如果status为fasle 则为node移除
				oppositeTaint = *UnreachableTaintTemplate
			case v1.ConditionUnknown:
				// 如果status为unknown 则为node添加
				taintToAdd = *UnreachableTaintTemplate
				// 如果status为unknown 则为node移除
				oppositeTaint = *NotReadyTaintTemplate
			default:
				// It seems that the Node is ready again, so there's no need to taint it.
				klog.V(4).Infof("Node %v was in a taint queue, but it's ready now. Ignoring taint request.", value.Value)
				return true, 0
			}
			// 更新node.taints
			result := nodeutil.SwapNodeControllerTaint(nc.kubeClient, []*v1.Taint{&taintToAdd}, []*v1.Taint{&oppositeTaint}, node)
			if result {
				//count the evictionsNumber
				zone := utilnode.GetZoneKey(node)
				evictionsNumber.WithLabelValues(zone).Inc()
			}

			return result, 0
		})
	}
}

func (nc *Controller) doEvictionPass() {
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()
	for k := range nc.zonePodEvictor {
		// Function should return 'false' and a time after which it should be retried, or 'true' if it shouldn't (it succeeded).
		nc.zonePodEvictor[k].Try(func(value scheduler.TimedValue) (bool, time.Duration) {
			// 获取node对象
			node, err := nc.nodeLister.Get(value.Value)
			if apierrors.IsNotFound(err) {
				klog.Warningf("Node %v no longer present in nodeLister!", value.Value)
			} else if err != nil {
				klog.Warningf("Failed to get Node %v from the nodeLister: %v", value.Value, err)
			}
			nodeUID, _ := value.UID.(string)
			// 根据nodename获取所有相关到Pod
			pods, err := nc.getPodsAssignedToNode(value.Value)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to list pods from node %q: %v", value.Value, err))
				return false, 0
			}
			// 删除该node上所有pod
			remaining, err := nodeutil.DeletePods(nc.kubeClient, pods, nc.recorder, value.Value, nodeUID, nc.daemonSetStore)
			if err != nil {
				// We are not setting eviction status here.
				// New pods will be handled by zonePodEvictor retry
				// instead of immediate pod eviction.
				utilruntime.HandleError(fmt.Errorf("unable to evict node %q: %v", value.Value, err))
				return false, 0
			}
			if !nc.nodeEvictionMap.setStatus(value.Value, evicted) {
				klog.V(2).Infof("node %v was unregistered in the meantime - skipping setting status", value.Value)
			}
			if remaining {
				klog.Infof("Pods awaiting deletion due to Controller eviction")
			}

			if node != nil {
				zone := utilnode.GetZoneKey(node)
				evictionsNumber.WithLabelValues(zone).Inc()
			}

			return true, 0
		})
	}
}

// monitorNodeHealth verifies node health are constantly updated by kubelet, and
// if not, post "NodeReady==ConditionUnknown".
// This function will taint nodes who are not ready or not reachable for a long period of time.
func (nc *Controller) monitorNodeHealth() error {
	// 监控node运行状态
	// 对异常的node进行处理
	// We are listing nodes from local cache as we can tolerate some small delays
	// comparing to state from etcd and there is eventual consistency anyway.
	// 获取所有node对象
	nodes, err := nc.nodeLister.List(labels.Everything())
	if err != nil {
		return err
	}
	// 将所有node进行分类
	// added = 需要新增的node
	// deleted = 需要删除的node
	// newZoneRepresentatives = 需要进行分类到不同zone里到node
	added, deleted, newZoneRepresentatives := nc.classifyNodes(nodes)

	// 遍历所有需要分类到不同zone里到node
	for i := range newZoneRepresentatives {
		// 进行分类 将node划分到zone里
		nc.addPodEvictorForNewZone(newZoneRepresentatives[i])
	}

	for i := range added {
		klog.V(1).Infof("Controller observed a new Node: %#v", added[i].Name)
		nodeutil.RecordNodeEvent(nc.recorder, added[i].Name, string(added[i].UID), v1.EventTypeNormal, "RegisteredNode", fmt.Sprintf("Registered Node %v in Controller", added[i].Name))
		// 添加到knownNodeSet
		nc.knownNodeSet[added[i].Name] = added[i]
		// 进行分类 将node划分到zone里
		nc.addPodEvictorForNewZone(added[i])
		// 判断是否开启TaintManager
		if nc.runTaintManager {
			// 移除node的taints中Unreachable，NotReady，并从zoneNoExecuteTainter移除
			nc.markNodeAsReachable(added[i])
		} else {
			// 从zonePodEvictor移除node
			nc.cancelPodEviction(added[i])
		}
	}
	// 遍历所有deleted 将其从knownNodeSet移除
	for i := range deleted {
		klog.V(1).Infof("Controller observed a Node deletion: %v", deleted[i].Name)
		nodeutil.RecordNodeEvent(nc.recorder, deleted[i].Name, string(deleted[i].UID), v1.EventTypeNormal, "RemovingNode", fmt.Sprintf("Removing Node %v from Controller", deleted[i].Name))
		delete(nc.knownNodeSet, deleted[i].Name)
	}

	zoneToNodeConditions := map[string][]*v1.NodeCondition{}
	// 遍历所有node 检查每个node心跳状态
	for i := range nodes {
		var gracePeriod time.Duration
		var observedReadyCondition v1.NodeCondition
		var currentReadyCondition *v1.NodeCondition
		node := nodes[i].DeepCopy()
		if err := wait.PollImmediate(retrySleepTime, retrySleepTime*scheduler.NodeHealthUpdateRetry, func() (bool, error) {
			// currentReadyCondition当前就绪条件
			// observedReadyCondition
			// gracePeriod
			gracePeriod, observedReadyCondition, currentReadyCondition, err = nc.tryUpdateNodeHealth(node)
			if err == nil {
				return true, nil
			}
			name := node.Name
			// 上面会进行更新node 所以这个地方需要重新获取下最新的数据
			node, err = nc.kubeClient.CoreV1().Nodes().Get(context.TODO(), name, metav1.GetOptions{})
			if err != nil {
				klog.Errorf("Failed while getting a Node to retry updating node health. Probably Node %s was deleted.", name)
				return false, err
			}
			return false, nil
		}); err != nil {
			klog.Errorf("Update health of Node '%v' from Controller error: %v. "+
				"Skipping - no pods will be evicted.", node.Name, err)
			continue
		}

		// Some nodes may be excluded from disruption checking
		// 判断node是否被排除, 如果没有被排除 则加入到zoneToNodeConditions里
		if !isNodeExcludedFromDisruptionChecks(node) {
			zoneToNodeConditions[utilnode.GetZoneKey(node)] = append(zoneToNodeConditions[utilnode.GetZoneKey(node)], currentReadyCondition)
		}

		if currentReadyCondition != nil {
			// 根据nodename获取所有相关到Pod
			pods, err := nc.getPodsAssignedToNode(node.Name)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to list pods of node %v: %v", node.Name, err))
				if currentReadyCondition.Status != v1.ConditionTrue && observedReadyCondition.Status == v1.ConditionTrue {
					// If error happened during node status transition (Ready -> NotReady)
					// we need to mark node for retry to force MarkPodsNotReady execution
					// in the next iteration.
					nc.nodesToRetry.Store(node.Name, struct{}{})
				}
				continue
			}
			// 如果启动了驱逐任务
			if nc.runTaintManager {
				nc.processTaintBaseEviction(node, &observedReadyCondition)
			} else {
				if err := nc.processNoTaintBaseEviction(node, &observedReadyCondition, gracePeriod, pods); err != nil {
					utilruntime.HandleError(fmt.Errorf("unable to evict all pods from node %v: %v; queuing for retry", node.Name, err))
				}
			}

			_, needsRetry := nc.nodesToRetry.Load(node.Name)
			switch {
			case currentReadyCondition.Status != v1.ConditionTrue && observedReadyCondition.Status == v1.ConditionTrue:
				// Report node event only once when status changed.
				nodeutil.RecordNodeStatusChange(nc.recorder, node, "NodeNotReady")
				fallthrough
			case needsRetry && observedReadyCondition.Status != v1.ConditionTrue:
				if err = nodeutil.MarkPodsNotReady(nc.kubeClient, nc.recorder, pods, node.Name); err != nil {
					utilruntime.HandleError(fmt.Errorf("unable to mark all pods NotReady on node %v: %v; queuing for retry", node.Name, err))
					nc.nodesToRetry.Store(node.Name, struct{}{})
					continue
				}
			}
		}
		nc.nodesToRetry.Delete(node.Name)
	}
	nc.handleDisruption(zoneToNodeConditions, nodes)

	return nil
}

func (nc *Controller) processTaintBaseEviction(node *v1.Node, observedReadyCondition *v1.NodeCondition) {
	decisionTimestamp := nc.now()
	// Check eviction timeout against decisionTimestamp
	switch observedReadyCondition.Status {
	case v1.ConditionFalse:
		// We want to update the taint straight away if Node is already tainted with the UnreachableTaint
		if taintutils.TaintExists(node.Spec.Taints, UnreachableTaintTemplate) {
			taintToAdd := *NotReadyTaintTemplate
			if !nodeutil.SwapNodeControllerTaint(nc.kubeClient, []*v1.Taint{&taintToAdd}, []*v1.Taint{UnreachableTaintTemplate}, node) {
				klog.Errorf("Failed to instantly swap UnreachableTaint to NotReadyTaint. Will try again in the next cycle.")
			}
		} else if nc.markNodeForTainting(node, v1.ConditionFalse) {
			klog.V(2).Infof("Node %v is NotReady as of %v. Adding it to the Taint queue.",
				node.Name,
				decisionTimestamp,
			)
		}
	case v1.ConditionUnknown:
		// We want to update the taint straight away if Node is already tainted with the UnreachableTaint
		if taintutils.TaintExists(node.Spec.Taints, NotReadyTaintTemplate) {
			taintToAdd := *UnreachableTaintTemplate
			if !nodeutil.SwapNodeControllerTaint(nc.kubeClient, []*v1.Taint{&taintToAdd}, []*v1.Taint{NotReadyTaintTemplate}, node) {
				klog.Errorf("Failed to instantly swap NotReadyTaint to UnreachableTaint. Will try again in the next cycle.")
			}
		} else if nc.markNodeForTainting(node, v1.ConditionUnknown) {
			klog.V(2).Infof("Node %v is unresponsive as of %v. Adding it to the Taint queue.",
				node.Name,
				decisionTimestamp,
			)
		}
	case v1.ConditionTrue:
		removed, err := nc.markNodeAsReachable(node)
		if err != nil {
			klog.Errorf("Failed to remove taints from node %v. Will retry in next iteration.", node.Name)
		}
		if removed {
			klog.V(2).Infof("Node %s is healthy again, removing all taints", node.Name)
		}
	}
}

func (nc *Controller) processNoTaintBaseEviction(node *v1.Node, observedReadyCondition *v1.NodeCondition, gracePeriod time.Duration, pods []*v1.Pod) error {
	decisionTimestamp := nc.now()
	nodeHealthData := nc.nodeHealthMap.getDeepCopy(node.Name)
	if nodeHealthData == nil {
		return fmt.Errorf("health data doesn't exist for node %q", node.Name)
	}
	// Check eviction timeout against decisionTimestamp
	switch observedReadyCondition.Status {
	case v1.ConditionFalse:
		if decisionTimestamp.After(nodeHealthData.readyTransitionTimestamp.Add(nc.podEvictionTimeout)) {
			enqueued, err := nc.evictPods(node, pods)
			if err != nil {
				return err
			}
			if enqueued {
				klog.V(2).Infof("Node is NotReady. Adding Pods on Node %s to eviction queue: %v is later than %v + %v",
					node.Name,
					decisionTimestamp,
					nodeHealthData.readyTransitionTimestamp,
					nc.podEvictionTimeout,
				)
			}
		}
	case v1.ConditionUnknown:
		if decisionTimestamp.After(nodeHealthData.probeTimestamp.Add(nc.podEvictionTimeout)) {
			enqueued, err := nc.evictPods(node, pods)
			if err != nil {
				return err
			}
			if enqueued {
				klog.V(2).Infof("Node is unresponsive. Adding Pods on Node %s to eviction queues: %v is later than %v + %v",
					node.Name,
					decisionTimestamp,
					nodeHealthData.readyTransitionTimestamp,
					nc.podEvictionTimeout-gracePeriod,
				)
			}
		}
	case v1.ConditionTrue:
		if nc.cancelPodEviction(node) {
			klog.V(2).Infof("Node %s is ready again, cancelled pod eviction", node.Name)
		}
	}
	return nil
}

// labelNodeDisruptionExclusion is a label on nodes that controls whether they are
// excluded from being considered for disruption checks by the node controller.
const labelNodeDisruptionExclusion = "node.kubernetes.io/exclude-disruption"

func isNodeExcludedFromDisruptionChecks(node *v1.Node) bool {
	// DEPRECATED: will be removed in 1.19
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.LegacyNodeRoleBehavior) {
		if legacyIsMasterNode(node.Name) {
			return true
		}
	}
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.NodeDisruptionExclusion) {
		if _, ok := node.Labels[labelNodeDisruptionExclusion]; ok {
			return true
		}
	}
	return false
}

// legacyIsMasterNode returns true if given node is a registered master according
// to the logic historically used for this function. This code path is deprecated
// and the node disruption exclusion label should be used in the future.
// This code will not be allowed to update to use the node-role label, since
// node-roles may not be used for feature enablement.
// DEPRECATED: Will be removed in 1.19
func legacyIsMasterNode(nodeName string) bool {
	// We are trying to capture "master(-...)?$" regexp.
	// However, using regexp.MatchString() results even in more than 35%
	// of all space allocations in ControllerManager spent in this function.
	// That's why we are trying to be a bit smarter.
	if strings.HasSuffix(nodeName, "master") {
		return true
	}
	if len(nodeName) >= 10 {
		return strings.HasSuffix(nodeName[:len(nodeName)-3], "master-")
	}
	return false
}

// tryUpdateNodeHealth checks a given node's conditions and tries to update it. Returns grace period to
// which given node is entitled, state of current and last observed Ready Condition, and an error if it occurred.
func (nc *Controller) tryUpdateNodeHealth(node *v1.Node) (time.Duration, v1.NodeCondition, *v1.NodeCondition, error) {
	// 获取node最近一次的心跳信息
	nodeHealth := nc.nodeHealthMap.getDeepCopy(node.Name)
	defer func() {
		// 更新node的心跳信息
		nc.nodeHealthMap.set(node.Name, nodeHealth)
	}()

	var gracePeriod time.Duration
	var observedReadyCondition v1.NodeCondition
	// 从当前node.status.Conditions获取NodeReady
	_, currentReadyCondition := nodeutil.GetNodeCondition(&node.Status, v1.NodeReady)
	if currentReadyCondition == nil {
		// If ready condition is nil, then kubelet (or nodecontroller) never posted node status.
		// A fake ready condition is created, where LastHeartbeatTime and LastTransitionTime is set
		// to node.CreationTimestamp to avoid handle the corner case.
		// 如果currentReadyCondition不存在 此时kubelet 或者 nodecontroller 应该是没有上报node的状态
		// 此时为node fake(伪造) 一个ready condition status="Unknown"
		// LastHeartbeatTime和LastTransitionTime 为node的创建时间
		observedReadyCondition = v1.NodeCondition{
			Type:               v1.NodeReady,
			Status:             v1.ConditionUnknown,
			LastHeartbeatTime:  node.CreationTimestamp,
			LastTransitionTime: node.CreationTimestamp,
		}

		gracePeriod = nc.nodeStartupGracePeriod
		// 如果存在 更新node状态
		if nodeHealth != nil {
			nodeHealth.status = &node.Status
		} else {
			// 如果不存在 就创建
			nodeHealth = &nodeHealthData{
				status:                   &node.Status,
				probeTimestamp:           node.CreationTimestamp,
				readyTransitionTimestamp: node.CreationTimestamp,
			}
		}
	} else {
		// If ready condition is not nil, make a copy of it, since we may modify it in place later.
		// 如果currentReadyCondition存在 则observedReadyCondition = currentReadyCondition
		observedReadyCondition = *currentReadyCondition
		gracePeriod = nc.nodeMonitorGracePeriod
	}
	// There are following cases to check:
	// - both saved and new status have no Ready Condition set - we leave everything as it is,
	// - saved status have no Ready Condition, but current one does - Controller was restarted with Node data already present in etcd,
	// - saved status have some Ready Condition, but current one does not - it's an error, but we fill it up because that's probably a good thing to do,
	// - both saved and current statuses have Ready Conditions and they have the same LastProbeTime - nothing happened on that Node, it may be
	//   unresponsive, so we leave it as it is,
	// - both saved and current statuses have Ready Conditions, they have different LastProbeTimes, but the same Ready Condition State -
	//   everything's in order, no transition occurred, we update only probeTimestamp,
	// - both saved and current statuses have Ready Conditions, different LastProbeTimes and different Ready Condition State -
	//   Ready Condition changed it state since we last seen it, so we update both probeTimestamp and readyTransitionTimestamp.
	// TODO: things to consider:
	//   - if 'LastProbeTime' have gone back in time its probably an error, currently we ignore it,
	//   - currently only correct Ready State transition outside of Node Controller is marking it ready by Kubelet, we don't check
	//     if that's the case, but it does not seem necessary.
	var savedCondition *v1.NodeCondition
	var savedLease *coordv1.Lease
	if nodeHealth != nil {
		// 从node心跳信息里获取node.status.condition.type = "Ready"
		_, savedCondition = nodeutil.GetNodeCondition(nodeHealth.status, v1.NodeReady)
		savedLease = nodeHealth.lease
	}
	// 根据savedCondition,currentReadyCondition来创建nodeHealth数据
	// 如果nodeHealth是空的 则创建nodeHealth
	if nodeHealth == nil {
		klog.Warningf("Missing timestamp for Node %s. Assuming now as a timestamp.", node.Name)
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: nc.now(),
		}
	} else if savedCondition == nil && currentReadyCondition != nil {
		klog.V(1).Infof("Creating timestamp entry for newly observed Node %s", node.Name)
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: nc.now(),
		}
	} else if savedCondition != nil && currentReadyCondition == nil {
		klog.Errorf("ReadyCondition was removed from Status of Node %s", node.Name)
		// TODO: figure out what to do in this case. For now we do the same thing as above.
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: nc.now(),
		}
	} else if savedCondition != nil && currentReadyCondition != nil && savedCondition.LastHeartbeatTime != currentReadyCondition.LastHeartbeatTime {
		var transitionTime metav1.Time
		// If ReadyCondition changed since the last time we checked, we update the transition timestamp to "now",
		// otherwise we leave it as it is.
		if savedCondition.LastTransitionTime != currentReadyCondition.LastTransitionTime {
			klog.V(3).Infof("ReadyCondition for Node %s transitioned from %v to %v", node.Name, savedCondition, currentReadyCondition)
			transitionTime = nc.now()
		} else {
			transitionTime = nodeHealth.readyTransitionTimestamp
		}
		if klog.V(5).Enabled() {
			klog.Infof("Node %s ReadyCondition updated. Updating timestamp: %+v vs %+v.", node.Name, nodeHealth.status, node.Status)
		} else {
			klog.V(3).Infof("Node %s ReadyCondition updated. Updating timestamp.", node.Name)
		}
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: transitionTime,
		}
	}
	// Always update the probe time if node lease is renewed.
	// Note: If kubelet never posted the node status, but continues renewing the
	// heartbeat leases, the node controller will assume the node is healthy and
	// take no action.
	// 获取当前node的lease(租约)
	observedLease, _ := nc.leaseLister.Leases(v1.NamespaceNodeLease).Get(node.Name)
	// 判断 然后更新当前lease信息及探测时间
	if observedLease != nil && (savedLease == nil || savedLease.Spec.RenewTime.Before(observedLease.Spec.RenewTime)) {
		nodeHealth.lease = observedLease
		nodeHealth.probeTimestamp = nc.now()
	}
	// 判断node是否已经超过gracePeriod没进行上报状态
	if nc.now().After(nodeHealth.probeTimestamp.Add(gracePeriod)) {
		// NodeReady condition or lease was last set longer ago than gracePeriod, so
		// update it to Unknown (regardless of its current value) in the master.

		nodeConditionTypes := []v1.NodeConditionType{
			v1.NodeReady,
			v1.NodeMemoryPressure,
			v1.NodeDiskPressure,
			v1.NodePIDPressure,
			// We don't change 'NodeNetworkUnavailable' condition, as it's managed on a control plane level.
			// v1.NodeNetworkUnavailable,
		}

		nowTimestamp := nc.now()
		for _, nodeConditionType := range nodeConditionTypes {
			_, currentCondition := nodeutil.GetNodeCondition(&node.Status, nodeConditionType)
			if currentCondition == nil {
				klog.V(2).Infof("Condition %v of node %v was never updated by kubelet", nodeConditionType, node.Name)
				node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
					Type:               nodeConditionType,
					Status:             v1.ConditionUnknown,
					Reason:             "NodeStatusNeverUpdated",
					Message:            "Kubelet never posted node status.",
					LastHeartbeatTime:  node.CreationTimestamp,
					LastTransitionTime: nowTimestamp,
				})
			} else {
				klog.V(2).Infof("node %v hasn't been updated for %+v. Last %v is: %+v",
					node.Name, nc.now().Time.Sub(nodeHealth.probeTimestamp.Time), nodeConditionType, currentCondition)
				if currentCondition.Status != v1.ConditionUnknown {
					currentCondition.Status = v1.ConditionUnknown
					currentCondition.Reason = "NodeStatusUnknown"
					currentCondition.Message = "Kubelet stopped posting node status."
					currentCondition.LastTransitionTime = nowTimestamp
				}
			}
		}
		// We need to update currentReadyCondition due to its value potentially changed.
		_, currentReadyCondition = nodeutil.GetNodeCondition(&node.Status, v1.NodeReady)
		// 更新node最新的状态及更新nodeHealth数据
		if !apiequality.Semantic.DeepEqual(currentReadyCondition, &observedReadyCondition) {
			if _, err := nc.kubeClient.CoreV1().Nodes().UpdateStatus(context.TODO(), node, metav1.UpdateOptions{}); err != nil {
				klog.Errorf("Error updating node %s: %v", node.Name, err)
				return gracePeriod, observedReadyCondition, currentReadyCondition, err
			}
			nodeHealth = &nodeHealthData{
				status:                   &node.Status,
				probeTimestamp:           nodeHealth.probeTimestamp,
				readyTransitionTimestamp: nc.now(),
				lease:                    observedLease,
			}
			return gracePeriod, observedReadyCondition, currentReadyCondition, nil
		}
	}

	return gracePeriod, observedReadyCondition, currentReadyCondition, nil
}

func (nc *Controller) handleDisruption(zoneToNodeConditions map[string][]*v1.NodeCondition, nodes []*v1.Node) {
	// 创建新的 zoneState
	newZoneStates := map[string]ZoneState{}
	// 默认值为true
	allAreFullyDisrupted := true
	for k, v := range zoneToNodeConditions {
		// metric
		zoneSize.WithLabelValues(k).Set(float64(len(v)))
		// 计算node所属zone及未就绪node数量
		unhealthy, newState := nc.computeZoneStateFunc(v)
		// metric
		zoneHealth.WithLabelValues(k).Set(float64(100*(len(v)-unhealthy)) / float64(len(v)))
		// metric
		unhealthyNodes.WithLabelValues(k).Set(float64(unhealthy))
		// newstate不为stateFullDisruption 将allAreFullyDisrupted标志为false
		if newState != stateFullDisruption {
			allAreFullyDisrupted = false
		}
		// 将newState保存到newZoneStates里
		newZoneStates[k] = newState
		if _, had := nc.zoneStates[k]; !had {
			klog.Errorf("Setting initial state for unseen zone: %v", k)
			// 设置为初始化状态
			nc.zoneStates[k] = stateInitial
		}
	}
	// 处理上一次观测处理的zoneState数据
	// 默认值为true
	allWasFullyDisrupted := true
	// 遍历上一次观测处理的zoneState数据
	for k, v := range nc.zoneStates {
		// 判断如果不在zoneToNodeConditions里 则从nc.zoneStates删除
		if _, have := zoneToNodeConditions[k]; !have {
			// metric
			zoneSize.WithLabelValues(k).Set(0)
			zoneHealth.WithLabelValues(k).Set(100)
			unhealthyNodes.WithLabelValues(k).Set(0)
			delete(nc.zoneStates, k)
			continue
		}
		// 判断上一次的状态是否为FullDisruption状态
		if v != stateFullDisruption {
			allWasFullyDisrupted = false
			break
		}
	}

	// At least one node was responding in previous pass or in the current pass. Semantics is as follows:
	// - if the new state is "partialDisruption" we call a user defined function that returns a new limiter to use,
	// - if the new state is "normal" we resume normal operation (go back to default limiter settings),
	// - if new state is "fullDisruption" we restore normal eviction rate,
	//   - unless all zones in the cluster are in "fullDisruption" - in that case we stop all evictions.
	// 如果新的zoneState 或者上一次观测的zoneState 有一个存在
	if !allAreFullyDisrupted || !allWasFullyDisrupted {
		// We're switching to full disruption mode
		// 切换到了FullyDisrupted模式
		// 当allAreFullyDisrupted = true allWasFullyDisrupted = false
		if allAreFullyDisrupted {
			klog.V(0).Info("Controller detected that all Nodes are not-Ready. Entering master disruption mode.")
			// 遍历所有node
			for i := range nodes {
				// 判断是否开启了TaintManager
				if nc.runTaintManager {
					// 移除node的taints中Unreachable，NotReady，并从zoneNoExecuteTainter移除
					_, err := nc.markNodeAsReachable(nodes[i])
					if err != nil {
						klog.Errorf("Failed to remove taints from Node %v", nodes[i].Name)
					}
				} else {
					nc.cancelPodEviction(nodes[i])
				}
			}
			// We stop all evictions.
			for k := range nc.zoneStates {
				if nc.runTaintManager {
					nc.zoneNoExecuteTainter[k].SwapLimiter(0)
				} else {
					nc.zonePodEvictor[k].SwapLimiter(0)
				}
			}
			for k := range nc.zoneStates {
				nc.zoneStates[k] = stateFullDisruption
			}
			// All rate limiters are updated, so we can return early here.
			return
		}
		// We're exiting full disruption mode
		if allWasFullyDisrupted {
			klog.V(0).Info("Controller detected that some Nodes are Ready. Exiting master disruption mode.")
			// When exiting disruption mode update probe timestamps on all Nodes.
			now := nc.now()
			for i := range nodes {
				v := nc.nodeHealthMap.getDeepCopy(nodes[i].Name)
				v.probeTimestamp = now
				v.readyTransitionTimestamp = now
				nc.nodeHealthMap.set(nodes[i].Name, v)
			}
			// We reset all rate limiters to settings appropriate for the given state.
			for k := range nc.zoneStates {
				nc.setLimiterInZone(k, len(zoneToNodeConditions[k]), newZoneStates[k])
				nc.zoneStates[k] = newZoneStates[k]
			}
			return
		}
		// We know that there's at least one not-fully disrupted so,
		// we can use default behavior for rate limiters
		// 遍历所有zoneState 为每个zoneState设置不同的驱逐速率
		for k, v := range nc.zoneStates {
			newState := newZoneStates[k]
			if v == newState {
				continue
			}
			klog.V(0).Infof("Controller detected that zone %v is now in state %v.", k, newState)
			nc.setLimiterInZone(k, len(zoneToNodeConditions[k]), newState)
			nc.zoneStates[k] = newState
		}
	}
}

func (nc *Controller) podUpdated(oldPod, newPod *v1.Pod) {
	if newPod == nil {
		return
	}
	if len(newPod.Spec.NodeName) != 0 && (oldPod == nil || newPod.Spec.NodeName != oldPod.Spec.NodeName) {
		podItem := podUpdateItem{newPod.Namespace, newPod.Name}
		nc.podUpdateQueue.Add(podItem)
	}
}

func (nc *Controller) doPodProcessingWorker() {
	for {
		obj, shutdown := nc.podUpdateQueue.Get()
		// "podUpdateQueue" will be shutdown when "stopCh" closed;
		// we do not need to re-check "stopCh" again.
		if shutdown {
			return
		}

		podItem := obj.(podUpdateItem)
		nc.processPod(podItem)
	}
}

// processPod is processing events of assigning pods to nodes. In particular:
// 1. for NodeReady=true node, taint eviction for this pod will be cancelled
// 2. for NodeReady=false or unknown node, taint eviction of pod will happen and pod will be marked as not ready
// 3. if node doesn't exist in cache, it will be skipped and handled later by doEvictionPass
func (nc *Controller) processPod(podItem podUpdateItem) {
	defer nc.podUpdateQueue.Done(podItem)
	// 获取Pod对象
	pod, err := nc.podLister.Pods(podItem.namespace).Get(podItem.name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the pod was deleted, there is no need to requeue.
			return
		}
		klog.Warningf("Failed to read pod %v/%v: %v.", podItem.namespace, podItem.name, err)
		nc.podUpdateQueue.AddRateLimited(podItem)
		return
	}

	nodeName := pod.Spec.NodeName

	nodeHealth := nc.nodeHealthMap.getDeepCopy(nodeName)
	if nodeHealth == nil {
		// Node data is not gathered yet or node has beed removed in the meantime.
		// Pod will be handled by doEvictionPass method.
		return
	}

	node, err := nc.nodeLister.Get(nodeName)
	if err != nil {
		klog.Warningf("Failed to read node %v: %v.", nodeName, err)
		nc.podUpdateQueue.AddRateLimited(podItem)
		return
	}

	_, currentReadyCondition := nodeutil.GetNodeCondition(nodeHealth.status, v1.NodeReady)
	if currentReadyCondition == nil {
		// Lack of NodeReady condition may only happen after node addition (or if it will be maliciously deleted).
		// In both cases, the pod will be handled correctly (evicted if needed) during processing
		// of the next node update event.
		return
	}

	pods := []*v1.Pod{pod}
	// In taint-based eviction mode, only node updates are processed by NodeLifecycleController.
	// Pods are processed by TaintManager.
	if !nc.runTaintManager {
		if err := nc.processNoTaintBaseEviction(node, currentReadyCondition, nc.nodeMonitorGracePeriod, pods); err != nil {
			klog.Warningf("Unable to process pod %+v eviction from node %v: %v.", podItem, nodeName, err)
			nc.podUpdateQueue.AddRateLimited(podItem)
			return
		}
	}

	if currentReadyCondition.Status != v1.ConditionTrue {
		if err := nodeutil.MarkPodsNotReady(nc.kubeClient, nc.recorder, pods, nodeName); err != nil {
			klog.Warningf("Unable to mark pod %+v NotReady on node %v: %v.", podItem, nodeName, err)
			nc.podUpdateQueue.AddRateLimited(podItem)
		}
	}
}

func (nc *Controller) setLimiterInZone(zone string, zoneSize int, state ZoneState) {
	// 根据不同的zoneState设置对应的驱逐速率
	switch state {
	case stateNormal:
		// Normal驱逐速率为evictionLimiterQPS 每隔10秒清除一个节点
		if nc.runTaintManager {
			nc.zoneNoExecuteTainter[zone].SwapLimiter(nc.evictionLimiterQPS)
		} else {
			nc.zonePodEvictor[zone].SwapLimiter(nc.evictionLimiterQPS)
		}
	case statePartialDisruption:
		if nc.runTaintManager {
			nc.zoneNoExecuteTainter[zone].SwapLimiter(
				nc.enterPartialDisruptionFunc(zoneSize))
		} else {
			nc.zonePodEvictor[zone].SwapLimiter(
				nc.enterPartialDisruptionFunc(zoneSize))
		}
	case stateFullDisruption:
		if nc.runTaintManager {
			nc.zoneNoExecuteTainter[zone].SwapLimiter(
				nc.enterFullDisruptionFunc(zoneSize))
		} else {
			nc.zonePodEvictor[zone].SwapLimiter(
				nc.enterFullDisruptionFunc(zoneSize))
		}
	}
}

// classifyNodes classifies the allNodes to three categories:
//   1. added: the nodes that in 'allNodes', but not in 'knownNodeSet'
//   2. deleted: the nodes that in 'knownNodeSet', but not in 'allNodes'
//   3. newZoneRepresentatives: the nodes that in both 'knownNodeSet' and 'allNodes', but no zone states
func (nc *Controller) classifyNodes(allNodes []*v1.Node) (added, deleted, newZoneRepresentatives []*v1.Node) {
	// 遍历所有node
	for i := range allNodes {
		// 判断node如果不在knownNodeSet里 将其添加到added
		if _, has := nc.knownNodeSet[allNodes[i].Name]; !has {
			added = append(added, allNodes[i])
		} else {
			// Currently, we only consider new zone as updated.
			// node如果在knownNodeSet里
			// 判断node是否在zoneStates里 如果不在将其添加到newZoneRepresentatives
			zone := utilnode.GetZoneKey(allNodes[i])
			if _, found := nc.zoneStates[zone]; !found {
				newZoneRepresentatives = append(newZoneRepresentatives, allNodes[i])
			}
		}
	}

	// If there's a difference between lengths of known Nodes and observed nodes
	// we must have removed some Node.
	// 将多余的node添加到deleted
	if len(nc.knownNodeSet)+len(added) != len(allNodes) {
		knowSetCopy := map[string]*v1.Node{}
		for k, v := range nc.knownNodeSet {
			knowSetCopy[k] = v
		}
		for i := range allNodes {
			delete(knowSetCopy, allNodes[i].Name)
		}
		for i := range knowSetCopy {
			deleted = append(deleted, knowSetCopy[i])
		}
	}
	return
}

// HealthyQPSFunc returns the default value for cluster eviction rate - we take
// nodeNum for consistency with ReducedQPSFunc.
func (nc *Controller) HealthyQPSFunc(nodeNum int) float32 {
	return nc.evictionLimiterQPS
}

// ReducedQPSFunc returns the QPS for when a the cluster is large make
// evictions slower, if they're small stop evictions altogether.
func (nc *Controller) ReducedQPSFunc(nodeNum int) float32 {
	if int32(nodeNum) > nc.largeClusterThreshold {
		return nc.secondaryEvictionLimiterQPS
	}
	return 0
}

// addPodEvictorForNewZone checks if new zone appeared, and if so add new evictor.
func (nc *Controller) addPodEvictorForNewZone(node *v1.Node) {
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()
	zone := utilnode.GetZoneKey(node)
	if _, found := nc.zoneStates[zone]; !found {
		// 新增加的 node 默认的 zoneStates 为 Initial
		nc.zoneStates[zone] = stateInitial
		// 判断是否开启TaintManager 然后将node加入到zonePodEvictor/zoneNoExecuteTainter里
		if !nc.runTaintManager {
			// 将node加入到zonePodEvictor
			nc.zonePodEvictor[zone] =
				scheduler.NewRateLimitedTimedQueue(
					flowcontrol.NewTokenBucketRateLimiter(nc.evictionLimiterQPS, scheduler.EvictionRateLimiterBurst))
		} else {
			// 将node加入到zoneNoExecuteTainter
			nc.zoneNoExecuteTainter[zone] =
				scheduler.NewRateLimitedTimedQueue(
					flowcontrol.NewTokenBucketRateLimiter(nc.evictionLimiterQPS, scheduler.EvictionRateLimiterBurst))
		}
		// Init the metric for the new zone.
		klog.Infof("Initializing eviction metric for zone: %v", zone)
		evictionsNumber.WithLabelValues(zone).Add(0)
	}
}

// cancelPodEviction removes any queued evictions, typically because the node is available again. It
// returns true if an eviction was queued.
func (nc *Controller) cancelPodEviction(node *v1.Node) bool {
	zone := utilnode.GetZoneKey(node)
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()
	if !nc.nodeEvictionMap.setStatus(node.Name, unmarked) {
		klog.V(2).Infof("node %v was unregistered in the meantime - skipping setting status", node.Name)
	}
	wasDeleting := nc.zonePodEvictor[zone].Remove(node.Name)
	if wasDeleting {
		klog.V(2).Infof("Cancelling pod Eviction on Node: %v", node.Name)
		return true
	}
	return false
}

// evictPods:
// - adds node to evictor queue if the node is not marked as evicted.
//   Returns false if the node name was already enqueued.
// - deletes pods immediately if node is already marked as evicted.
//   Returns false, because the node wasn't added to the queue.
func (nc *Controller) evictPods(node *v1.Node, pods []*v1.Pod) (bool, error) {
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()
	status, ok := nc.nodeEvictionMap.getStatus(node.Name)
	if ok && status == evicted {
		// Node eviction already happened for this node.
		// Handling immediate pod deletion.
		_, err := nodeutil.DeletePods(nc.kubeClient, pods, nc.recorder, node.Name, string(node.UID), nc.daemonSetStore)
		if err != nil {
			return false, fmt.Errorf("unable to delete pods from node %q: %v", node.Name, err)
		}
		return false, nil
	}
	if !nc.nodeEvictionMap.setStatus(node.Name, toBeEvicted) {
		klog.V(2).Infof("node %v was unregistered in the meantime - skipping setting status", node.Name)
	}
	return nc.zonePodEvictor[utilnode.GetZoneKey(node)].Add(node.Name, string(node.UID)), nil
}

func (nc *Controller) markNodeForTainting(node *v1.Node, status v1.ConditionStatus) bool {
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()
	if status == v1.ConditionFalse {
		if !taintutils.TaintExists(node.Spec.Taints, NotReadyTaintTemplate) {
			nc.zoneNoExecuteTainter[utilnode.GetZoneKey(node)].SetRemove(node.Name)
		}
	}

	if status == v1.ConditionUnknown {
		if !taintutils.TaintExists(node.Spec.Taints, UnreachableTaintTemplate) {
			nc.zoneNoExecuteTainter[utilnode.GetZoneKey(node)].SetRemove(node.Name)
		}
	}

	return nc.zoneNoExecuteTainter[utilnode.GetZoneKey(node)].Add(node.Name, string(node.UID))
}

func (nc *Controller) markNodeAsReachable(node *v1.Node) (bool, error) {
	// 移除node的taints中Unreachable，NotReady，并从zoneNoExecuteTainter移除
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()
	err := controller.RemoveTaintOffNode(nc.kubeClient, node.Name, node, UnreachableTaintTemplate)
	if err != nil {
		klog.Errorf("Failed to remove taint from node %v: %v", node.Name, err)
		return false, err
	}
	err = controller.RemoveTaintOffNode(nc.kubeClient, node.Name, node, NotReadyTaintTemplate)
	if err != nil {
		klog.Errorf("Failed to remove taint from node %v: %v", node.Name, err)
		return false, err
	}
	return nc.zoneNoExecuteTainter[utilnode.GetZoneKey(node)].Remove(node.Name), nil
}

// ComputeZoneState returns a slice of NodeReadyConditions for all Nodes in a given zone.
// The zone is considered:
// - fullyDisrupted if there're no Ready Nodes,
// - partiallyDisrupted if at least than nc.unhealthyZoneThreshold percent of Nodes are not Ready,
// - normal otherwise
func (nc *Controller) ComputeZoneState(nodeReadyConditions []*v1.NodeCondition) (int, ZoneState) {
	// 根据ready和notready来对node划分到不同zone下
	readyNodes := 0
	notReadyNodes := 0
	for i := range nodeReadyConditions {
		if nodeReadyConditions[i] != nil && nodeReadyConditions[i].Status == v1.ConditionTrue {
			readyNodes++
		} else {
			notReadyNodes++
		}
	}
	switch {
	// fullyDisrupted zone下所有node都处于notready状态
	case readyNodes == 0 && notReadyNodes > 0:
		return notReadyNodes, stateFullDisruption
		// PartialDisruption zone下 notready总占比要>=unhealthyZoneThreshold的值
	case notReadyNodes > 2 && float32(notReadyNodes)/float32(notReadyNodes+readyNodes) >= nc.unhealthyZoneThreshold:
		return notReadyNodes, statePartialDisruption
	default:
		// 不属于以上2种情况
		return notReadyNodes, stateNormal
	}
}

// reconcileNodeLabels reconciles node labels.
func (nc *Controller) reconcileNodeLabels(nodeName string) error {
	node, err := nc.nodeLister.Get(nodeName)
	if err != nil {
		// If node not found, just ignore it.
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if node.Labels == nil {
		// Nothing to reconcile.
		return nil
	}

	labelsToUpdate := map[string]string{}
	for _, r := range labelReconcileInfo {
		primaryValue, primaryExists := node.Labels[r.primaryKey]
		secondaryValue, secondaryExists := node.Labels[r.secondaryKey]

		if !primaryExists {
			// The primary label key does not exist. This should not happen
			// within our supported version skew range, when no external
			// components/factors modifying the node object. Ignore this case.
			continue
		}
		if secondaryExists && primaryValue != secondaryValue {
			// Secondary label exists, but not consistent with the primary
			// label. Need to reconcile.
			labelsToUpdate[r.secondaryKey] = primaryValue

		} else if !secondaryExists && r.ensureSecondaryExists {
			// Apply secondary label based on primary label.
			labelsToUpdate[r.secondaryKey] = primaryValue
		}
	}

	if len(labelsToUpdate) == 0 {
		return nil
	}
	if !nodeutil.AddOrUpdateLabelsOnNode(nc.kubeClient, labelsToUpdate, node) {
		return fmt.Errorf("failed update labels for node %+v", node)
	}
	return nil
}
