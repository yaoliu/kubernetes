/*
Copyright 2015 The Kubernetes Authors.

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

package job

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"

	batch "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	batchinformers "k8s.io/client-go/informers/batch/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	batchv1listers "k8s.io/client-go/listers/batch/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/metrics/prometheus/ratelimiter"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/utils/integer"
)

// 状态更新重试次数
const statusUpdateRetries = 3

// controllerKind contains the schema.GroupVersionKind for this controller type.
var controllerKind = batch.SchemeGroupVersion.WithKind("Job")

var (
	// DefaultJobBackOff is the default backoff period, exported for the e2e test
	DefaultJobBackOff = 10 * time.Second
	// MaxJobBackOff is the max backoff period, exported for the e2e test
	MaxJobBackOff = 360 * time.Second
)

// Controller ensures that all Job objects have corresponding pods to
// run their configured workload.
type Controller struct {
	// 用于访问apiserver的client
	kubeClient clientset.Interface
	// 用于管理pod 会使用访问apiserver的client 对pod进行操作 如新增 删除 更新
	podControl controller.PodControlInterface

	// To allow injection of updateJobStatus for testing.
	// 用于更新job状态的函数
	updateHandler func(job *batch.Job) error
	// 核心功能 syncJob
	syncHandler func(jobKey string) (bool, error)
	// podStoreSynced returns true if the pod store has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	// 用于判断pod是否同步过到cache
	podStoreSynced cache.InformerSynced
	// jobStoreSynced returns true if the job store has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	// 用于判断job是否同步到cache
	jobStoreSynced cache.InformerSynced
	// A TTLCache of pod creates/deletes each rc expects to see
	// 用于维护期望值 记录pods的adds次数 del次数
	expectations controller.ControllerExpectationsInterface

	// A store of jobs
	// 用于获取job元数据
	jobLister batchv1listers.JobLister

	// A store of pods, populated by the podController
	// 用于获取pod元数据
	podStore corelisters.PodLister

	// Jobs that need to be updated
	// 用于存放job数据 syncjob的时候从queue里get一个jobkey进行操作
	queue workqueue.RateLimitingInterface
	// 事件记录器 用于记录事件
	recorder record.EventRecorder
}

// NewController creates a new Job controller that keeps the relevant pods
// in sync with their corresponding Job objects.
func NewController(podInformer coreinformers.PodInformer, jobInformer batchinformers.JobInformer, kubeClient clientset.Interface) *Controller {
	// 创建事件管理器
	eventBroadcaster := record.NewBroadcaster()
	// 设置事件上报到klog
	eventBroadcaster.StartStructuredLogging(0)
	// 设置事件上报到Api Server
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	// 限流策略
	if kubeClient != nil && kubeClient.CoreV1().RESTClient().GetRateLimiter() != nil {
		ratelimiter.RegisterMetricAndTrackRateLimiterUsage("job_controller", kubeClient.CoreV1().RESTClient().GetRateLimiter())
	}

	jm := &Controller{
		// 用于访问apiserver的client
		kubeClient: kubeClient,
		// 用于管理pod 会使用访问apiserver的client 对pod进行操作 如新增 删除 更新
		podControl: controller.RealPodControl{
			// 用于访问apiserver的client
			KubeClient: kubeClient,
			// 事件记录器
			Recorder: eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "job-controller"}),
		},
		// 期望值维护
		expectations: controller.NewControllerExpectations(),
		// workqueue队列
		queue: workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(DefaultJobBackOff, MaxJobBackOff), "job"),
		// 事件记录器
		recorder: eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "job-controller"}),
	}
	// 监听watch job add/update/delete等事件 并且调用对应事件注册的函数
	jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			jm.enqueueController(obj, true)
		},
		UpdateFunc: jm.updateJob,
		DeleteFunc: func(obj interface{}) {
			jm.enqueueController(obj, true)
		},
	})
	jm.jobLister = jobInformer.Lister()
	jm.jobStoreSynced = jobInformer.Informer().HasSynced
	// 监听watch pod add/update/delete等事件 并且调用对应事件注册的函数
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    jm.addPod,
		UpdateFunc: jm.updatePod,
		DeleteFunc: jm.deletePod,
	})
	jm.podStore = podInformer.Lister()
	jm.podStoreSynced = podInformer.Informer().HasSynced
	// 更新job status的函数
	jm.updateHandler = jm.updateJobStatus
	// 核心实现 处理job的函数
	jm.syncHandler = jm.syncJob

	return jm
}

// Run the main goroutine responsible for watching and syncing jobs.
func (jm *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer jm.queue.ShutDown()

	klog.Infof("Starting job controller")
	defer klog.Infof("Shutting down job controller")
	// 等待pod cache && job cache 是否同步完成
	if !cache.WaitForNamedCacheSync("job", stopCh, jm.podStoreSynced, jm.jobStoreSynced) {
		return
	}
	// 开启workers数的gorutinue 每个gorutinue都调用jm.worker
	for i := 0; i < workers; i++ {
		go wait.Until(jm.worker, time.Second, stopCh)
	}

	<-stopCh
}

// getPodJobs returns a list of Jobs that potentially match a Pod.
func (jm *Controller) getPodJobs(pod *v1.Pod) []*batch.Job {
	jobs, err := jm.jobLister.GetPodJobs(pod)
	if err != nil {
		return nil
	}
	if len(jobs) > 1 {
		// ControllerRef will ensure we don't do anything crazy, but more than one
		// item in this list nevertheless constitutes user error.
		utilruntime.HandleError(fmt.Errorf("user error! more than one job is selecting pods with labels: %+v", pod.Labels))
	}
	ret := make([]*batch.Job, 0, len(jobs))
	for i := range jobs {
		ret = append(ret, &jobs[i])
	}
	return ret
}

// resolveControllerRef returns the controller referenced by a ControllerRef,
// or nil if the ControllerRef could not be resolved to a matching controller
// of the correct Kind.
func (jm *Controller) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *batch.Job {
	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	job, err := jm.jobLister.Jobs(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if job.UID != controllerRef.UID {
		// The controller we found with this Name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return job
}

// When a pod is created, enqueue the controller that manages it and update it's expectations.
func (jm *Controller) addPod(obj interface{}) {
	pod := obj.(*v1.Pod)
	if pod.DeletionTimestamp != nil {
		// on a restart of the controller, it's possible a new pod shows up in a state that
		// is already pending deletion. Prevent the pod from being a creation observation.
		jm.deletePod(pod)
		return
	}

	// If it has a ControllerRef, that's all that matters.
	if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
		job := jm.resolveControllerRef(pod.Namespace, controllerRef)
		if job == nil {
			return
		}
		jobKey, err := controller.KeyFunc(job)
		if err != nil {
			return
		}
		jm.expectations.CreationObserved(jobKey)
		jm.enqueueController(job, true)
		return
	}

	// Otherwise, it's an orphan. Get a list of all matching controllers and sync
	// them to see if anyone wants to adopt it.
	// DO NOT observe creation because no controller should be waiting for an
	// orphan.
	for _, job := range jm.getPodJobs(pod) {
		jm.enqueueController(job, true)
	}
}

// When a pod is updated, figure out what job/s manage it and wake them up.
// If the labels of the pod have changed we need to awaken both the old
// and new job. old and cur must be *v1.Pod types.
func (jm *Controller) updatePod(old, cur interface{}) {
	curPod := cur.(*v1.Pod)
	oldPod := old.(*v1.Pod)
	if curPod.ResourceVersion == oldPod.ResourceVersion {
		// Periodic resync will send update events for all known pods.
		// Two different versions of the same pod will always have different RVs.
		return
	}
	if curPod.DeletionTimestamp != nil {
		// when a pod is deleted gracefully it's deletion timestamp is first modified to reflect a grace period,
		// and after such time has passed, the kubelet actually deletes it from the store. We receive an update
		// for modification of the deletion timestamp and expect an job to create more pods asap, not wait
		// until the kubelet actually deletes the pod.
		jm.deletePod(curPod)
		return
	}

	// the only time we want the backoff to kick-in, is when the pod failed
	immediate := curPod.Status.Phase != v1.PodFailed

	curControllerRef := metav1.GetControllerOf(curPod)
	oldControllerRef := metav1.GetControllerOf(oldPod)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// The ControllerRef was changed. Sync the old controller, if any.
		if job := jm.resolveControllerRef(oldPod.Namespace, oldControllerRef); job != nil {
			jm.enqueueController(job, immediate)
		}
	}

	// If it has a ControllerRef, that's all that matters.
	if curControllerRef != nil {
		job := jm.resolveControllerRef(curPod.Namespace, curControllerRef)
		if job == nil {
			return
		}
		jm.enqueueController(job, immediate)
		return
	}

	// Otherwise, it's an orphan. If anything changed, sync matching controllers
	// to see if anyone wants to adopt it now.
	labelChanged := !reflect.DeepEqual(curPod.Labels, oldPod.Labels)
	if labelChanged || controllerRefChanged {
		for _, job := range jm.getPodJobs(curPod) {
			jm.enqueueController(job, immediate)
		}
	}
}

// When a pod is deleted, enqueue the job that manages the pod and update its expectations.
// obj could be an *v1.Pod, or a DeletionFinalStateUnknown marker item.
func (jm *Controller) deletePod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)

	// When a delete is dropped, the relist will notice a pod in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value. Note that this value might be stale. If the pod
	// changed labels the new job will not be woken up till the periodic resync.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a pod %+v", obj))
			return
		}
	}

	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil {
		// No controller should care about orphans being deleted.
		return
	}
	job := jm.resolveControllerRef(pod.Namespace, controllerRef)
	if job == nil {
		return
	}
	jobKey, err := controller.KeyFunc(job)
	if err != nil {
		return
	}
	jm.expectations.DeletionObserved(jobKey)
	jm.enqueueController(job, true)
}

func (jm *Controller) updateJob(old, cur interface{}) {
	oldJob := old.(*batch.Job)
	curJob := cur.(*batch.Job)

	// never return error
	key, err := controller.KeyFunc(curJob)
	if err != nil {
		return
	}
	jm.enqueueController(curJob, true)
	// check if need to add a new rsync for ActiveDeadlineSeconds
	if curJob.Status.StartTime != nil {
		curADS := curJob.Spec.ActiveDeadlineSeconds
		if curADS == nil {
			return
		}
		oldADS := oldJob.Spec.ActiveDeadlineSeconds
		if oldADS == nil || *oldADS != *curADS {
			now := metav1.Now()
			start := curJob.Status.StartTime.Time
			passed := now.Time.Sub(start)
			total := time.Duration(*curADS) * time.Second
			// AddAfter will handle total < passed
			jm.queue.AddAfter(key, total-passed)
			klog.V(4).Infof("job ActiveDeadlineSeconds updated, will rsync after %d seconds", total-passed)
		}
	}
}

// obj could be an *batch.Job, or a DeletionFinalStateUnknown marker item,
// immediate tells the controller to update the status right away, and should
// happen ONLY when there was a successful pod run.
func (jm *Controller) enqueueController(obj interface{}, immediate bool) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}

	backoff := time.Duration(0)
	if !immediate {
		backoff = getBackoff(jm.queue, key)
	}

	// TODO: Handle overlapping controllers better. Either disallow them at admission time or
	// deterministically avoid syncing controllers that fight over pods. Currently, we only
	// ensure that the same controller is synced for a given pod. When we periodically relist
	// all controllers there will still be some replica instability. One way to handle this is
	// by querying the store for all controllers that this rc overlaps, as well as all
	// controllers that overlap this rc, and sorting them.
	jm.queue.AddAfter(key, backoff)
}

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
// It enforces that the syncHandler is never invoked concurrently with the same key.
func (jm *Controller) worker() {
	for jm.processNextWorkItem() {
	}
}

func (jm *Controller) processNextWorkItem() bool {
	// 从queue队列获取一个jobKey jobKey的格式为{nameSpace}/{jobName} 例如:default/pi
	key, quit := jm.queue.Get()
	if quit {
		return false
	}
	// jobKey用完需要告知队列
	defer jm.queue.Done(key)
	// 将jobKey传给jm.syncJob
	forget, err := jm.syncHandler(key.(string))
	if err == nil {
		if forget {
			// 如果执行成功 那么需要从queue里删除jobKey
			jm.queue.Forget(key)
		}
		return true
	}
	// 如果执行失败 打印错误信息 并且把jobKey放回queue里 等待下次sync
	utilruntime.HandleError(fmt.Errorf("Error syncing job: %v", err))
	jm.queue.AddRateLimited(key)

	return true
}

// getPodsForJob returns the set of pods that this Job should manage.
// It also reconciles ControllerRef by adopting/orphaning.
// Note that the returned Pods are pointers into the cache.
// 获取Job下所有相关的Pod
func (jm *Controller) getPodsForJob(j *batch.Job) ([]*v1.Pod, error) {
	// 初始化一个label选择器
	selector, err := metav1.LabelSelectorAsSelector(j.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("couldn't convert Job selector: %v", err)
	}
	// List all pods to include those that don't match the selector anymore
	// but have a ControllerRef pointing to this controller./.
	// 获取job对应namespace下的所有pod
	pods, err := jm.podStore.Pods(j.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	// If any adoptions are attempted, we should first recheck for deletion
	// with an uncached quorum read sometime after listing Pods (see #42639).
	// 重新检查metadata.DeletionTimestamp是否为空 前置条件先根据jobName获取最新的元数据(fresh)和当前的job元数据比较UID是否一致 然后在重新检查
	canAdoptFunc := controller.RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh, err := jm.kubeClient.BatchV1().Jobs(j.Namespace).Get(context.TODO(), j.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != j.UID {
			return nil, fmt.Errorf("original Job %v/%v is gone: got uid %v, wanted %v", j.Namespace, j.Name, fresh.UID, j.UID)
		}
		return fresh, nil
	})
	cm := controller.NewPodControllerRefManager(jm.podControl, j, selector, controllerKind, canAdoptFunc)
	return cm.ClaimPods(pods)
}

// syncJob will sync the job with the given key if it has had its expectations fulfilled, meaning
// it did not expect to see any more of its pods created or deleted. This function is not meant to be invoked
// concurrently with the same key.
func (jm *Controller) syncJob(key string) (bool, error) {
	// startTime和defer配合记录syncJob的耗时
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished syncing job %q (%v)", key, time.Since(startTime))
	}()
	// 将key切分为namespace和name 例如:defalut/pi 切分为default和pi pi为JobName default为namespace
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return false, err
	}
	if len(ns) == 0 || len(name) == 0 {
		return false, fmt.Errorf("invalid job key %q: either namespace or name is missing", key)
	}
	// 根据namespace和name获取Job对象
	sharedJob, err := jm.jobLister.Jobs(ns).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(4).Infof("Job has been deleted: %v", key)
			jm.expectations.DeleteExpectations(key)
			return true, nil
		}
		return false, err
	}
	job := *sharedJob

	// if job was finished previously, we don't want to redo the termination
	// 判断Job是否已经完成 判断条件为Job.Status.Conditions中有Type=Complete或者Failed，Status=True代表此Job已经完成
	if IsJobFinished(&job) {
		return true, nil
	}

	// retrieve the previous number of retry
	// 获取Job的重试次数
	previousRetry := jm.queue.NumRequeues(key)

	// Check the expectations of the job before counting active pods, otherwise a new pod can sneak in
	// and update the expectations after we've retrieved active pods from the store. If a new pod enters
	// the store after we've checked the expectation, the job sync is just deferred till the next relist.
	// 判断Job是否能进行sync
	// 1. 该job的ControlleeExpectations add次数和del次数 都<=0
	// 2. 该job的ControlleeExpectations 已经超过5min没有更新
	// 3. 该job的ControlleeExpectations 不存在
	// 4. 获取该job的ControlleeExpectations失败
	jobNeedsSync := jm.expectations.SatisfiedExpectations(key)
	// 获取Job所关联的Pod
	// 使用job的selector获取可以匹配的pod 如果存在孤儿的就进行关联 如果已经关联的 如果pod label有变化 那就取消关联
	// 疑问 1.匹配规则及方式是怎样的 2.如果确定此pod是孤儿pod 关联的条件是什么 3 如何删掉已经变化的
	// 1. 使用job的selector和pod的label进行匹配
	// 2. 如果是孤儿的pod 那么会进行关联 对pod设置OwnerReferences
	pods, err := jm.getPodsForJob(&job)
	if err != nil {
		return false, err
	}
	// 获取active的Pod 过滤条件 pod.status.phase!=success及failed。DeletionTimestamp=空
	activePods := controller.FilterActivePods(pods)
	active := int32(len(activePods))
	// 获取succeeded的Pod pod.status.phase=Succeeded pod.status.phase=Failed
	succeeded, failed := getStatus(pods)
	conditions := len(job.Status.Conditions)
	// job first start
	// 判断是否为第一次运行，如果是第一次运行那么设置StartTime时间
	if job.Status.StartTime == nil {
		now := metav1.Now()
		job.Status.StartTime = &now
		// enqueue a sync to check if job past ActiveDeadlineSeconds
		// 判断ActiveDeadlineSeconds是否为空，如果不为空，将key放入队列中。等待ActiveDeadlineSeconds时间后进行sync
		if job.Spec.ActiveDeadlineSeconds != nil {
			klog.V(4).Infof("Job %s has ActiveDeadlineSeconds will sync after %d seconds",
				key, *job.Spec.ActiveDeadlineSeconds)
			jm.queue.AddAfter(key, time.Duration(*job.Spec.ActiveDeadlineSeconds)*time.Second)
		}
	}

	var manageJobErr error
	jobFailed := false
	var failureReason string
	var failureMessage string
	// Pod的failed数是否已经大于Job的failed数
	jobHaveNewFailure := failed > job.Status.Failed
	// new failures happen when status does not reflect the failures and active
	// is different than parallelism, otherwise the previous controller loop
	// failed updating status so even if we pick up failure it is not a new one
	// 判断Job的重试次数已经大于job.Spec.BackoffLimit次数 BackoffLimit默认为6次
	// 判断Pod的failed数是否已经大于Job的failed数
	// 判断活跃的pod数是否不等于job所设置的pod的并行数
	exceedsBackoffLimit := jobHaveNewFailure && (active != *job.Spec.Parallelism) &&
		(int32(previousRetry)+1 > *job.Spec.BackoffLimit)
	// exceedsBackoffLimit = true 或者
	if exceedsBackoffLimit || pastBackoffLimitOnFailure(&job, pods) {
		// check if the number of pod restart exceeds backoff (for restart OnFailure only)
		// OR if the number of failed jobs increased since the last syncJob
		jobFailed = true
		failureReason = "BackoffLimitExceeded"
		failureMessage = "Job has reached the specified backoff limit"
	} else if pastActiveDeadline(&job) {
		// 判断job的运行时间是否已经到了job.spec.ActiveDeadlineSeconds的值 如果是那么将Job设置failed状态 原因为DeadlineExceeded
		jobFailed = true
		failureReason = "DeadlineExceeded"
		failureMessage = "Job was active longer than specified deadline"
	}
	// 判断如果job处于failed状态，调用deleteJobPods将此Job的所有Pod并发删除
	if jobFailed {
		errCh := make(chan error, active)
		jm.deleteJobPods(&job, activePods, errCh)
		select {
		case manageJobErr = <-errCh:
			if manageJobErr != nil {
				break
			}
		default:
		}

		// update status values accordingly
		// 将failed的数量设置为active的 将active设置为0
		failed += active
		active = 0
		// 更新Job状态
		job.Status.Conditions = append(job.Status.Conditions, newCondition(batch.JobFailed, failureReason, failureMessage))
		// 记录相应的事件
		jm.recorder.Event(&job, v1.EventTypeWarning, failureReason, failureMessage)
	} else {
		// 如果非failed状态 判断是否可以进行同步 及 job没有被删除 那么进行同步
		if jobNeedsSync && job.DeletionTimestamp == nil {
			// manageJob的主要功能就是来根据job配置的 对pod进行对比 达到期望的状态
			active, manageJobErr = jm.manageJob(activePods, succeeded, &job)
		}
		// 设置完成数 = 成功数
		completions := succeeded
		complete := false
		// 如果没有设置Completions 那么只要有大于一个pod运行完成 并且活跃pod=0 就设置该job为complete状态
		if job.Spec.Completions == nil {
			// This type of job is complete when any pod exits with success.
			// Each pod is capable of
			// determining whether or not the entire Job is done.  Subsequent pods are
			// not expected to fail, but if they do, the failure is ignored.  Once any
			// pod succeeds, the controller waits for remaining pods to finish, and
			// then the job is complete.
			if succeeded > 0 && active == 0 {
				complete = true
			}
		} else {
			// Job specifies a number of completions.  This type of job signals
			// success by having that number of successes.  Since we do not
			// start more pods than there are remaining completions, there should
			// not be any remaining active pods once this count is reached.
			// 如果设置了Completions 那>=Completions就代表该Job已经完成
			if completions >= *job.Spec.Completions {
				complete = true
				//如果active pod 大于0 那么发送事件
				if active > 0 {
					jm.recorder.Event(&job, v1.EventTypeWarning, "TooManyActivePods", "Too many active pods running after completion count reached")
				}
				// 如果实际完成数大于job所设置的完成数 那么发送事件
				if completions > *job.Spec.Completions {
					jm.recorder.Event(&job, v1.EventTypeWarning, "TooManySucceededPods", "Too many succeeded pods running after completion count reached")
				}
			}
		}
		//如果job完成了 设置job的状态及记录对应事件
		if complete {
			job.Status.Conditions = append(job.Status.Conditions, newCondition(batch.JobComplete, "", ""))
			now := metav1.Now()
			job.Status.CompletionTime = &now
			jm.recorder.Event(&job, v1.EventTypeNormal, "Completed", "Job completed")
		}
	}

	forget := false
	// Check if the number of jobs succeeded increased since the last check. If yes "forget" should be true
	// This logic is linked to the issue: https://github.com/kubernetes/kubernetes/issues/56853 that aims to
	// improve the Job backoff policy when parallelism > 1 and few Jobs failed but others succeed.
	// In this case, we should clear the backoff delay.
	if job.Status.Succeeded < succeeded {
		forget = true
	}
	// 更新job的状态 将状态更新到apiserver
	// no need to update the job if the status hasn't changed since last time
	if job.Status.Active != active || job.Status.Succeeded != succeeded || job.Status.Failed != failed || len(job.Status.Conditions) != conditions {
		job.Status.Active = active
		job.Status.Succeeded = succeeded
		job.Status.Failed = failed

		if err := jm.updateHandler(&job); err != nil {
			return forget, err
		}
		// 如果job所对应的pod 有failed 并且该job的状态为完成 那么forget=fasle 这个jobKey还需要放回queue里 等待下次sync
		if jobHaveNewFailure && !IsJobFinished(&job) {
			// returning an error will re-enqueue Job after the backoff period
			return forget, fmt.Errorf("failed pod(s) detected for job key %q", key)
		}

		forget = true
	}

	return forget, manageJobErr
}

func (jm *Controller) deleteJobPods(job *batch.Job, pods []*v1.Pod, errCh chan<- error) {
	// TODO: below code should be replaced with pod termination resulting in
	// pod failures, rather than killing pods. Unfortunately none such solution
	// exists ATM. There's an open discussion in the topic in
	// https://github.com/kubernetes/kubernetes/issues/14602 which might give
	// some sort of solution to above problem.
	// kill remaining active pods
	// 并发删除Pod
	wait := sync.WaitGroup{}
	nbPods := len(pods)
	wait.Add(nbPods)
	for i := int32(0); i < int32(nbPods); i++ {
		go func(ix int32) {
			defer wait.Done()
			if err := jm.podControl.DeletePod(job.Namespace, pods[ix].Name, job); err != nil && !apierrors.IsNotFound(err) {
				defer utilruntime.HandleError(err)
				klog.V(2).Infof("Failed to delete %v, job %q/%q deadline exceeded", pods[ix].Name, job.Namespace, job.Name)
				errCh <- err
			}
		}(i)
	}
	wait.Wait()
}

// pastBackoffLimitOnFailure checks if container restartCounts sum exceeds BackoffLimit
// this method applies only to pods with restartPolicy == OnFailure
func pastBackoffLimitOnFailure(job *batch.Job, pods []*v1.Pod) bool {
	// 判断pod的重启策略!=OnFailure
	if job.Spec.Template.Spec.RestartPolicy != v1.RestartPolicyOnFailure {
		return false
	}
	// 如果pod的重启策略==OnFailure 遍历pod
	result := int32(0)
	for i := range pods {
		po := pods[i]
		// job重试的次数 = pods InitContainerStatuses RestartCount 和 ContainerStatuses RestartCount
		if po.Status.Phase == v1.PodRunning || po.Status.Phase == v1.PodPending {
			for j := range po.Status.InitContainerStatuses {
				stat := po.Status.InitContainerStatuses[j]
				result += stat.RestartCount
			}
			for j := range po.Status.ContainerStatuses {
				stat := po.Status.ContainerStatuses[j]
				result += stat.RestartCount
			}
		}
	}
	// 如果默认BackoffLimit=0 那么判断pod是否重启次数大于0
	if *job.Spec.BackoffLimit == 0 {
		return result > 0
	}
	// 比较重启次数是否大于等于job的BackoffLimit
	return result >= *job.Spec.BackoffLimit
}

// pastActiveDeadline checks if job has ActiveDeadlineSeconds field set and if it is exceeded.
func pastActiveDeadline(job *batch.Job) bool {
	if job.Spec.ActiveDeadlineSeconds == nil || job.Status.StartTime == nil {
		return false
	}
	now := metav1.Now()
	start := job.Status.StartTime.Time
	duration := now.Time.Sub(start)
	allowedDuration := time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second
	return duration >= allowedDuration
}

func newCondition(conditionType batch.JobConditionType, reason, message string) batch.JobCondition {
	return batch.JobCondition{
		Type:               conditionType,
		Status:             v1.ConditionTrue,
		LastProbeTime:      metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// getStatus returns no of succeeded and failed pods running a job
//
func getStatus(pods []*v1.Pod) (succeeded, failed int32) {
	succeeded = int32(filterPods(pods, v1.PodSucceeded))
	failed = int32(filterPods(pods, v1.PodFailed))
	return
}

// manageJob is the core method responsible for managing the number of running
// pods according to what is specified in the job.Spec.
// Does NOT modify <activePods>.
func (jm *Controller) manageJob(activePods []*v1.Pod, succeeded int32, job *batch.Job) (int32, error) {
	var activeLock sync.Mutex
	active := int32(len(activePods))
	// 获取Job并行数量
	parallelism := *job.Spec.Parallelism
	// 根据job对象获取jobkey jobkey的格式为 {namespace}/{job} 例如default/pi
	jobKey, err := controller.KeyFunc(job)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for job %#v: %v", job, err))
		return 0, nil
	}

	var errCh chan error
	// 如果活跃pod数大于并行数 那么需要删除多余的active状态的pod
	if active > parallelism {
		// 计算多余pod数量
		diff := active - parallelism
		errCh = make(chan error, diff)
		jm.expectations.ExpectDeletions(jobKey, int(diff))
		klog.V(4).Infof("Too many pods running job %q, need %d, deleting %d", jobKey, parallelism, diff)
		// Sort the pods in the order such that not-ready < ready, unscheduled
		// < scheduled, and pending < running. This ensures that we delete pods
		// in the earlier stages whenever possible.
		// 进行优先级排序
		// 1. Unassigned < assigned //按照pod 分配(调度)/未分配(未调度) 到node进行比较
		// 2. PodPending < PodUnknown < PodRunning //按照pod.status.phase进行比较
		// 3. Not ready < ready //按照pod是否已经就绪进行比较
		// 4. Been ready for empty time < less time < more time
		// 5. Pods with containers with higher restart counts < lower restart counts  //按照pod重启次数进行比较
		// 6. Empty creation time pods < newer pods < older pods
		// 按照以上几种优先级对acticepod排序
		sort.Sort(controller.ActivePods(activePods))

		active -= diff
		// 将多余的pod并发删除 按照排序后的active pod 从头进行删除 删除diff个pod
		wait := sync.WaitGroup{}
		wait.Add(int(diff))
		for i := int32(0); i < diff; i++ {
			go func(ix int32) {
				defer wait.Done()
				if err := jm.podControl.DeletePod(job.Namespace, activePods[ix].Name, job); err != nil {
					// Decrement the expected number of deletes because the informer won't observe this deletion
					// 如果pod删除失败 则需要回滚active值 把失败的pod数量加回来 并且更新jobkey的expectations
					jm.expectations.DeletionObserved(jobKey)
					if !apierrors.IsNotFound(err) {
						klog.V(2).Infof("Failed to delete %v, decremented expectations for job %q/%q", activePods[ix].Name, job.Namespace, job.Name)
						activeLock.Lock()
						active++
						activeLock.Unlock()
						errCh <- err
						utilruntime.HandleError(err)
					}

				}
			}(i)
		}
		wait.Wait()

	} else if active < parallelism {
		// 如果活跃pod(active pod)数小于Job设置的并行pod数量(parallelism) 那么需要创建pod 使活跃pod数和job设置的pod数量保持一致
		// 主要逻辑为计算出diff值 然后以倍数进行创建diff个pod
		// 如果job.Spec.Completions==nil succeeded>0  wantActive=active diff = 0
		// 如果job.Spec.Completions==nil succeeded=0  wantActive=parallelism diff=(parallelism-active)
		// 如果job.Spec.Completions!=nil diff=(job.Spec.Completions-successed-active)
		wantActive := int32(0)
		if job.Spec.Completions == nil {
			// Job does not specify a number of completions.  Therefore, number active
			// should be equal to parallelism, unless the job has seen at least
			// once success, in which leave whatever is running, running.
			if succeeded > 0 {
				wantActive = active
			} else {
				wantActive = parallelism
			}
		} else {
			// Job specifies a specific number of completions.  Therefore, number
			// active should not ever exceed number of remaining completions.
			wantActive = *job.Spec.Completions - succeeded
			if wantActive > parallelism {
				wantActive = parallelism
			}
		}
		diff := wantActive - active
		if diff < 0 {
			utilruntime.HandleError(fmt.Errorf("More active than wanted: job %q, want %d, have %d", jobKey, wantActive, active))
			diff = 0
		}
		if diff == 0 {
			return active, nil
		}
		jm.expectations.ExpectCreations(jobKey, int(diff))
		errCh = make(chan error, diff)
		klog.V(4).Infof("Too few pods running job %q, need %d, creating %d", jobKey, wantActive, diff)

		active += diff
		wait := sync.WaitGroup{}

		// Batch the pod creates. Batch sizes start at SlowStartInitialBatchSize
		// and double with each successful iteration in a kind of "slow start".
		// This handles attempts to start large numbers of pods that would
		// likely all fail with the same error. For example a project with a
		// low quota that attempts to create a large number of pods will be
		// prevented from spamming the API service with the pod create requests
		// after one of its pods fails.  Conveniently, this also prevents the
		// event spam that those failures would generate.
		// 从1开始创建，每次2的倍数的进行创建 例如 1、2、4、8......，呈指数级增长
		for batchSize := int32(integer.IntMin(int(diff), controller.SlowStartInitialBatchSize)); diff > 0; batchSize = integer.Int32Min(2*batchSize, diff) {
			errorCount := len(errCh)
			wait.Add(int(batchSize))
			for i := int32(0); i < batchSize; i++ {
				go func() {
					defer wait.Done()
					// 创建pod
					err := jm.podControl.CreatePodsWithControllerRef(job.Namespace, &job.Spec.Template, job, metav1.NewControllerRef(job, controllerKind))
					if err != nil {
						if errors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
							// If the namespace is being torn down, we can safely ignore
							// this error since all subsequent creations will fail.
							return
						}
					}
					if err != nil {
						defer utilruntime.HandleError(err)
						// Decrement the expected number of creates because the informer won't observe this pod
						// 如果pod删除失败 则需要回滚active值 把失败的pod数量减回来 并且创建jobkey的expectations
						klog.V(2).Infof("Failed creation, decrementing expectations for job %q/%q", job.Namespace, job.Name)
						jm.expectations.CreationObserved(jobKey)
						activeLock.Lock()
						active--
						activeLock.Unlock()
						errCh <- err
					}
				}()
			}
			wait.Wait()
			// any skipped pods that we never attempted to start shouldn't be expected.
			// 计算剩余未执行的pods数量，并更新ControllerExpectations的adds值
			// 统计这次分批创建pods有失败的情况，则不在处理后续未执行的pods
			skippedPods := diff - batchSize
			if errorCount < len(errCh) && skippedPods > 0 {
				klog.V(2).Infof("Slow-start failure. Skipping creation of %d pods, decrementing expectations for job %q/%q", skippedPods, job.Namespace, job.Name)
				active -= skippedPods
				for i := int32(0); i < skippedPods; i++ {
					// Decrement the expected number of creates because the informer won't observe this pod
					jm.expectations.CreationObserved(jobKey)
				}
				// The skipped pods will be retried later. The next controller resync will
				// retry the slow start process.
				break
			}
			diff -= batchSize
		}
	}

	select {
	case err := <-errCh:
		// all errors have been reported before, we only need to inform the controller that there was an error and it should re-try this job once more next time.
		if err != nil {
			return active, err
		}
	default:
	}

	return active, nil
}

func (jm *Controller) updateJobStatus(job *batch.Job) error {
	jobClient := jm.kubeClient.BatchV1().Jobs(job.Namespace)
	var err error
	for i := 0; i <= statusUpdateRetries; i = i + 1 {
		var newJob *batch.Job
		newJob, err = jobClient.Get(context.TODO(), job.Name, metav1.GetOptions{})
		if err != nil {
			break
		}
		newJob.Status = job.Status
		if _, err = jobClient.UpdateStatus(context.TODO(), newJob, metav1.UpdateOptions{}); err == nil {
			break
		}
	}

	return err
}

func getBackoff(queue workqueue.RateLimitingInterface, key interface{}) time.Duration {
	exp := queue.NumRequeues(key)

	if exp <= 0 {
		return time.Duration(0)
	}

	// The backoff is capped such that 'calculated' value never overflows.
	backoff := float64(DefaultJobBackOff.Nanoseconds()) * math.Pow(2, float64(exp-1))
	if backoff > math.MaxInt64 {
		return MaxJobBackOff
	}

	calculated := time.Duration(backoff)
	if calculated > MaxJobBackOff {
		return MaxJobBackOff
	}
	return calculated
}

// filterPods returns pods based on their phase.
func filterPods(pods []*v1.Pod, phase v1.PodPhase) int {
	result := 0
	for i := range pods {
		if phase == pods[i].Status.Phase {
			result++
		}
	}
	return result
}
