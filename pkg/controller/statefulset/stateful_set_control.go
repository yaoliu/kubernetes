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

package statefulset

import (
	"math"
	"sort"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/history"
)

// StatefulSetControl implements the control logic for updating StatefulSets and their children Pods. It is implemented
// as an interface to allow for extensions that provide different semantics. Currently, there is only one implementation.
type StatefulSetControlInterface interface {
	// UpdateStatefulSet implements the control logic for Pod creation, update, and deletion, and
	// persistent volume creation, update, and deletion.
	// If an implementation returns a non-nil error, the invocation will be retried using a rate-limited strategy.
	// Implementors should sink any errors that they do not wish to trigger a retry, and they may feel free to
	// exit exceptionally at any point provided they wish the update to be re-run at a later point in time.
	// UpdateStatefulSet实现用于Pod创建，更新和删除以及持久卷创建，更新和删除的控制逻辑。
	// 如果返回非空错误，则将使用速率限制策略重试调用。 然后等待下次syncloop
	UpdateStatefulSet(set *apps.StatefulSet, pods []*v1.Pod) error
	// ListRevisions returns a array of the ControllerRevisions that represent the revisions of set. If the returned
	// error is nil, the returns slice of ControllerRevisions is valid.
	// 返回所有和set对象有关的ControllerRevision slice
	ListRevisions(set *apps.StatefulSet) ([]*apps.ControllerRevision, error)
	// AdoptOrphanRevisions adopts any orphaned ControllerRevisions that match set's Selector. If all adoptions are
	// successful the returned error is nil.
	AdoptOrphanRevisions(set *apps.StatefulSet, revisions []*apps.ControllerRevision) error
}

// NewDefaultStatefulSetControl returns a new instance of the default implementation StatefulSetControlInterface that
// implements the documented semantics for StatefulSets. podControl is the PodControlInterface used to create, update,
// and delete Pods and to create PersistentVolumeClaims. statusUpdater is the StatefulSetStatusUpdaterInterface used
// to update the status of StatefulSets. You should use an instance returned from NewRealStatefulPodControl() for any
// scenario other than testing.
func NewDefaultStatefulSetControl(
	podControl StatefulPodControlInterface,
	statusUpdater StatefulSetStatusUpdaterInterface,
	controllerHistory history.Interface,
	recorder record.EventRecorder) StatefulSetControlInterface {
	return &defaultStatefulSetControl{podControl, statusUpdater, controllerHistory, recorder}
}

type defaultStatefulSetControl struct {
	// 用于对pod进行创建/删除/更新
	podControl StatefulPodControlInterface
	// 用于对statefulset 进行更新
	statusUpdater StatefulSetStatusUpdaterInterface
	// 操作controllerrevision
	controllerHistory history.Interface
	// 事件记录器
	recorder record.EventRecorder
}

// UpdateStatefulSet executes the core logic loop for a stateful set, applying the predictable and
// consistent monotonic update strategy by default - scale up proceeds in ordinal order, no new pod
// is created while any pod is unhealthy, and pods are terminated in descending order. The burst
// strategy allows these constraints to be relaxed - pods will be created and deleted eagerly and
// in no particular order. Clients using the burst strategy should be careful to ensure they
// understand the consistency implications of having unpredictable numbers of pods available.
func (ssc *defaultStatefulSetControl) UpdateStatefulSet(set *apps.StatefulSet, pods []*v1.Pod) error {

	// list all revisions and sort them
	// 获取所有ControllerRevisions
	revisions, err := ssc.ListRevisions(set)
	if err != nil {
		return err
	}
	// 根据创建时间进行排序
	history.SortControllerRevisions(revisions)
	// 执行更新操作 并获取当前修订版本号和更新修订版本号
	currentRevision, updateRevision, err := ssc.performUpdate(set, pods, revisions)
	if err != nil {
		return utilerrors.NewAggregate([]error{err, ssc.truncateHistory(set, pods, revisions, currentRevision, updateRevision)})
	}

	// maintain the set's revision history limit
	// 清理过期的ControllerRevision
	return ssc.truncateHistory(set, pods, revisions, currentRevision, updateRevision)
}

func (ssc *defaultStatefulSetControl) performUpdate(
	set *apps.StatefulSet, pods []*v1.Pod, revisions []*apps.ControllerRevision) (*apps.ControllerRevision, *apps.ControllerRevision, error) {

	// get the current, and update revisions
	// 获取当前修订版本,更新修订版本，及碰撞hash种子
	currentRevision, updateRevision, collisionCount, err := ssc.getStatefulSetRevisions(set, revisions)
	if err != nil {
		return currentRevision, updateRevision, err
	}

	// perform the main update function and get the status
	// 执行主要更新操作并且获取状态
	status, err := ssc.updateStatefulSet(set, currentRevision, updateRevision, collisionCount, pods)
	if err != nil {
		return currentRevision, updateRevision, err
	}

	// update the set's status
	// 更新状态
	err = ssc.updateStatefulSetStatus(set, status)
	if err != nil {
		return currentRevision, updateRevision, err
	}

	klog.V(4).Infof("StatefulSet %s/%s pod status replicas=%d ready=%d current=%d updated=%d",
		set.Namespace,
		set.Name,
		status.Replicas,
		status.ReadyReplicas,
		status.CurrentReplicas,
		status.UpdatedReplicas)

	klog.V(4).Infof("StatefulSet %s/%s revisions current=%s update=%s",
		set.Namespace,
		set.Name,
		status.CurrentRevision,
		status.UpdateRevision)

	return currentRevision, updateRevision, nil
}

func (ssc *defaultStatefulSetControl) ListRevisions(set *apps.StatefulSet) ([]*apps.ControllerRevision, error) {
	// 创建selector选择器
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	return ssc.controllerHistory.ListControllerRevisions(set, selector)
}

func (ssc *defaultStatefulSetControl) AdoptOrphanRevisions(
	set *apps.StatefulSet,
	revisions []*apps.ControllerRevision) error {
	for i := range revisions {
		adopted, err := ssc.controllerHistory.AdoptControllerRevision(set, controllerKind, revisions[i])
		if err != nil {
			return err
		}
		revisions[i] = adopted
	}
	return nil
}

// truncateHistory truncates any non-live ControllerRevisions in revisions from set's history. The UpdateRevision and
// CurrentRevision in set's Status are considered to be live. Any revisions associated with the Pods in pods are also
// considered to be live. Non-live revisions are deleted, starting with the revision with the lowest Revision, until
// only RevisionHistoryLimit revisions remain. If the returned error is nil the operation was successful. This method
// expects that revisions is sorted when supplied.
func (ssc *defaultStatefulSetControl) truncateHistory(
	set *apps.StatefulSet,
	pods []*v1.Pod,
	revisions []*apps.ControllerRevision,
	current *apps.ControllerRevision,
	update *apps.ControllerRevision) error {
	history := make([]*apps.ControllerRevision, 0, len(revisions))
	// mark all live revisions
	live := map[string]bool{}
	// 将当前的修订版本加入live
	if current != nil {
		live[current.Name] = true
	}
	// 将更新的修订版本加入live
	if update != nil {
		live[update.Name] = true
	}
	// 遍历所有的pod 获取每个pod的revision 加入live
	for i := range pods {
		live[getPodRevision(pods[i])] = true
	}
	// collect live revisions and historic revisions
	// 遍历所有revision 如果不再live里 则加入到history
	for i := range revisions {
		if !live[revisions[i].Name] {
			history = append(history, revisions[i])
		}
	}
	// 按照set.Spec.RevisionHistoryLimit进行删除多余的revision
	historyLen := len(history)
	historyLimit := int(*set.Spec.RevisionHistoryLimit)
	if historyLen <= historyLimit {
		return nil
	}
	// delete any non-live history to maintain the revision limit.
	history = history[:(historyLen - historyLimit)]
	for i := 0; i < len(history); i++ {
		if err := ssc.controllerHistory.DeleteControllerRevision(history[i]); err != nil {
			return err
		}
	}
	return nil
}

// getStatefulSetRevisions returns the current and update ControllerRevisions for set. It also
// returns a collision count that records the number of name collisions set saw when creating
// new ControllerRevisions. This count is incremented on every name collision and is used in
// building the ControllerRevision names for name collision avoidance. This method may create
// a new revision, or modify the Revision of an existing revision if an update to set is detected.
// This method expects that revisions is sorted when supplied.
func (ssc *defaultStatefulSetControl) getStatefulSetRevisions(
	set *apps.StatefulSet,
	revisions []*apps.ControllerRevision) (*apps.ControllerRevision, *apps.ControllerRevision, int32, error) {
	var currentRevision, updateRevision *apps.ControllerRevision
	// 计算ControllerRevision数量
	revisionCount := len(revisions)
	// 对ControllerRevision进行排序
	history.SortControllerRevisions(revisions)

	// Use a local copy of set.Status.CollisionCount to avoid modifying set.Status directly.
	// This copy is returned so the value gets carried over to set.Status in updateStatefulSet.
	// 用来hash的种子
	var collisionCount int32
	if set.Status.CollisionCount != nil {
		collisionCount = *set.Status.CollisionCount
	}

	// create a new revision from the current set
	// 创建新的ControllerRevision
	updateRevision, err := newRevision(set, nextRevision(revisions), &collisionCount)
	if err != nil {
		return nil, nil, collisionCount, err
	}

	// find any equivalent revisions
	// 查找和updateRevision相等的controllerrevisions
	equalRevisions := history.FindEqualRevisions(revisions, updateRevision)
	equalCount := len(equalRevisions)
	// 根据不同条件来获取最终的updateRevision
	// 如果equalCount>0代表有相等的 并且最后一个
	if equalCount > 0 && history.EqualRevision(revisions[revisionCount-1], equalRevisions[equalCount-1]) {
		// if the equivalent revision is immediately prior the update revision has not changed
		updateRevision = revisions[revisionCount-1]
	} else if equalCount > 0 {
		// if the equivalent revision is not immediately prior we will roll back by incrementing the
		// Revision of the equivalent revision
		updateRevision, err = ssc.controllerHistory.UpdateControllerRevision(
			equalRevisions[equalCount-1],
			updateRevision.Revision)
		if err != nil {
			return nil, nil, collisionCount, err
		}
	} else {
		//if there is no equivalent revision we create a new one
		// 如果没有相同的 那就创建最新的ControllerRevision
		updateRevision, err = ssc.controllerHistory.CreateControllerRevision(set, updateRevision, &collisionCount)
		if err != nil {
			return nil, nil, collisionCount, err
		}
	}

	// attempt to find the revision that corresponds to the current revision
	// 获取当前修订版本(遍历revisions，尝试查找和当前sts.status.currentRevision一致的revision修订版本)
	for i := range revisions {
		if revisions[i].Name == set.Status.CurrentRevision {
			currentRevision = revisions[i]
			break
		}
	}

	// if the current revision is nil we initialize the history by setting it to the update revision
	// 如果当前修订版本为空 则将其设置为更新版本来完成初始化历史记录
	if currentRevision == nil {
		currentRevision = updateRevision
	}

	return currentRevision, updateRevision, collisionCount, nil
}

// updateStatefulSet performs the update function for a StatefulSet. This method creates, updates, and deletes Pods in
// the set in order to conform the system to the target state for the set. The target state always contains
// set.Spec.Replicas Pods with a Ready Condition. If the UpdateStrategy.Type for the set is
// RollingUpdateStatefulSetStrategyType then all Pods in the set must be at set.Status.CurrentRevision.
// If the UpdateStrategy.Type for the set is OnDeleteStatefulSetStrategyType, the target state implies nothing about
// the revisions of Pods in the set. If the UpdateStrategy.Type for the set is PartitionStatefulSetStrategyType, then
// all Pods with ordinal less than UpdateStrategy.Partition.Ordinal must be at Status.CurrentRevision and all other
// Pods must be at Status.UpdateRevision. If the returned error is nil, the returned StatefulSetStatus is valid and the
// update must be recorded. If the error is not nil, the method should be retried until successful.
func (ssc *defaultStatefulSetControl) updateStatefulSet(
	set *apps.StatefulSet,
	currentRevision *apps.ControllerRevision,
	updateRevision *apps.ControllerRevision,
	collisionCount int32,
	pods []*v1.Pod) (*apps.StatefulSetStatus, error) {
	// 核心方法 创建，删除，更新 都在这个方法里完成
	// get the current and update revisions of the set.
	// 获取最新版本及更新版本(将controllerrevision.data转换为sts对象)
	currentSet, err := ApplyRevision(set, currentRevision)
	if err != nil {
		return nil, err
	}
	updateSet, err := ApplyRevision(set, updateRevision)
	if err != nil {
		return nil, err
	}

	// set the generation, and revisions in the returned status
	// 计算状态
	status := apps.StatefulSetStatus{}
	status.ObservedGeneration = set.Generation
	status.CurrentRevision = currentRevision.Name
	status.UpdateRevision = updateRevision.Name
	status.CollisionCount = new(int32)
	*status.CollisionCount = collisionCount

	replicaCount := int(*set.Spec.Replicas)
	// slice that will contain all Pods such that 0 <= getOrdinal(pod) < set.Spec.Replicas
	// replicas为有效副本集 所有sts.pods 按照ord值进行分组 将0 <= getOrdinal(pod) < set.Spec.Replicas放入到replicas里
	replicas := make([]*v1.Pod, replicaCount)
	// slice that will contain all Pods such that set.Spec.Replicas <= getOrdinal(pod)
	// condemned为要删除副本集 所有sts.pods 按照ord值进行分组 将set.Spec.Replicas <= getOrdinal(pod)放入到replicas里
	condemned := make([]*v1.Pod, 0, len(pods))
	unhealthy := 0
	firstUnhealthyOrdinal := math.MaxInt32
	var firstUnhealthyPod *v1.Pod

	// First we partition pods into two lists valid replicas and condemned Pods
	// 遍历所有的pod 将pod划分到replicas and condemned中
	for i := range pods {
		status.Replicas++

		// count the number of running and ready replicas
		// 判断pod处于running && ready状态 status.ReadyReplicas++
		if isRunningAndReady(pods[i]) {
			status.ReadyReplicas++
		}

		// count the number of current and update replicas
		// 判断pod正在创建并且未处于删除状态中
		if isCreated(pods[i]) && !isTerminating(pods[i]) {
			if getPodRevision(pods[i]) == currentRevision.Name {
				status.CurrentReplicas++
			}
			if getPodRevision(pods[i]) == updateRevision.Name {
				status.UpdatedReplicas++
			}
		}
		// 根据ord序号将pod划分到replicas and condemned中
		// 此处为了扩缩容
		if ord := getOrdinal(pods[i]); 0 <= ord && ord < replicaCount {
			// if the ordinal of the pod is within the range of the current number of replicas,
			// insert it at the indirection of its ordinal
			replicas[ord] = pods[i]

		} else if ord >= replicaCount {
			// if the ordinal is greater than the number of replicas add it to the condemned list
			condemned = append(condemned, pods[i])
		}
		// If the ordinal could not be parsed (ord < 0), ignore the Pod.
	}

	// for any empty indices in the sequence [0,set.Spec.Replicas) create a new Pod at the correct revision
	// 将replicas中缺失的位置创建出对应的Pod ord序号是有序自增的 0-->1-->2-->3
	for ord := 0; ord < replicaCount; ord++ {
		if replicas[ord] == nil {
			replicas[ord] = newVersionedStatefulSetPod(
				currentSet,
				updateSet,
				currentRevision.Name,
				updateRevision.Name, ord)
		}
	}

	// sort the condemned Pods by their ordinals
	// 根据ord序号对condemned进行排序
	sort.Sort(ascendingOrdinal(condemned))

	// find the first unhealthy Pod
	// 查找出第一个不健康的Pod 获取其ord序号
	for i := range replicas {
		if !isHealthy(replicas[i]) {
			unhealthy++
			// 判断如果pod ord < firstUnhealthyOrdinal 那么更新firstUnhealthyOrdinal=当前pod
			if ord := getOrdinal(replicas[i]); ord < firstUnhealthyOrdinal {
				firstUnhealthyOrdinal = ord
				firstUnhealthyPod = replicas[i]
			}
		}
	}
	// 查找出第一个不健康的Pod 获取其ord序号
	for i := range condemned {
		if !isHealthy(condemned[i]) {
			unhealthy++
			// 判断如果pod ord < firstUnhealthyOrdinal 那么更新firstUnhealthyOrdinal=当前pod
			if ord := getOrdinal(condemned[i]); ord < firstUnhealthyOrdinal {
				firstUnhealthyOrdinal = ord
				firstUnhealthyPod = condemned[i]
			}
		}
	}

	if unhealthy > 0 {
		klog.V(4).Infof("StatefulSet %s/%s has %d unhealthy Pods starting with %s",
			set.Namespace,
			set.Name,
			unhealthy,
			firstUnhealthyPod.Name)
	}

	// If the StatefulSet is being deleted, don't do anything other than updating
	// status.
	// 判断sts对象是否处于删除中
	if set.DeletionTimestamp != nil {
		return &status, nil
	}

	monotonic := !allowsBurst(set)

	// Examine each replica with respect to its ordinal
	for i := range replicas {
		// delete and recreate failed pods
		// 判断如果失败 则删除并重新创建
		if isFailed(replicas[i]) {
			// 事件记录
			ssc.recorder.Eventf(set, v1.EventTypeWarning, "RecreatingFailedPod",
				"StatefulSet %s/%s is recreating failed Pod %s",
				set.Namespace,
				set.Name,
				replicas[i].Name)
			// 删除pod
			if err := ssc.podControl.DeleteStatefulPod(set, replicas[i]); err != nil {
				return &status, err
			}
			if getPodRevision(replicas[i]) == currentRevision.Name {
				status.CurrentReplicas--
			}
			if getPodRevision(replicas[i]) == updateRevision.Name {
				status.UpdatedReplicas--
			}
			status.Replicas--
			// 创建pod
			replicas[i] = newVersionedStatefulSetPod(
				currentSet,
				updateSet,
				currentRevision.Name,
				updateRevision.Name,
				i)
		}
		// If we find a Pod that has not been created we create the Pod
		//  判断如果pod.Status.Phase == "" 需要创建pod
		if !isCreated(replicas[i]) {
			// 创建pod
			if err := ssc.podControl.CreateStatefulPod(set, replicas[i]); err != nil {
				return &status, err
			}
			status.Replicas++
			// 判断 pod.label["revision"] == 当前revision
			if getPodRevision(replicas[i]) == currentRevision.Name {
				status.CurrentReplicas++
			}
			// 判断 pod.label["revision"] == 当前revision
			if getPodRevision(replicas[i]) == updateRevision.Name {
				status.UpdatedReplicas++
			}

			// if the set does not allow bursting, return immediately
			if monotonic {
				return &status, nil
			}
			// pod created, no more work possible for this round
			continue
		}
		// If we find a Pod that is currently terminating, we must wait until graceful deletion
		// completes before we continue to make progress.
		// 判断如果pod处于删除中
		if isTerminating(replicas[i]) && monotonic {
			klog.V(4).Infof(
				"StatefulSet %s/%s is waitingw for Pod %s to Terminate",
				set.Namespace,
				set.Name,
				replicas[i].Name)
			return &status, nil
		}
		// If we have a Pod that has been created but is not running and ready we can not make progress.
		// We must ensure that all for each Pod, when we create it, all of its predecessors, with respect to its
		// ordinal, are Running and Ready.
		// 判断如果pod状态不是running && ready状态
		// 按照statefulSet的定义 必须上一个pod status = running && ready 才能创建下一个
		if !isRunningAndReady(replicas[i]) && monotonic {
			klog.V(4).Infof(
				"StatefulSet %s/%s is waiting for Pod %s to be Running and Ready",
				set.Namespace,
				set.Name,
				replicas[i].Name)
			return &status, nil
		}
		// Enforce the StatefulSet invariants
		// 检查pod元信息和存储信息是否和sts匹配 如果不匹配 则需要更新状态
		if identityMatches(set, replicas[i]) && storageMatches(set, replicas[i]) {
			continue
		}
		// Make a deep copy so we don't mutate the shared cache
		replica := replicas[i].DeepCopy()
		// 更新pod状态
		if err := ssc.podControl.UpdateStatefulPod(updateSet, replica); err != nil {
			return &status, err
		}
	}

	// At this point, all of the current Replicas are Running and Ready, we can consider termination.
	// We will wait for all predecessors to be Running and Ready prior to attempting a deletion.
	// We will terminate Pods in a monotonically decreasing order over [len(pods),set.Spec.Replicas).
	// Note that we do not resurrect Pods in this interval. Also note that scaling will take precedence over
	// updates.
	// 倒序处理condemned中的pod
	for target := len(condemned) - 1; target >= 0; target-- {
		// wait for terminating pods to expire
		// 判断是否处于删除中
		if isTerminating(condemned[target]) {
			klog.V(4).Infof(
				"StatefulSet %s/%s is waiting for Pod %s to Terminate prior to scale down",
				set.Namespace,
				set.Name,
				condemned[target].Name)
			// block if we are in monotonic mode
			if monotonic {
				return &status, nil
			}
			continue
		}
		// if we are in monotonic mode and the condemned target is not the first unhealthy Pod block
		if !isRunningAndReady(condemned[target]) && monotonic && condemned[target] != firstUnhealthyPod {
			klog.V(4).Infof(
				"StatefulSet %s/%s is waiting for Pod %s to be Running and Ready prior to scale down",
				set.Namespace,
				set.Name,
				firstUnhealthyPod.Name)
			return &status, nil
		}
		klog.V(2).Infof("StatefulSet %s/%s terminating Pod %s for scale down",
			set.Namespace,
			set.Name,
			condemned[target].Name)

		if err := ssc.podControl.DeleteStatefulPod(set, condemned[target]); err != nil {
			return &status, err
		}
		if getPodRevision(condemned[target]) == currentRevision.Name {
			status.CurrentReplicas--
		}
		if getPodRevision(condemned[target]) == updateRevision.Name {
			status.UpdatedReplicas--
		}
		if monotonic {
			return &status, nil
		}
	}

	// for the OnDelete strategy we short circuit. Pods will be updated when they are manually deleted.
	// 判断如果更新策略等于OnDelete将直接返回
	if set.Spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType {
		return &status, nil
	}

	// we compute the minimum ordinal of the target sequence for a destructive update based on the strategy.
	// 更新策略RollingUpdate
	// 分段更新吧
	updateMin := 0
	if set.Spec.UpdateStrategy.RollingUpdate != nil {
		updateMin = int(*set.Spec.UpdateStrategy.RollingUpdate.Partition)
	}
	// we terminate the Pod with the largest ordinal that does not match the update revision.
	// 倒序进行更新
	for target := len(replicas) - 1; target >= updateMin; target-- {

		// delete the Pod if it is not already terminating and does not match the update revision.
		// 如果pod的revision和更新修订版本不一致并且该pod不处于被删除状态
		if getPodRevision(replicas[target]) != updateRevision.Name && !isTerminating(replicas[target]) {
			klog.V(2).Infof("StatefulSet %s/%s terminating Pod %s for update",
				set.Namespace,
				set.Name,
				replicas[target].Name)
			// 删除此pod
			err := ssc.podControl.DeleteStatefulPod(set, replicas[target])
			status.CurrentReplicas--
			return &status, err
		}

		// wait for unhealthy Pods on update
		// 等待不健康的pod更新
		if !isHealthy(replicas[target]) {
			klog.V(4).Infof(
				"StatefulSet %s/%s is waiting for Pod %s to update",
				set.Namespace,
				set.Name,
				replicas[target].Name)
			return &status, nil
		}

	}
	return &status, nil
}

// updateStatefulSetStatus updates set's Status to be equal to status. If status indicates a complete update, it is
// mutated to indicate completion. If status is semantically equivalent to set's Status no update is performed. If the
// returned error is nil, the update is successful.
func (ssc *defaultStatefulSetControl) updateStatefulSetStatus(
	set *apps.StatefulSet,
	status *apps.StatefulSetStatus) error {

	// complete any in progress rolling update if necessary
	// 计算滚动更新
	completeRollingUpdate(set, status)

	// if the status is not inconsistent do not perform an update
	if !inconsistentStatus(set, status) {
		return nil
	}

	// copy set and update its status
	set = set.DeepCopy()
	if err := ssc.statusUpdater.UpdateStatefulSetStatus(set, status); err != nil {
		return err
	}

	return nil
}

var _ StatefulSetControlInterface = &defaultStatefulSetControl{}
