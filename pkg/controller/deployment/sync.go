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

package deployment

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	apps "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
	labelsutil "k8s.io/kubernetes/pkg/util/labels"
)

// syncStatusOnly only updates Deployments Status and doesn't take any mutating actions.
func (dc *DeploymentController) syncStatusOnly(d *apps.Deployment, rsList []*apps.ReplicaSet) error {
	// 获取最新的rs 和 所有旧的 rs 并且同步revision
	newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(d, rsList, false)
	if err != nil {
		return err
	}

	allRSs := append(oldRSs, newRS)
	// 同步deployments状态
	return dc.syncDeploymentStatus(allRSs, newRS, d)
}

// sync is responsible for reconciling deployments on scaling events or when they
// are paused.
func (dc *DeploymentController) sync(d *apps.Deployment, rsList []*apps.ReplicaSet) error {
	// 获取new rs 和old rs
	newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(d, rsList, false)
	if err != nil {
		return err
	}
	// 进行扩容/缩容
	if err := dc.scale(d, newRS, oldRSs); err != nil {
		// If we get an error while trying to scale, the deployment will be requeued
		// so we can abort this resync
		return err
	}

	// Clean up the deployment when it's paused and no rollback is in flight.
	// 如果处于暂停状态 并且没有执行回滚操作 那就清理 old rs
	if d.Spec.Paused && getRollbackTo(d) == nil {
		if err := dc.cleanupDeployment(oldRSs, d); err != nil {
			return err
		}
	}

	allRSs := append(oldRSs, newRS)
	// 同步状态
	return dc.syncDeploymentStatus(allRSs, newRS, d)
}

// checkPausedConditions checks if the given deployment is paused or not and adds an appropriate condition.
// These conditions are needed so that we won't accidentally report lack of progress for resumed deployments
// that were paused for longer than progressDeadlineSeconds.
func (dc *DeploymentController) checkPausedConditions(d *apps.Deployment) error {
	if !deploymentutil.HasProgressDeadline(d) {
		return nil
	}
	// 获取d.status.conditions.type = Progressing的conditions
	cond := deploymentutil.GetDeploymentCondition(d.Status, apps.DeploymentProgressing)
	// 判断条件reson 是不是timeout 如果是就返回nil
	if cond != nil && cond.Reason == deploymentutil.TimedOutReason {
		// If we have reported lack of progress, do not overwrite it with a paused condition.
		return nil
	}
	// 判断当前是不是处于DeploymentPaused 暂停状态
	pausedCondExists := cond != nil && cond.Reason == deploymentutil.PausedDeployReason

	needsUpdate := false
	// 判断是不是处于暂停状态
	if d.Spec.Paused && !pausedCondExists {
		// 当我执行kubectl rollout pause deployment/nginx 会先d.spec.paused = true 但是condition对应的一条需要在这里创建
		condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionUnknown, deploymentutil.PausedDeployReason, "Deployment is paused")
		deploymentutil.SetDeploymentCondition(&d.Status, *condition)
		needsUpdate = true
	} else if !d.Spec.Paused && pausedCondExists {
		// 当d.spec.paused = false 代表已经恢复了 已经执行了 kubectl rollout resume deployment/nginx
		condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionUnknown, deploymentutil.ResumedDeployReason, "Deployment is resumed")
		deploymentutil.SetDeploymentCondition(&d.Status, *condition)
		needsUpdate = true
	}

	if !needsUpdate {
		return nil
	}

	var err error
	_, err = dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(context.TODO(), d, metav1.UpdateOptions{})
	return err
}

// getAllReplicaSetsAndSyncRevision returns all the replica sets for the provided deployment (new and all old), with new RS's and deployment's revision updated.
//
// rsList should come from getReplicaSetsForDeployment(d).
//
// 1. Get all old RSes this deployment targets, and calculate the max revision number among them (maxOldV).
// 2. Get new RS this deployment targets (whose pod template matches deployment's), and update new RS's revision number to (maxOldV + 1),
//    only if its revision number is smaller than (maxOldV + 1). If this step failed, we'll update it in the next deployment sync loop.
// 3. Copy new RS's revision number to deployment (update deployment's revision). If this step failed, we'll update it in the next deployment sync loop.
//
// Note that currently the deployment controller is using caches to avoid querying the server for reads.
// This may lead to stale reads of replica sets, thus incorrect deployment status.
func (dc *DeploymentController) getAllReplicaSetsAndSyncRevision(d *apps.Deployment, rsList []*apps.ReplicaSet, createIfNotExisted bool) (*apps.ReplicaSet, []*apps.ReplicaSet, error) {
	// 获取所有 old rs 排除掉当前的 rs
	_, allOldRSs := deploymentutil.FindOldReplicaSets(d, rsList)

	// Get new replica set with the updated revision number
	// 获取 一个 Rs对象 如果不存在 就创建一个新的RS对象 如果存在就更新RS对象 主要更新revision
	newRS, err := dc.getNewReplicaSet(d, rsList, allOldRSs, createIfNotExisted)
	if err != nil {
		return nil, nil, err
	}

	return newRS, allOldRSs, nil
}

const (
	// limit revision history length to 100 element (~2000 chars)
	maxRevHistoryLengthInChars = 2000
)

// Returns a replica set that matches the intent of the given deployment. Returns nil if the new replica set doesn't exist yet.
// 1. Get existing new RS (the RS that the given deployment targets, whose pod template is the same as deployment's).
// 2. If there's existing new RS, update its revision number if it's smaller than (maxOldRevision + 1), where maxOldRevision is the max revision number among all old RSes.
// 3. If there's no existing new RS and createIfNotExisted is true, create one with appropriate revision number (maxOldRevision + 1) and replicas.
// Note that the pod-template-hash will be added to adopted RSes and pods.
func (dc *DeploymentController) getNewReplicaSet(d *apps.Deployment, rsList, oldRSs []*apps.ReplicaSet, createIfNotExisted bool) (*apps.ReplicaSet, error) {
	// 获取最新的rs 根据ds.spec.template == rs.spec.template
	// 根据rs 创建时间排序 判断ds.spec.template 和 rs.spec.template是否一样 如果一样把这个rs设定为最新的rs
	existingNewRS := deploymentutil.FindNewReplicaSet(d, rsList)

	// Calculate the max revision number among all old RSes
	// 1. 比较所有和deployment有关的 old rs
	// 2. 根据rs.metadata.annotations[deployment.kubernetes.io/revision] 获取最大的rs版本，
	// 3. 默认是0
	maxOldRevision := deploymentutil.MaxRevision(oldRSs)
	// Calculate revision number for this new replica set
	// 4. 生成一个新的revision
	newRevision := strconv.FormatInt(maxOldRevision+1, 10)

	// Latest replica set exists. We need to sync its annotations (includes copying all but
	// annotationsToSkip from the parent deployment, and update revision, desiredReplicas,
	// and maxReplicas) and also update the revision annotation in the deployment with the
	// latest revision.
	// 判断是否已经存在了最新的rs 如果存在了 更新revision 并且返回这个rs对象
	if existingNewRS != nil {
		// 复制已经存在最新的的rs
		rsCopy := existingNewRS.DeepCopy()

		// Set existing new replica set's annotation
		// 设置 new rs annotations 更新revision值
		annotationsUpdated := deploymentutil.SetNewReplicaSetAnnotations(d, rsCopy, newRevision, true, maxRevHistoryLengthInChars)
		// 判断最新的rs.spec.MinReadySeconds 和 deployment.spec.MinReadySeconds是否一致
		minReadySecondsNeedsUpdate := rsCopy.Spec.MinReadySeconds != d.Spec.MinReadySeconds
		if annotationsUpdated || minReadySecondsNeedsUpdate {
			rsCopy.Spec.MinReadySeconds = d.Spec.MinReadySeconds
			// 更新此rs的状态
			return dc.client.AppsV1().ReplicaSets(rsCopy.ObjectMeta.Namespace).Update(context.TODO(), rsCopy, metav1.UpdateOptions{})
		}

		// Should use the revision in existingNewRS's annotation, since it set by before\
		// 设置deployment的revision 值为rs的revision
		needsUpdate := deploymentutil.SetDeploymentRevision(d, rsCopy.Annotations[deploymentutil.RevisionAnnotation])
		// If no other Progressing condition has been recorded and we need to estimate the progress
		// of this deployment then it is likely that old users started caring about progress. In that
		// case we need to take into account the first time we noticed their new replica set.
		cond := deploymentutil.GetDeploymentCondition(d.Status, apps.DeploymentProgressing)
		if deploymentutil.HasProgressDeadline(d) && cond == nil {
			msg := fmt.Sprintf("Found new replica set %q", rsCopy.Name)
			condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.FoundNewRSReason, msg)
			deploymentutil.SetDeploymentCondition(&d.Status, *condition)
			needsUpdate = true
		}

		if needsUpdate {
			var err error
			// 更新状态
			if _, err = dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(context.TODO(), d, metav1.UpdateOptions{}); err != nil {
				return nil, err
			}
		}
		return rsCopy, nil
	}
	// 判断是否需要创建Pod
	if !createIfNotExisted {
		return nil, nil
	}
	// 如果不存在最新的rs对象 那就创建一个rs对象
	// new ReplicaSet does not exist, create one.
	// new pod
	newRSTemplate := *d.Spec.Template.DeepCopy()
	// 计算一个hash pod.metadata.label pod-template-hash: 748c6fff66
	podTemplateSpecHash := controller.ComputeHash(&newRSTemplate, d.Status.CollisionCount)
	// 复制deployment.spec.template.label 并且 添加新的label pod-template-hash 设置给pod.label
	newRSTemplate.Labels = labelsutil.CloneAndAddLabel(d.Spec.Template.Labels, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)
	// Add podTemplateHash label to selector.
	// 创建一个新的选择器
	newRSSelector := labelsutil.CloneSelectorAndAddLabel(d.Spec.Selector, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)

	// Create new ReplicaSet
	// 创建一个新的rs对象
	newRS := apps.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			// Make the name deterministic, to ensure idempotence
			Name:            d.Name + "-" + podTemplateSpecHash,
			Namespace:       d.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, controllerKind)},
			Labels:          newRSTemplate.Labels,
		},
		Spec: apps.ReplicaSetSpec{
			Replicas:        new(int32),
			MinReadySeconds: d.Spec.MinReadySeconds,
			Selector:        newRSSelector,
			Template:        newRSTemplate,
		},
	}
	allRSs := append(oldRSs, &newRS)
	// 计算一个副本数
	newReplicasCount, err := deploymentutil.NewRSNewReplicas(d, allRSs, &newRS)
	if err != nil {
		return nil, err
	}
	// 设置新rs所需要的副本数
	*(newRS.Spec.Replicas) = newReplicasCount
	// Set new replica set's annotation
	// 设置Annotations
	deploymentutil.SetNewReplicaSetAnnotations(d, &newRS, newRevision, false, maxRevHistoryLengthInChars)
	// Create the new ReplicaSet. If it already exists, then we need to check for possible
	// hash collisions. If there is any other error, we need to report it in the status of
	// the Deployment.
	alreadyExists := false
	// 创建新的rs
	createdRS, err := dc.client.AppsV1().ReplicaSets(d.Namespace).Create(context.TODO(), &newRS, metav1.CreateOptions{})
	switch {
	// We may end up hitting this due to a slow cache or a fast resync of the Deployment.
	case errors.IsAlreadyExists(err):
		// 如果已经存在了这个rs 就获取这个rs 复制给rs对象
		alreadyExists = true

		// Fetch a copy of the ReplicaSet.
		rs, rsErr := dc.rsLister.ReplicaSets(newRS.Namespace).Get(newRS.Name)
		if rsErr != nil {
			return nil, rsErr
		}

		// If the Deployment owns the ReplicaSet and the ReplicaSet's PodTemplateSpec is semantically
		// deep equal to the PodTemplateSpec of the Deployment, it's the Deployment's new ReplicaSet.
		// Otherwise, this is a hash collision and we need to increment the collisionCount field in
		// the status of the Deployment and requeue to try the creation in the next sync.
		controllerRef := metav1.GetControllerOf(rs)
		if controllerRef != nil && controllerRef.UID == d.UID && deploymentutil.EqualIgnoreHash(&d.Spec.Template, &rs.Spec.Template) {
			createdRS = rs
			err = nil
			break
		}

		// Matching ReplicaSet is not equal - increment the collisionCount in the DeploymentStatus
		// and requeue the Deployment.
		if d.Status.CollisionCount == nil {
			d.Status.CollisionCount = new(int32)
		}
		preCollisionCount := *d.Status.CollisionCount
		*d.Status.CollisionCount++
		// Update the collisionCount for the Deployment and let it requeue by returning the original
		// error.
		_, dErr := dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(context.TODO(), d, metav1.UpdateOptions{})
		if dErr == nil {
			klog.V(2).Infof("Found a hash collision for deployment %q - bumping collisionCount (%d->%d) to resolve it", d.Name, preCollisionCount, *d.Status.CollisionCount)
		}
		return nil, err
	case errors.HasStatusCause(err, v1.NamespaceTerminatingCause):
		// if the namespace is terminating, all subsequent creates will fail and we can safely do nothing
		return nil, err
	case err != nil:
		msg := fmt.Sprintf("Failed to create new replica set %q: %v", newRS.Name, err)
		if deploymentutil.HasProgressDeadline(d) {
			cond := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionFalse, deploymentutil.FailedRSCreateReason, msg)
			deploymentutil.SetDeploymentCondition(&d.Status, *cond)
			// We don't really care about this error at this point, since we have a bigger issue to report.
			// TODO: Identify which errors are permanent and switch DeploymentIsFailed to take into account
			// these reasons as well. Related issue: https://github.com/kubernetes/kubernetes/issues/18568
			_, _ = dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(context.TODO(), d, metav1.UpdateOptions{})
		}
		dc.eventRecorder.Eventf(d, v1.EventTypeWarning, deploymentutil.FailedRSCreateReason, msg)
		return nil, err
	}
	if !alreadyExists && newReplicasCount > 0 {
		dc.eventRecorder.Eventf(d, v1.EventTypeNormal, "ScalingReplicaSet", "Scaled up replica set %s to %d", createdRS.Name, newReplicasCount)
	}

	needsUpdate := deploymentutil.SetDeploymentRevision(d, newRevision)
	if !alreadyExists && deploymentutil.HasProgressDeadline(d) {
		msg := fmt.Sprintf("Created new replica set %q", createdRS.Name)
		condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.NewReplicaSetReason, msg)
		deploymentutil.SetDeploymentCondition(&d.Status, *condition)
		needsUpdate = true
	}
	if needsUpdate {
		_, err = dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(context.TODO(), d, metav1.UpdateOptions{})
	}
	return createdRS, err
}

// scale scales proportionally in order to mitigate risk. Otherwise, scaling up can increase the size
// of the new replica set and scaling down can decrease the sizes of the old ones, both of which would
// have the effect of hastening the rollout progress, which could produce a higher proportion of unavailable
// replicas in the event of a problem with the rolled out template. Should run only on scaling events or
// when a deployment is paused and not during the normal rollout process.
func (dc *DeploymentController) scale(deployment *apps.Deployment, newRS *apps.ReplicaSet, oldRSs []*apps.ReplicaSet) error {
	// If there is only one active replica set then we should scale that up to the full count of the
	// deployment. If there is no active replica set, then we should scale up the newest replica set.
	// 获取一个活跃的rs 如果有活跃的rs 就对这个rs进行扩容 如果没有活跃的 就获取一个最新的rs 对它进行扩容
	if activeOrLatest := deploymentutil.FindActiveOrLatest(newRS, oldRSs); activeOrLatest != nil {
		// 如果这个活跃的rs.spec.replicas == d.spec.relicas 那就不需要扩容了
		if *(activeOrLatest.Spec.Replicas) == *(deployment.Spec.Replicas) {
			return nil
		}
		// 对这个活跃的rs/最新的rs进行扩容/缩容操作
		_, _, err := dc.scaleReplicaSetAndRecordEvent(activeOrLatest, *(deployment.Spec.Replicas), deployment)
		return err
	}

	// If the new replica set is saturated, old replica sets should be fully scaled down.
	// This case handles replica set adoption during a saturated new replica set.
	// 判断当前rs是否饱和状态 如果饱和 就遍历old rs 进行删除 把old rs 的replicas == 0
	if deploymentutil.IsSaturated(deployment, newRS) {
		// 遍历所有rs.spec.replicas >0 的rs
		for _, old := range controller.FilterActiveReplicaSets(oldRSs) {
			if _, _, err := dc.scaleReplicaSetAndRecordEvent(old, 0, deployment); err != nil {
				return err
			}
		}
		return nil
	}

	// There are old replica sets with pods and the new replica set is not saturated.
	// We need to proportionally scale all replica sets (new and old) in case of a
	// rolling deployment.
	// 如果deployment 没有活跃的rs和副本数并没有饱和
	// 需要判断策略是否为滚动更新 根据策略对rs进行按比例扩容/缩容
	if deploymentutil.IsRollingUpdate(deployment) {
		// 获取所有活跃的rs 条件为rs.spec.replicas>0
		allRSs := controller.FilterActiveReplicaSets(append(oldRSs, newRS))
		// 获取所有rs.spec.replicas的累加值 计算当前deployment下rs对象所有副本数
		allRSsReplicas := deploymentutil.GetReplicaCountForReplicaSets(allRSs)

		allowedSize := int32(0)
		if *(deployment.Spec.Replicas) > 0 {
			// 计算deployment最多可以创建多少个Pod
			allowedSize = *(deployment.Spec.Replicas) + deploymentutil.MaxSurge(*deployment)
		}

		// Number of additional replicas that can be either added or removed from the total
		// replicas count. These replicas should be distributed proportionally to the active
		// replica sets.
		// 计算出需要创建/删除的副本数
		deploymentReplicasToAdd := allowedSize - allRSsReplicas

		// The additional replicas should be distributed proportionally amongst the active
		// replica sets from the larger to the smaller in size replica set. Scaling direction
		// drives what happens in case we are trying to scale replica sets of the same size.
		// In such a case when scaling up, we should scale up newer replica sets first, and
		// when scaling down, we should scale down older replica sets first.
		var scalingOperation string
		switch {
		case deploymentReplicasToAdd > 0:
			//排序 从新到旧 然后确定扩容(up)
			sort.Sort(controller.ReplicaSetsBySizeNewer(allRSs))
			scalingOperation = "up"

		case deploymentReplicasToAdd < 0:
			//排序 从旧到新 然后确定缩容(down)
			sort.Sort(controller.ReplicaSetsBySizeOlder(allRSs))
			scalingOperation = "down"
		}

		// Iterate over all active replica sets and estimate proportions for each of them.
		// The absolute value of deploymentReplicasAdded should never exceed the absolute
		// value of deploymentReplicasToAdd.
		deploymentReplicasAdded := int32(0)
		nameToSize := make(map[string]int32)
		// 遍历所有rs
		for i := range allRSs {
			rs := allRSs[i]

			// Estimate proportions if we have replicas to add, otherwise simply populate
			// nameToSize with the current sizes for each replica set.
			if deploymentReplicasToAdd != 0 {
				// 为每个rs计算出需要扩容/缩容多副本数
				proportion := deploymentutil.GetProportion(rs, *deployment, deploymentReplicasToAdd, deploymentReplicasAdded)
				// 保存每个rs的期望副本数
				nameToSize[rs.Name] = *(rs.Spec.Replicas) + proportion
				deploymentReplicasAdded += proportion
			} else {
				nameToSize[rs.Name] = *(rs.Spec.Replicas)
			}
		}

		// Update all replica sets
		// 遍历所有的rs
		for i := range allRSs {
			rs := allRSs[i]

			// Add/remove any leftovers to the largest replica set.
			// 对第一个活跃的rs.spec.replicas 设置为
			if i == 0 && deploymentReplicasToAdd != 0 {
				leftover := deploymentReplicasToAdd - deploymentReplicasAdded
				nameToSize[rs.Name] = nameToSize[rs.Name] + leftover
				if nameToSize[rs.Name] < 0 {
					nameToSize[rs.Name] = 0
				}
			}

			// TODO: Use transactions when we have them.
			// 对rs进行扩容/缩容操作
			if _, _, err := dc.scaleReplicaSet(rs, nameToSize[rs.Name], deployment, scalingOperation); err != nil {
				// Return as soon as we fail, the deployment is requeued
				return err
			}
		}
	}
	return nil
}

func (dc *DeploymentController) scaleReplicaSetAndRecordEvent(rs *apps.ReplicaSet, newScale int32, deployment *apps.Deployment) (bool, *apps.ReplicaSet, error) {
	// No need to scale
	// 如果相等 表示不需要扩容
	if *(rs.Spec.Replicas) == newScale {
		return false, rs, nil
	}
	var scalingOperation string
	// 判断是扩容还是缩容
	if *(rs.Spec.Replicas) < newScale {
		scalingOperation = "up"
	} else {
		scalingOperation = "down"
	}
	// 对rs进行扩容/缩容 进行scale操作
	scaled, newRS, err := dc.scaleReplicaSet(rs, newScale, deployment, scalingOperation)
	return scaled, newRS, err
}

func (dc *DeploymentController) scaleReplicaSet(rs *apps.ReplicaSet, newScale int32, deployment *apps.Deployment, scalingOperation string) (bool, *apps.ReplicaSet, error) {
	// 根据rs.spec.replicas判断是否需要scale
	sizeNeedsUpdate := *(rs.Spec.Replicas) != newScale
	// 根据rs.metadata.annotations[] 判断是否需要scale
	// deployment.kubernetes.io/desired-replicas
	// deployment.kubernetes.io/max-replicas
	// deployment.Spec.Replicas
	// 根据surge配置 最多有多少个replicas存在 deployment.Spec.Replicas + maxsurge
	annotationsNeedUpdate := deploymentutil.ReplicasAnnotationsNeedUpdate(rs, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+deploymentutil.MaxSurge(*deployment))

	scaled := false
	var err error
	// 满足一个条件就可以更新
	if sizeNeedsUpdate || annotationsNeedUpdate {
		rsCopy := rs.DeepCopy()
		// 设置最新的replicas数量
		*(rsCopy.Spec.Replicas) = newScale
		// 设置rs.metadata.annotations 期望副本数和最大副本数 最大副本数就是replicas+maxsurge
		deploymentutil.SetReplicasAnnotations(rsCopy, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+deploymentutil.MaxSurge(*deployment))
		// 更新rs状态
		rs, err = dc.client.AppsV1().ReplicaSets(rsCopy.Namespace).Update(context.TODO(), rsCopy, metav1.UpdateOptions{})
		if err == nil && sizeNeedsUpdate {
			// 标记扩容成功
			scaled = true
			// 记录事件
			dc.eventRecorder.Eventf(deployment, v1.EventTypeNormal, "ScalingReplicaSet", "Scaled %s replica set %s to %d", scalingOperation, rs.Name, newScale)
		}
	}
	return scaled, rs, err
}

// cleanupDeployment is responsible for cleaning up a deployment ie. retains all but the latest N old replica sets
// where N=d.Spec.RevisionHistoryLimit. Old replica sets are older versions of the podtemplate of a deployment kept
// around by default 1) for historical reasons and 2) for the ability to rollback a deployment.
func (dc *DeploymentController) cleanupDeployment(oldRSs []*apps.ReplicaSet, deployment *apps.Deployment) error {
	if !deploymentutil.HasRevisionHistoryLimit(deployment) {
		return nil
	}

	// Avoid deleting replica set with deletion timestamp set
	// 过滤所有
	aliveFilter := func(rs *apps.ReplicaSet) bool {
		return rs != nil && rs.ObjectMeta.DeletionTimestamp == nil
	}
	cleanableRSes := controller.FilterReplicaSets(oldRSs, aliveFilter)

	diff := int32(len(cleanableRSes)) - *deployment.Spec.RevisionHistoryLimit
	if diff <= 0 {
		return nil
	}

	sort.Sort(controller.ReplicaSetsByCreationTimestamp(cleanableRSes))
	klog.V(4).Infof("Looking to cleanup old replica sets for deployment %q", deployment.Name)

	for i := int32(0); i < diff; i++ {
		rs := cleanableRSes[i]
		// Avoid delete replica set with non-zero replica counts
		if rs.Status.Replicas != 0 || *(rs.Spec.Replicas) != 0 || rs.Generation > rs.Status.ObservedGeneration || rs.DeletionTimestamp != nil {
			continue
		}
		klog.V(4).Infof("Trying to cleanup replica set %q for deployment %q", rs.Name, deployment.Name)
		if err := dc.client.AppsV1().ReplicaSets(rs.Namespace).Delete(context.TODO(), rs.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			// Return error instead of aggregating and continuing DELETEs on the theory
			// that we may be overloading the api server.
			return err
		}
	}

	return nil
}

// syncDeploymentStatus checks if the status is up-to-date and sync it if necessary
func (dc *DeploymentController) syncDeploymentStatus(allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, d *apps.Deployment) error {
	// 使用 deployments 所关联的 all rs 和 new rs 计算最新的deployments.status
	newStatus := calculateStatus(allRSs, newRS, d)
	// 判断deployments.status和newStatus是否一样
	if reflect.DeepEqual(d.Status, newStatus) {
		return nil
	}
	// 设置deployments.status
	newDeployment := d
	newDeployment.Status = newStatus
	// 更新最新deployments状态
	_, err := dc.client.AppsV1().Deployments(newDeployment.Namespace).UpdateStatus(context.TODO(), newDeployment, metav1.UpdateOptions{})
	return err
}

// calculateStatus calculates the latest status for the provided deployment by looking into the provided replica sets.
func calculateStatus(allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment) apps.DeploymentStatus {
	// 获取所有rs.status.availableReplicas的累加值
	// 获取所有rs.status.AvailableReplicas(可用的副本数)的累加值
	availableReplicas := deploymentutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	// 获取所有rs.spec.replicas的累加值
	totalReplicas := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	// 获取不可用的replicas
	unavailableReplicas := totalReplicas - availableReplicas
	// If unavailableReplicas is negative, then that means the Deployment has more available replicas running than
	// desired, e.g. whenever it scales down. In such a case we should simply default unavailableReplicas to zero.
	if unavailableReplicas < 0 {
		unavailableReplicas = 0
	}
	// 创建ds的status
	status := apps.DeploymentStatus{
		// TODO: Ensure that if we start retrying status updates, we won't pick up a new Generation value.
		ObservedGeneration: deployment.Generation,
		// 所有rs副本数
		Replicas:        deploymentutil.GetActualReplicaCountForReplicaSets(allRSs),
		UpdatedReplicas: deploymentutil.GetActualReplicaCountForReplicaSets([]*apps.ReplicaSet{newRS}),
		// 已经就绪的副本数
		ReadyReplicas: deploymentutil.GetReadyReplicaCountForReplicaSets(allRSs),
		// 可用的副本数
		AvailableReplicas: availableReplicas,
		// 不可用的副本数
		UnavailableReplicas: unavailableReplicas,
		// 用来计算hash的种子
		CollisionCount: deployment.Status.CollisionCount,
	}

	// Copy conditions one by one so we won't mutate the original object.
	// 把当前的deployments.conditions都复制到status.conditions里
	conditions := deployment.Status.Conditions
	for i := range conditions {
		status.Conditions = append(status.Conditions, conditions[i])
	}
	// 可有副本数 >= 当前副本数 - 最大不可用副本数
	if availableReplicas >= *(deployment.Spec.Replicas)-deploymentutil.MaxUnavailable(*deployment) {
		// 设置最小可用副本数状态
		minAvailability := deploymentutil.NewDeploymentCondition(apps.DeploymentAvailable, v1.ConditionTrue, deploymentutil.MinimumReplicasAvailable, "Deployment has minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *minAvailability)
	} else {
		// 设置最小不可用副本数状态
		noMinAvailability := deploymentutil.NewDeploymentCondition(apps.DeploymentAvailable, v1.ConditionFalse, deploymentutil.MinimumReplicasUnavailable, "Deployment does not have minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *noMinAvailability)
	}

	return status
}

// isScalingEvent checks whether the provided deployment has been updated with a scaling event
// by looking at the desired-replicas annotation in the active replica sets of the deployment.
//
// rsList should come from getReplicaSetsForDeployment(d).
func (dc *DeploymentController) isScalingEvent(d *apps.Deployment, rsList []*apps.ReplicaSet) (bool, error) {
	// 获取最新的rs 和所有old rs 并且同步revision
	newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(d, rsList, false)
	if err != nil {
		return false, err
	}
	allRSs := append(oldRSs, newRS)
	// 遍历所有处于 rs.Spec.Replicas > 0 的rs
	for _, rs := range controller.FilterActiveReplicaSets(allRSs) {
		// 获取每个rs.metadata.annoations["deployment.kubernetes.io/desired-replicas"]
		desired, ok := deploymentutil.GetDesiredReplicasAnnotation(rs)
		if !ok {
			continue
		}
		// 如果和deployment.spec.replicase  不一致 代表扩缩容
		if desired != *(d.Spec.Replicas) {
			return true, nil
		}
	}
	return false, nil
}
