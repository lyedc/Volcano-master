/*
Copyright 2019 The Volcano Authors.

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
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/klog/v2"

	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"volcano.sh/volcano/pkg/controllers/apis"
	jobhelpers "volcano.sh/volcano/pkg/controllers/job/helpers"
	"volcano.sh/volcano/pkg/controllers/job/state"
	"volcano.sh/volcano/pkg/controllers/util"
)

var calMutex sync.Mutex

func (cc *jobcontroller) killJob(jobInfo *apis.JobInfo, podRetainPhase state.PhaseMap, updateStatus state.UpdateStatusFn) error {
	job := jobInfo.Job
	klog.V(3).Infof("Killing Job <%s/%s>, current version %d", job.Namespace, job.Name, job.Status.Version)
	defer klog.V(3).Infof("Finished Job <%s/%s> killing, current version %d", job.Namespace, job.Name, job.Status.Version)

	if job.DeletionTimestamp != nil {
		klog.Infof("Job <%s/%s> is terminating, skip management process.",
			job.Namespace, job.Name)
		return nil
	}
	// 状态计数器， 用于更新job的状态
	var pending, running, terminating, succeeded, failed, unknown int32
	taskStatusCount := make(map[string]batch.TaskState)

	var errs []error
	var total int

	for _, pods := range jobInfo.Pods {
		for _, pod := range pods {
			total++

			if pod.DeletionTimestamp != nil {
				klog.Infof("Pod <%s/%s> is terminating", pod.Namespace, pod.Name)
				terminating++
				continue
			}

			maxRetry := job.Spec.MaxRetry
			lastRetry := false
			// 判断是否是最后一次重试
			if job.Status.RetryCount >= maxRetry-1 {
				lastRetry = true
			}

			// Only retain the Failed and Succeeded pods at the last retry.
			// If it is not the last retry, kill pod as defined in `podRetainPhase`.
			retainPhase := podRetainPhase
			// 如果是最后一次重试， 则保留失败和成功的pod
			if lastRetry {
				// var PodRetainPhaseSoft = PhaseMap{
				//     v1.PodSucceeded: {},
				//     v1.PodFailed:    {},
				// }
				retainPhase = state.PodRetainPhaseSoft
			}
			_, retain := retainPhase[pod.Status.Phase]
			// 如果pod的状态不是上面的两种状态，那么久删除这个job对应的pod
			// 上述两种状态也就是成功的成功和失败的。
			if !retain {
				err := cc.deleteJobPod(job.Name, pod)
				if err == nil {
					terminating++
					continue
				}
				// record the err, and then collect the pod info like retained pod
				errs = append(errs, err)
				//放入到失败重试队列中。。errTasks是专门用于重试的队列。。
				cc.resyncTask(pod)
			}
            // 根据pod的状态，更新状态计数器。。。
			classifyAndAddUpPodBaseOnPhase(pod, &pending, &running, &succeeded, &failed, &unknown)
			calcPodStatus(pod, taskStatusCount)
		}
	}

	if len(errs) != 0 {
		klog.Errorf("failed to kill pods for job %s/%s, with err %+v", job.Namespace, job.Name, errs)
		cc.recorder.Event(job, v1.EventTypeWarning, FailedDeletePodReason,
			fmt.Sprintf("Error deleting pods: %+v", errs))
		return fmt.Errorf("failed to kill %d pods of %d", len(errs), total)
	}

	job = job.DeepCopy()
	// 更新job的状态计数
	// Job version is bumped only when job is killed
	job.Status.Version++
	job.Status.Pending = pending
	job.Status.Running = running
	job.Status.Succeeded = succeeded
	job.Status.Failed = failed
	job.Status.Terminating = terminating
	job.Status.Unknown = unknown
	job.Status.TaskStatusCount = taskStatusCount

	// Update running duration
	klog.V(3).Infof("Running duration is %s", metav1.Duration{Duration: time.Since(jobInfo.Job.CreationTimestamp.Time)}.ToUnstructured())
	// 更新运行持续时间
	job.Status.RunningDuration = &metav1.Duration{Duration: time.Since(jobInfo.Job.CreationTimestamp.Time)}
	// 更新job的状态
	if updateStatus != nil {
		// updateStatus 主要是为了更新job的状态。
		if updateStatus(&job.Status) {
			job.Status.State.LastTransitionTime = metav1.Now()
			jobCondition := newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
			job.Status.Conditions = append(job.Status.Conditions, jobCondition)
		}
	}

	// must be called before update job status
	// RegisterPluginBuilder 在这里进行了注册逻辑。。。可以来这里查找到注册的插件。。。以及默认的执行方法
	if err := cc.pluginOnJobDelete(job); err != nil {
		return err
	}

	// Update Job status
	newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v",
			job.Namespace, job.Name, err)
		return err
	}
	if e := cc.cache.Update(newJob); e != nil {
		klog.Errorf("KillJob - Failed to update Job %v/%v in cache:  %v",
			newJob.Namespace, newJob.Name, e)
		return e
	}

	// Delete PodGroup
	pgName := job.Name + "-" + string(job.UID)
	if err := cc.vcClient.SchedulingV1beta1().PodGroups(job.Namespace).Delete(context.TODO(), pgName, metav1.DeleteOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to delete PodGroup of Job %v/%v: %v",
				job.Namespace, job.Name, err)
			return err
		}
	}

	// NOTE(k82cn): DO NOT delete input/output until job is deleted.

	return nil
}

func (cc *jobcontroller) initiateJob(job *batch.Job) (*batch.Job, error) {
	klog.V(3).Infof("Starting to initiate Job <%s/%s>", job.Namespace, job.Name)
	jobInstance, err := cc.initJobStatus(job)
	if err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.JobStatusError),
			fmt.Sprintf("Failed to initialize job status, err: %v", err))
		return nil, err
	}

	if err := cc.pluginOnJobAdd(jobInstance); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PluginError),
			fmt.Sprintf("Execute plugin when job add failed, err: %v", err))
		return nil, err
	}

	newJob, err := cc.createJobIOIfNotExist(jobInstance)
	if err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PVCError),
			fmt.Sprintf("Failed to create PVC, err: %v", err))
		return nil, err
	}
    // 按照最新的job信息，更新pg，如果没有pg，就创建一个新的pg，pg中的最小task数量也来自job，资源情况也是通过job中声明的资源填充
	if err := cc.createOrUpdatePodGroup(newJob); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PodGroupError),
			fmt.Sprintf("Failed to create PodGroup, err: %v", err))
		return nil, err
	}

	return newJob, nil
}

func (cc *jobcontroller) initOnJobUpdate(job *batch.Job) error {
	klog.V(3).Infof("Starting to initiate Job <%s/%s> on update", job.Namespace, job.Name)

	if err := cc.pluginOnJobUpdate(job); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PluginError),
			fmt.Sprintf("Execute plugin when job add failed, err: %v", err))
		return err
	}

	if err := cc.createOrUpdatePodGroup(job); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PodGroupError),
			fmt.Sprintf("Failed to create PodGroup, err: %v", err))
		return err
	}

	return nil
}

func (cc *jobcontroller) GetQueueInfo(queue string) (*scheduling.Queue, error) {
	queueInfo, err := cc.queueLister.Get(queue)
	if err != nil {
		klog.Errorf("Failed to get queue from queueLister, error: %s", err.Error())
	}

	return queueInfo, err
}

// syncJob 同步作业状态，确保作业的管理和更新与集群的当前状态一致。
//
// 参数:
// - jobInfo: 包含作业信息的指针，提供了作业的详细配置和状态。
// - updateStatus: 一个状态更新函数，如果非空，将被调用以更新作业的状态。
//
// 返回值:
// - 返回可能遇到的错误，如果操作成功，则返回 nil。
func (cc *jobcontroller) syncJob(jobInfo *apis.JobInfo, updateStatus state.UpdateStatusFn) error {
	job := jobInfo.Job
	klog.V(3).Infof("Starting to sync up Job <%s/%s>, current version %d", job.Namespace, job.Name, job.Status.Version)
	defer klog.V(3).Infof("Finished Job <%s/%s> sync up, current version %d", job.Namespace, job.Name, job.Status.Version)

	// 如果作业正在终止，则跳过管理过程。
	if jobInfo.Job.DeletionTimestamp != nil {
		klog.Infof("Job <%s/%s> is terminating, skip management process.",
			jobInfo.Job.Namespace, jobInfo.Job.Name)
		return nil
	}

	// 深拷贝作业，防止原始作业被修改。
	job = job.DeepCopy()

	// 获取作业所属的队列信息，并检查队列是否有转发元数据。
	queueInfo, err := cc.GetQueueInfo(job.Spec.Queue)
	if err != nil {
		return err
	}

	var jobForwarding bool
	if len(queueInfo.Spec.ExtendClusters) != 0 {
		jobForwarding = true
		if len(job.Annotations) == 0 {
			job.Annotations = make(map[string]string)
		}
		job.Annotations[batch.JobForwardingKey] = "true"
		// 更新作业注释以标记转发状态。
		job, err = cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("failed to update job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
			return err
		}
	}

	// 如果作业未初始化，则进行初始化；否则，优化作业更新过程。
	if !isInitiated(job) {
		// // initiateJob中会更新job状态、调用add插件、更新podgroup
		// 新创建的pod因为job的状态为空，初始化更新job的状态为pending
		// 并且会根据job的情况，更新pg中的最小task数量，资源情况也是通过job中声明的资源填充
		if job, err = cc.initiateJob(job); err != nil {
			return err
		}
	} else {
		// // initOnJobUpdate会调用add插件、如果没有podGroup就创建一个podGroup，有的话就根据计算更新podgroup
		if err = cc.initOnJobUpdate(job); err != nil {
			return err
		}
	}

	// 如果队列有扩展集群，更新作业以标记转发状态。
	if len(queueInfo.Spec.ExtendClusters) != 0 {
		jobForwarding = true
		job.Annotations[batch.JobForwardingKey] = "true"
		_, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("failed to update job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
			return err
		}
	}

	// 检查是否存在对应的 PodGroup，并根据状态决定是否需要同步任务。
	var syncTask bool
	pgName := job.Name + "-" + string(job.UID)
	if pg, _ := cc.pgLister.PodGroups(job.Namespace).Get(pgName); pg != nil {
		if pg.Status.Phase != "" && pg.Status.Phase != scheduling.PodGroupPending {
			syncTask = true
		}

		// 记录 PodGroup 可调度性条件。
		/*
		Event(v1.ObjectReference{Kind:"Job", Namespace:"default", Name:"jobb", UID:"621d0426-d080-4271-bf07-6e294dfa9baf", APIVersion:"batch.volcano.sh/v1alpha1", ResourceVersion:"45197320", FieldPath:""}): type: 'Warning' reason: 'PodGroupPending'
		PodGroup default:jobb unschedule,reason: 2/0 tasks in gang unschedulable: pod group is not ready, 2 minAvailable

		*/
		// 这里有问题，为什么podgroup已经调度完成了，还要展示之前的未调度的原因呢。。。
		for _, condition := range pg.Status.Conditions {
			if condition.Type == scheduling.PodGroupUnschedulableType {
				cc.recorder.Eventf(job, v1.EventTypeWarning, string(batch.PodGroupPending),
					fmt.Sprintf("PodGroup %s:%s unschedule,reason: %s", job.Namespace, job.Name, condition.Message))
			}
		}
	}

	var jobCondition batch.JobCondition
	// 如果不需要同步任务，更新作业状态。
	if !syncTask {
		if updateStatus != nil {
			// 新创建的job，返回的是false，更新状态信息。
			if updateStatus(&job.Status) {
				job.Status.State.LastTransitionTime = metav1.Now()
				jobCondition = newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
				job.Status.Conditions = append(job.Status.Conditions, jobCondition)
			}
		}
		// 更新作业状态并更新缓存。
		newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update status of Job %v/%v: %v",
				job.Namespace, job.Name, err)
			return err
		}
		if e := cc.cache.Update(newJob); e != nil {
			klog.Errorf("SyncJob - Failed to update Job %v/%v in cache:  %v",
				newJob.Namespace, newJob.Name, e)
			return e
		}
		return nil
	}

	// 初始化各种状态的计数器和相关容器
	var running, pending, terminating, succeeded, failed, unknown int32
	taskStatusCount := make(map[string]batch.TaskState)

	// 初始化用于记录要创建和删除的Pod的映射及错误容器
	podToCreate := make(map[string][]*v1.Pod)
	var podToDelete []*v1.Pod
	var creationErrs []error
	var deletionErrs []error

	// 用于并发安全添加错误的互斥锁
	appendMutex := sync.Mutex{}

	// 将错误安全地添加到错误容器中
	appendError := func(container *[]error, err error) {
		appendMutex.Lock()
		defer appendMutex.Unlock()
		*container = append(*container, err)
	}

	// 初始化用于等待所有Pod创建完成的同步组
	waitCreationGroup := sync.WaitGroup{}

	// 遍历Job中定义的所有任务
	for _, ts := range job.Spec.Tasks {
		// 设置任务模板的名称并创建任务的副本
		ts.Template.Name = ts.Name
		tc := ts.Template.DeepCopy()
		name := ts.Template.Name

		// 尝试从jobInfo获取当前任务名称对应的Pods
		// 新创建的job中没有pod的信息。
		pods, found := jobInfo.Pods[name]
		if !found {
			pods = map[string]*v1.Pod{}
		}

		// 为当前任务创建所有必要的Pod副本
		var podToCreateEachTask []*v1.Pod
		for i := 0; i < int(ts.Replicas); i++ {
			podName := fmt.Sprintf(jobhelpers.PodNameFmt, job.Name, name, i)
			// 新创建的job中没有pod的信息。会走创建pod的逻辑。
			if pod, found := pods[podName]; !found {
				// 如果Pod不存在，则创建新的Pod
				newPod := createJobPod(job, tc, ts.TopologyPolicy, i, jobForwarding)
				if err := cc.pluginOnPodCreate(job, newPod); err != nil {
					return err
				}
				podToCreateEachTask = append(podToCreateEachTask, newPod)
				waitCreationGroup.Add(1)
			} else {
				// 如果Pod已存在，则根据其状态进行分类计数，并从待处理Pod映射中移除,如果 已经存在了，就从job.pods中删除。。
				// 这里的从job的pods中删除，是为了从下面的删除逻辑中，这个pod不被删除掉，因为下面的计算待删除的pod的逻辑
				// 是直接循环了Pods这个列表，如果不从这里面删除的话，就会导致pod在下面的删除逻辑中被标记为删除。
				delete(pods, podName)
				if pod.DeletionTimestamp != nil {
					// 如果Pod正在终止，则增加terminating计数
					klog.Infof("Pod <%s/%s> is terminating", pod.Namespace, pod.Name)
					atomic.AddInt32(&terminating, 1)
					continue
				}

				// 根据Pod的状态进行分类计数
				classifyAndAddUpPodBaseOnPhase(pod, &pending, &running, &succeeded, &failed, &unknown)
				// 更新任务状态计数
				calcPodStatus(pod, taskStatusCount)
			}
		}
		// 将当前任务的所有待创建Pod添加到全局待创建Pod映射中
		podToCreate[ts.Name] = podToCreateEachTask
		// 将当前任务的所有待删除Pod添加到全局待删除Pod切片中
		// 现在这里存放的是不是应该有的pod，根据上面的规则，如果没有对应的pod，就创建出来一个新的
		// 如果存在就从这个pods中删除，所有这里存放的及时不应该出现的pod，需要删除了。。。
		// 这里的pods已经删除了task中需要的pod，这里不会误删除的。
		for _, pod := range pods {
			podToDelete = append(podToDelete, pod)
		}
	}

	// 此段代码主要负责创建和删除作业（Job）中的Pods。
	// 先为每个任务（Task）创建Pods，然后等待所有Pods创建完成。
	// 如果创建过程中有错误，会记录事件并返回错误信息。
	// 创建完成后，如果作业规模缩小，会删除多余的Pods。
	// 删除过程中同样处理错误，并在完成后更新作业状态。
	// 为每个任务创建Pods
	for taskName, podToCreateEachTask := range podToCreate {
		// 跳过空任务
		if len(podToCreateEachTask) == 0 {
			continue
		}
		// 并发创建Pods
		go func(taskName string, podToCreateEachTask []*v1.Pod) {
			// 获取任务索引，检查任务是否有依赖
			taskIndex := jobhelpers.GetTaskIndexUnderJob(taskName, job)
			// 查看是否存在依赖项
			if job.Spec.Tasks[taskIndex].DependsOn != nil {
				// 如果任务有依赖且依赖未满足，则跳过当前任务的Pod创建
				if !cc.waitDependsOnTaskMeetCondition(taskName, taskIndex, podToCreateEachTask, job) {
					klog.V(3).Infof("Job %s/%s depends on task not ready", job.Name, job.Namespace)
					// 释放等待组
					for _, pod := range podToCreateEachTask {
						go func(pod *v1.Pod) {
							defer waitCreationGroup.Done()
						}(pod)
					}
					return
				}
			}

			// 为任务中的每个Pod创建实例
			for _, pod := range podToCreateEachTask {
				go func(pod *v1.Pod) {
					defer waitCreationGroup.Done()
					// 尝试创建Pod，处理创建失败的情况
					newPod, err := cc.kubeClient.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
					if err != nil && !apierrors.IsAlreadyExists(err) {
						// Pod创建失败，记录错误并重试
						klog.Errorf("Failed to create pod %s for Job %s, err %#v",
							pod.Name, job.Name, err)
						appendError(&creationErrs, fmt.Errorf("failed to create pod %s, err: %#v", pod.Name, err))
					} else {
						// Pod创建成功，更新状态
						classifyAndAddUpPodBaseOnPhase(newPod, &pending, &running, &succeeded, &failed, &unknown)
						calcPodStatus(pod, taskStatusCount)
						klog.V(5).Infof("Created Task <%s> of Job <%s/%s>",
							pod.Name, job.Namespace, job.Name)
					}
				}(pod)
			}
		}(taskName, podToCreateEachTask)
	}

	// 等待所有Pod创建完成
	waitCreationGroup.Wait()

	// 处理Pod创建过程中的错误
	// 这里没有做一致性的处理，因为这里虽然失败了。但是在下一个周期中还会去创建这个pod，也就是会保证这个pod永远被创建出来。
	if len(creationErrs) != 0 {
		cc.recorder.Event(job, v1.EventTypeWarning, FailedCreatePodReason,
			fmt.Sprintf("Error creating pods: %+v", creationErrs))
		return fmt.Errorf("failed to create %d pods of %d", len(creationErrs), len(podToCreate))
	}

	// 开始删除不再需要的Pods
	waitDeletionGroup := sync.WaitGroup{}
	waitDeletionGroup.Add(len(podToDelete))
	for _, pod := range podToDelete {
		go func(pod *v1.Pod) {
			defer waitDeletionGroup.Done()
			// 尝试删除Pod，处理删除失败的情况
			err := cc.deleteJobPod(job.Name, pod)
			if err != nil {
				// Pod删除失败，记录错误并重试
				klog.Errorf("Failed to delete pod %s for Job %s, err %#v",
					pod.Name, job.Name, err)
				appendError(&deletionErrs, err)
				cc.resyncTask(pod)
			} else {
				// Pod删除成功，更新状态
				klog.V(3).Infof("Deleted Task <%s> of Job <%s/%s>",
					pod.Name, job.Namespace, job.Name)
				atomic.AddInt32(&terminating, 1)
			}
		}(pod)
	}
	waitDeletionGroup.Wait()

	// 处理Pod删除过程中的错误
	if len(deletionErrs) != 0 {
		cc.recorder.Event(job, v1.EventTypeWarning, FailedDeletePodReason,
			fmt.Sprintf("Error deleting pods: %+v", deletionErrs))
		return fmt.Errorf("failed to delete %d pods of %d", len(deletionErrs), len(podToDelete))
	}

	// 更新作业状态
	job.Status = batch.JobStatus{
		State: job.Status.State,

		Pending:             pending,
		Running:             running,
		Succeeded:           succeeded,
		Failed:              failed,
		Terminating:         terminating,
		Unknown:             unknown,
		Version:             job.Status.Version,
		MinAvailable:        job.Spec.MinAvailable,
		TaskStatusCount:     taskStatusCount,
		ControlledResources: job.Status.ControlledResources,
		Conditions:          job.Status.Conditions,
		RetryCount:          job.Status.RetryCount,
	}

	// 更新作业状态，包括条件和最后转换时间
	if updateStatus != nil && updateStatus(&job.Status) {
		job.Status.State.LastTransitionTime = metav1.Now()
		jobCondition = newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
		job.Status.Conditions = append(job.Status.Conditions, jobCondition)
	}
	// 尝试更新作业状态
	newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v",
			job.Namespace, job.Name, err)
		return err
	}
	// 更新缓存中的作业状态
	if e := cc.cache.Update(newJob); e != nil {
		klog.Errorf("SyncJob - Failed to update Job %v/%v in cache:  %v",
			newJob.Namespace, newJob.Name, e)
		return e
	}

	return nil
}

// waitDependsOnTaskMeetCondition 等待依赖的任务满足条件。
// 该函数检查指定任务是否满足其依赖条件，如果满足，则可以继续创建新的Pod。
// taskName: 任务名称。
// taskIndex: 任务在作业中的索引。
// podToCreateEachTask: 每个任务要创建的Pod列表。
// job: 当前作业对象。
// 返回值: 如果依赖条件已满足，返回true；否则返回false。
func (cc *jobcontroller) waitDependsOnTaskMeetCondition(taskName string, taskIndex int, podToCreateEachTask []*v1.Pod, job *batch.Job) bool {
	// 检查任务是否有依赖项
	if job.Spec.Tasks[taskIndex].DependsOn == nil {
		// 无依赖项，直接返回true
		return true
	}
	dependsOn := *job.Spec.Tasks[taskIndex].DependsOn
	// 处理依赖于任何任务的场景
	if len(dependsOn.Name) > 1 && dependsOn.Iteration == batch.IterationAny {
		// 检查所有依赖任务，任一任务准备就绪即返回true
		for _, task := range dependsOn.Name {
			if cc.isDependsOnPodsReady(task, job) {
				return true
			}
		}
		// 所有依赖任务均未就绪，返回false
		return false
	}
	// 检查每个指定的依赖任务
	for _, dependsOnTask := range dependsOn.Name {
		// 任一依赖任务未就绪，返回false
		if !cc.isDependsOnPodsReady(dependsOnTask, job) {
			return false
		}
	}
	// 所有依赖任务均已就绪，返回true
	return true
}


// isDependsOnPodsReady 检查任务所依赖的 Pod 是否准备就绪。
//
// 参数:
// task - 当前任务的名称。
// job - 当前作业的引用。
//
// 返回值:
// 返回一个布尔值，表示依赖的 Pod 是否准备就绪。
func (cc *jobcontroller) isDependsOnPodsReady(task string, job *batch.Job) bool {
	// 获取当前任务所依赖的 Pod 名称列表。
	dependsOnPods := jobhelpers.GetPodsNameUnderTask(task, job)
	// 获取当前任务在作业中的索引。
	dependsOnTaskIndex := jobhelpers.GetTaskIndexUnderJob(task, job)
	runningPodCount := 0 // 记录正在运行的 Pod 数量。

	for _, podName := range dependsOnPods {
		// 尝试获取 Pod 信息。
		pod, err := cc.podLister.Pods(job.Namespace).Get(podName)
		if err != nil {
			// 如果获取 Pod 失败，判断是 Pod 不存在还是其他错误。
			if apierrors.IsNotFound(err) {
				// 检查作业是否还存在，如果作业不存在，则认为依赖的 Pod 已经准备就绪。
				_, errGetJob := cc.jobLister.Jobs(job.Namespace).Get(job.Name)
				return apierrors.IsNotFound(errGetJob)
			}
			// 记录获取 Pod 失败的日志。
			klog.Errorf("Failed to get pod %v/%v %v", job.Namespace, podName, err)
			continue
		}

		// 检查 Pod 的状态是否为运行中或已成功。
		if pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodSucceeded {
			// 如果 Pod 不处于运行中或已成功状态，则继续检查下一个 Pod。
			klog.V(5).Infof("Sequential state, pod %v/%v of depends on tasks is not running", pod.Namespace, pod.Name)
			continue
		}

		// 检查 Pod 内的所有容器是否都已准备就绪。
		allContainerReady := true
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if !containerStatus.Ready {
				allContainerReady = false
				break
			}
		}
		if allContainerReady {
			runningPodCount++
		}
	}

	// 检查运行中的 Pod 数量是否满足依赖任务的最小可用要求。
	dependsOnTaskMinReplicas := job.Spec.Tasks[dependsOnTaskIndex].MinAvailable
	if dependsOnTaskMinReplicas != nil {
		if runningPodCount < int(*dependsOnTaskMinReplicas) {
			// 如果运行中的 Pod 数量小于最小可用数量，则认为依赖的 Pod 未准备就绪。
			klog.V(5).Infof("In a depends on startup state, there are already %d pods running, which is less than the minimum number of runs", runningPodCount)
			return false
		}
	}
	// 所有检查通过，认为依赖的 Pod 已经准备就绪。
	return true
}


func (cc *jobcontroller) createJobIOIfNotExist(job *batch.Job) (*batch.Job, error) {
	// If PVC does not exist, create them for Job.
	var needUpdate bool
	if job.Status.ControlledResources == nil {
		job.Status.ControlledResources = make(map[string]string)
	}
	for index, volume := range job.Spec.Volumes {
		vcName := volume.VolumeClaimName
		if len(vcName) == 0 {
			// NOTE(k82cn): Ensure never have duplicated generated names.
			for {
				vcName = jobhelpers.GenPVCName(job.Name)
				exist, err := cc.checkPVCExist(job, vcName)
				if err != nil {
					return job, err
				}
				if exist {
					continue
				}
				job.Spec.Volumes[index].VolumeClaimName = vcName
				needUpdate = true
				break
			}
			// TODO: check VolumeClaim must be set if VolumeClaimName is empty
			if volume.VolumeClaim != nil {
				if err := cc.createPVC(job, vcName, volume.VolumeClaim); err != nil {
					return job, err
				}
			}
		} else {
			exist, err := cc.checkPVCExist(job, vcName)
			if err != nil {
				return job, err
			}
			if !exist {
				return job, fmt.Errorf("pvc %s is not found, the job will be in the Pending state until the PVC is created", vcName)
			}
		}
		job.Status.ControlledResources["volume-pvc-"+vcName] = vcName
	}
	if needUpdate {
		newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update Job %v/%v for volume claim name: %v ",
				job.Namespace, job.Name, err)
			return job, err
		}

		newJob.Status = job.Status
		return newJob, err
	}
	return job, nil
}

func (cc *jobcontroller) checkPVCExist(job *batch.Job, pvc string) (bool, error) {
	if _, err := cc.pvcLister.PersistentVolumeClaims(job.Namespace).Get(pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		klog.V(3).Infof("Failed to get PVC %s for job <%s/%s>: %v",
			pvc, job.Namespace, job.Name, err)
		return false, err
	}
	return true, nil
}

func (cc *jobcontroller) createPVC(job *batch.Job, vcName string, volumeClaim *v1.PersistentVolumeClaimSpec) error {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace,
			Name:      vcName,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, helpers.JobKind),
			},
		},
		Spec: *volumeClaim,
	}

	klog.V(3).Infof("Try to create PVC: %v", pvc)

	if _, e := cc.kubeClient.CoreV1().PersistentVolumeClaims(job.Namespace).Create(context.TODO(), pvc, metav1.CreateOptions{}); e != nil {
		klog.V(3).Infof("Failed to create PVC for Job <%s/%s>: %v",
			job.Namespace, job.Name, e)
		return e
	}
	return nil
}

func (cc *jobcontroller) createOrUpdatePodGroup(job *batch.Job) error {
	// If PodGroup does not exist, create one for Job.
	pgName := job.Name + "-" + string(job.UID)
	var pg *scheduling.PodGroup
	var err error
	pg, err = cc.pgLister.PodGroups(job.Namespace).Get(pgName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to get PodGroup for Job <%s/%s>: %v",
				job.Namespace, job.Name, err)
			return err
		}
		// try to get old pg if new pg not exist
		pg, err = cc.pgLister.PodGroups(job.Namespace).Get(job.Name)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				klog.Errorf("Failed to get PodGroup for Job <%s/%s>: %v",
					job.Namespace, job.Name, err)
				return err
			}

			minTaskMember := map[string]int32{}
			for _, task := range job.Spec.Tasks {
				if task.MinAvailable != nil {
					minTaskMember[task.Name] = *task.MinAvailable
				} else {
					minTaskMember[task.Name] = task.Replicas
				}
			}

			pg := &scheduling.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: job.Namespace,
					// add job.UID into its name when create new PodGroup
					Name:        pgName,
					Annotations: job.Annotations,
					Labels:      job.Labels,
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(job, helpers.JobKind),
					},
				},
				Spec: scheduling.PodGroupSpec{
					MinMember:         job.Spec.MinAvailable,
					MinTaskMember:     minTaskMember,
					Queue:             job.Spec.Queue,
					MinResources:      cc.calcPGMinResources(job),
					PriorityClassName: job.Spec.PriorityClassName,
				},
			}

			if _, err = cc.vcClient.SchedulingV1beta1().PodGroups(job.Namespace).Create(context.TODO(), pg, metav1.CreateOptions{}); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					klog.Errorf("Failed to create PodGroup for Job <%s/%s>: %v",
						job.Namespace, job.Name, err)
					return err
				}
			}
			return nil
		}
	}

	pgShouldUpdate := false
	if pg.Spec.PriorityClassName != job.Spec.PriorityClassName {
		pg.Spec.PriorityClassName = job.Spec.PriorityClassName
		pgShouldUpdate = true
	}

	minResources := cc.calcPGMinResources(job)
	if pg.Spec.MinMember != job.Spec.MinAvailable || !reflect.DeepEqual(pg.Spec.MinResources, minResources) {
		pg.Spec.MinMember = job.Spec.MinAvailable
		pg.Spec.MinResources = minResources
		pgShouldUpdate = true
	}

	if pg.Spec.MinTaskMember == nil {
		pgShouldUpdate = true
		pg.Spec.MinTaskMember = make(map[string]int32)
	}

	for _, task := range job.Spec.Tasks {
		cnt := task.Replicas
		if task.MinAvailable != nil {
			cnt = *task.MinAvailable
		}

		if taskMember, ok := pg.Spec.MinTaskMember[task.Name]; !ok {
			pgShouldUpdate = true
			pg.Spec.MinTaskMember[task.Name] = cnt
		} else {
			if taskMember == cnt {
				continue
			}

			pgShouldUpdate = true
			pg.Spec.MinTaskMember[task.Name] = cnt
		}
	}

	if !pgShouldUpdate {
		return nil
	}

	_, err = cc.vcClient.SchedulingV1beta1().PodGroups(job.Namespace).Update(context.TODO(), pg, metav1.UpdateOptions{})
	if err != nil {
		klog.V(3).Infof("Failed to update PodGroup for Job <%s/%s>: %v",
			job.Namespace, job.Name, err)
	}
	return err
}

func (cc *jobcontroller) deleteJobPod(jobName string, pod *v1.Pod) error {
	err := cc.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete pod %s/%s for Job %s, err %#v",
			pod.Namespace, pod.Name, jobName, err)

		return fmt.Errorf("failed to delete pod %s, err %#v", pod.Name, err)
	}

	return nil
}

func (cc *jobcontroller) calcPGMinResources(job *batch.Job) *v1.ResourceList {
	// sort task by priorityClasses
	var tasksPriority TasksPriority
	for _, task := range job.Spec.Tasks {
		tp := TaskPriority{0, task}
		pc := task.Template.Spec.PriorityClassName

		if pc != "" {
			priorityClass, err := cc.pcLister.Get(pc)
			if err != nil || priorityClass == nil {
				klog.Warningf("Ignore task %s priority class %s: %v", task.Name, pc, err)
			} else {
				tp.priority = priorityClass.Value
			}
		}
		tasksPriority = append(tasksPriority, tp)
	}

	sort.Sort(tasksPriority)

	minReq := v1.ResourceList{}
	podCnt := int32(0)
	for _, task := range tasksPriority {
		for i := int32(0); i < task.Replicas; i++ {
			if podCnt >= job.Spec.MinAvailable {
				break
			}

			podCnt++
			pod := &v1.Pod{
				Spec: task.Template.Spec,
			}
			minReq = quotav1.Add(minReq, *util.GetPodQuotaUsage(pod))
		}
	}

	return &minReq
}

func (cc *jobcontroller) initJobStatus(job *batch.Job) (*batch.Job, error) {
	if job.Status.State.Phase != "" {
		return job, nil
	}

	job.Status.State.Phase = batch.Pending
	job.Status.State.LastTransitionTime = metav1.Now()
	job.Status.MinAvailable = job.Spec.MinAvailable
	jobCondition := newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
	job.Status.Conditions = append(job.Status.Conditions, jobCondition)
	newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v",
			job.Namespace, job.Name, err)
		return nil, err
	}
	if err := cc.cache.Update(newJob); err != nil {
		klog.Errorf("CreateJob - Failed to update Job %v/%v in cache:  %v",
			newJob.Namespace, newJob.Name, err)
		return nil, err
	}

	return newJob, nil
}

func classifyAndAddUpPodBaseOnPhase(pod *v1.Pod, pending, running, succeeded, failed, unknown *int32) {
	switch pod.Status.Phase {
	case v1.PodPending:
		atomic.AddInt32(pending, 1)
	case v1.PodRunning:
		atomic.AddInt32(running, 1)
	case v1.PodSucceeded:
		atomic.AddInt32(succeeded, 1)
	case v1.PodFailed:
		atomic.AddInt32(failed, 1)
	default:
		atomic.AddInt32(unknown, 1)
	}
}

func calcPodStatus(pod *v1.Pod, taskStatusCount map[string]batch.TaskState) {
	taskName, found := pod.Annotations[batch.TaskSpecKey]
	if !found {
		return
	}

	calMutex.Lock()
	defer calMutex.Unlock()
	if _, ok := taskStatusCount[taskName]; !ok {
		taskStatusCount[taskName] = batch.TaskState{
			Phase: make(map[v1.PodPhase]int32),
		}
	}

	switch pod.Status.Phase {
	case v1.PodPending:
		taskStatusCount[taskName].Phase[v1.PodPending]++
	case v1.PodRunning:
		taskStatusCount[taskName].Phase[v1.PodRunning]++
	case v1.PodSucceeded:
		taskStatusCount[taskName].Phase[v1.PodSucceeded]++
	case v1.PodFailed:
		taskStatusCount[taskName].Phase[v1.PodFailed]++
	default:
		taskStatusCount[taskName].Phase[v1.PodUnknown]++
	}
}

func isInitiated(job *batch.Job) bool {
	if job.Status.State.Phase == "" || job.Status.State.Phase == batch.Pending {
		return false
	}

	return true
}

func newCondition(status batch.JobPhase, lastTransitionTime *metav1.Time) batch.JobCondition {
	return batch.JobCondition{
		Status:             status,
		LastTransitionTime: lastTransitionTime,
	}
}
