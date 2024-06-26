/*
Copyright 2019 The Kubernetes Authors.

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

package enqueue

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (enqueue *Action) Name() string {
	return "enqueue"
}

func (enqueue *Action) Initialize() {}

func (enqueue *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Enqueue ...")
	defer klog.V(5).Infof("Leaving Enqueue ...")
	// 传入queue排序方法，对queue进行排序
	// 实际是用的堆排序，下面的两个插件支持这个方法
	/*
	drf
	proportion
	*/
	// 给所有的queue排序
	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	queueSet := sets.NewString()
	jobsMap := map[api.QueueID]*util.PriorityQueue{}
	// session的Jobs是通过SchedulerCache中的Jobs拷贝过来的。
	for _, job := range ssn.Jobs {

		if job.ScheduleStartTimestamp.IsZero() {
			// 设置这个Job的调度开始时间。
			ssn.Jobs[job.UID].ScheduleStartTimestamp = metav1.Time{
				Time: time.Now(),
			}
		}
		// 如果job的queue不存在，跳过，因为queue是job资源分配的唯一路径。
		// session中的Queue是通过SchedulerCache中的Queues拷贝过来的。
		// schedulerCache中的Queues是通过queue的informer中的addQueue方法添加到cache中的。
		// queue和Job的关联是通过queueId是通过  (ji *JobInfo) SetPodGroup(pg *PodGroup) 这个方法说设定的，也就是在PodGroup的Add方法中完成的
		if queue, found := ssn.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>",
				job.Queue, job.Namespace, job.Name)
			continue
		} else if !queueSet.Has(string(queue.UID)) {
			// 如果queue没有添加到queues中，添加到queues中
			klog.V(5).Infof("Added Queue <%s> for Job <%s/%s>",
				queue.Name, job.Namespace, job.Name)

			queueSet.Insert(string(queue.UID))
			// 加入queues的queue会进行优先级排序，默认使用的是顶堆。
			queues.Push(queue)
		}
		// 如果job是pending状态，添加到jobsMap中
		// 如果job的状态不是pending的，就表示没有需要调度的job
		// 这里表示的是PodGroup的状态是pending。
		if job.IsPending() {
			if _, found := jobsMap[job.Queue]; !found {
				// 如果jobsMap中没有queue，创建一个queue
				// 一样是用的堆排序，下面的这些插件支持这个方法
				/*
				drf
				gang
				priority
				sla
				tdm
				*/
				// 对一个queue中的job按照优先级进行排序
				// 给queue中的job排序。
				jobsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			}
			klog.V(5).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
			// 加入到queue中的job会按照优先级进行排除
			jobsMap[job.Queue].Push(job)
		}
	}

	klog.V(3).Infof("Try to enqueue PodGroup to %d Queues", len(jobsMap))
	// 数据处理阶段
	// 遍历queues，对每个queue中的job进行准入判断
	// 这意味将会阻塞当前这个session
	for {
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*api.QueueInfo)

		// skip the Queue that has no pending job
		// 这里的job是一个优先级的队列
		jobs, found := jobsMap[queue.UID]
		if !found || jobs.Empty() {
			continue
		}
		// 弹出优先级最高的job
		job := jobs.Pop().(*api.JobInfo)
		// 如果job没有资源要求， 或者JobEnqueueable允许，标记job已经开始调度
		// 下面这些插件可以支持这个方法
		/*
		extender
		overcommit
		proportion
		resourcequota
		sla
		*/
		// ssn.JobEnqueueable(job) 判断job中声明的资源是否在requestQuota中能满足
		if job.PodGroup.Spec.MinResources == nil || ssn.JobEnqueueable(job) {
			/*
			JobEnqueued 用于标记Job已经开始调度
			overcommit。处理下podGroup中声明的资源
			*/
			ssn.JobEnqueued(job)
			// 将podgroup的状态设置为Inqueue,也就是修改了job的状态，从pending状态转换到入队中。
			job.PodGroup.Status.Phase = scheduling.PodGroupInqueue
			// 把更新job的状态
			ssn.Jobs[job.UID] = job
		}

		// Added Queue back until no job in Queue.
		// 把queue放回队列中，直到队列中没有job，这个时候queue中的job都放入到了ssh的jobs中了。
		queues.Push(queue)
	}
}

func (enqueue *Action) UnInitialize() {}
