/*
Copyright 2018 The Kubernetes Authors.

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

package reclaim

import (
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (ra *Action) Name() string {
	return "reclaim"
}

func (ra *Action) Initialize() {}

func (ra *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Reclaim ...")
	defer klog.V(5).Infof("Leaving Reclaim ...")
    // 对queue进行排序,按照小顶堆的方式进行排序,上面的就是优先级比较低的.
	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	queueMap := map[api.QueueID]*api.QueueInfo{}
    // 抢占
	preemptorsMap := map[api.QueueID]*util.PriorityQueue{}
	preemptorTasks := map[api.JobID]*util.PriorityQueue{}

	klog.V(3).Infof("There are <%d> Jobs and <%d> Queues in total for scheduling.",
		len(ssn.Jobs), len(ssn.Queues))

	for _, job := range ssn.Jobs {
		if job.IsPending() {
			continue
		}

		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip reclaim, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		if queue, found := ssn.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>",
				job.Queue, job.Namespace, job.Name)
			continue
		} else if _, existed := queueMap[queue.UID]; !existed {
			klog.V(4).Infof("Added Queue <%s> for Job <%s/%s>", queue.Name, job.Namespace, job.Name)
			queueMap[queue.UID] = queue
			// 放入到优先级队列中,是一个小顶堆,上面优先级低.
			queues.Push(queue)
		}
		// job中是否存在没有调度的task
		if job.HasPendingTasks() {
			if _, found := preemptorsMap[job.Queue]; !found {
				// 创建时间最早的放到最上面,按照时间排序,小顶堆,上面是创建时间比较靠前的
				preemptorsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			}
			preemptorsMap[job.Queue].Push(job)
			preemptorTasks[job.UID] = util.NewPriorityQueue(ssn.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				preemptorTasks[job.UID].Push(task)
			}
		}
	}

	for {
		// If no queues, break
		if queues.Empty() {
			break
		}

		var job *api.JobInfo
		var task *api.TaskInfo

		queue := queues.Pop().(*api.QueueInfo)
		// 资源是否超用了..
		// 这里应该理解为当这个资源queue资源用超了,那么这个queue中的job就不应该在参与抢占调度了...
		// 这个queue已经超载了，那么这个queue中的job就不应该在参与抢占调度了...
		if ssn.Overused(queue) {
			klog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		// Found "high" priority job
		jobs, found := preemptorsMap[queue.UID]
		if !found || jobs.Empty() {
			continue
		} else {
			job = jobs.Pop().(*api.JobInfo)
		}

		// Found "high" priority task to reclaim others
		if tasks, found := preemptorTasks[job.UID]; !found || tasks.Empty() {
			continue
		} else {
			task = tasks.Pop().(*api.TaskInfo)
		}

		if !ssn.Allocatable(queue, task) {
			klog.V(3).Infof("Queue <%s> is overused when considering task <%s>, ignore it.", queue.Name, task.Name)
			continue
		}

		if err := ssn.PrePredicateFn(task); err != nil {
			klog.V(3).Infof("PrePredicate for task %s/%s failed for: %v", task.Namespace, task.Name, err)
			continue
		}

		assigned := false
		for _, n := range ssn.Nodes {
			var statusSets util.StatusSets
			statusSets, err := ssn.PredicateFn(task, n)
			if err != nil {
				klog.V(5).Infof("reclaim predicates failed for task <%s/%s> on node <%s>: %v",
					task.Namespace, task.Name, n.Name, err)
				continue
			}

			// Allows scheduling to nodes that are in Success or Unschedulable state after filtering by predicate.
			if statusSets.ContainsUnschedulableAndUnresolvable() || statusSets.ContainsErrorSkipOrWait() {
				klog.V(5).Infof("predicates failed in reclaim for task <%s/%s> on node <%s>, reason is %s.",
					task.Namespace, task.Name, n.Name, statusSets.Message())
				continue
			}
			klog.V(3).Infof("Considering Task <%s/%s> on Node <%s>.",
				task.Namespace, task.Name, n.Name)

			var reclaimees []*api.TaskInfo
			// 遍历节点上的task,也就相当于是遍历节点上的pod了.
			for _, task := range n.Tasks {
				// Ignore non running task.
				if task.Status != api.Running {
					continue
				}
				//  task不允许抢占,也就是不允许被回收.,那么这个task就跳过.
				if !task.Preemptable {
					continue
				}

				if j, found := ssn.Jobs[task.Job]; !found {
					continue
					// 如果这个job的queue和job的queue不相同,那么就尝试回收这个queue的资源
					// 这里是抢占逻辑,不是传统意义的资源回收....只能抢占不是同一个queue的job的task.
				    // 要抢占的job不能占用自己queue中的资源。
				} else if j.Queue != job.Queue {
					q := ssn.Queues[j.Queue]
					// 借用的资源是否允许被回收.
					if !q.Reclaimable() {
						continue
					}
					// Clone task to avoid modify Task's status on node.
					// 需要回收的task列表.
					reclaimees = append(reclaimees, task.Clone())
				}
			}

			if len(reclaimees) == 0 {
				klog.V(4).Infof("No reclaimees on Node <%s>.", n.Name)
				continue
			}
            // 需要的资源，被驱逐的task列表
			victims := ssn.Reclaimable(task, reclaimees)

			if err := util.ValidateVictims(task, n, victims); err != nil {
				klog.V(3).Infof("No validated victims on Node <%s>: %v", n.Name, err)
				continue
			}

			resreq := task.InitResreq.Clone()
			reclaimed := api.EmptyResource()

			// Reclaim victims for tasks.
			for _, reclaimee := range victims {
				klog.Errorf("Try to reclaim Task <%s/%s> for Tasks <%s/%s>",
					reclaimee.Namespace, reclaimee.Name, task.Namespace, task.Name)
				// 这里回调用驱逐pod的方法。驱逐这个task对应的pod
				if err := ssn.Evict(reclaimee, "reclaim"); err != nil {
					klog.Errorf("Failed to reclaim Task <%s/%s> for Tasks <%s/%s>: %v",
						reclaimee.Namespace, reclaimee.Name, task.Namespace, task.Name, err)
					continue
				}
				reclaimed.Add(reclaimee.Resreq)
				// If reclaimed enough resources, break loop to avoid Sub panic.
				// task中的资源够了，就不在进行驱逐，抢占了。
				if resreq.LessEqual(reclaimed, api.Zero) {
					break
				}
			}

			klog.V(3).Infof("Reclaimed <%v> for task <%s/%s> requested <%v>.",
				reclaimed, task.Namespace, task.Name, task.InitResreq)

			if task.InitResreq.LessEqual(reclaimed, api.Zero) {
				if err := ssn.Pipeline(task, n.Name); err != nil {
					klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
						task.Namespace, task.Name, n.Name)
				}

				// Ignore error of pipeline, will be corrected in next scheduling loop.
				assigned = true

				break
			}
		}

		if assigned {
			jobs.Push(job)
		}
		queues.Push(queue)
	}
}

func (ra *Action) UnInitialize() {
}
