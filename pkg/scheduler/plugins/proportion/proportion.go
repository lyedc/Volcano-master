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

package proportion

import (
	"math"
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/helpers"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

// PluginName indicates name of volcano scheduler plugin.
const PluginName = "proportion"

type proportionPlugin struct {
	totalResource  *api.Resource
	totalGuarantee *api.Resource
	queueOpts      map[api.QueueID]*queueAttr
	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

type queueAttr struct {
	queueID api.QueueID
	name    string
	weight  int32
	share   float64

	deserved  *api.Resource
	allocated *api.Resource
	request   *api.Resource
	// elastic represents the sum of job's elastic resource, job's elastic = job.allocated - job.minAvailable
	elastic *api.Resource
	// inqueue represents the resource request of the inqueue job
	inqueue    *api.Resource
	capability *api.Resource
	// realCapability represents the resource limit of the queue, LessEqual capability
	realCapability *api.Resource
	guarantee      *api.Resource
}

// New return proportion action
func New(arguments framework.Arguments) framework.Plugin {
	return &proportionPlugin{
		totalResource:   api.EmptyResource(),
		totalGuarantee:  api.EmptyResource(),
		queueOpts:       map[api.QueueID]*queueAttr{},
		pluginArguments: arguments,
	}
}

func (pp *proportionPlugin) Name() string {
	return PluginName
}

func (pp *proportionPlugin) OnSessionOpen(ssn *framework.Session) {
	// Prepare scheduling data for this session.
	/*
	for _, n := range ssn.Nodes {
			ssn.TotalResource.Add(n.Allocatable)
		}
	// 计算资源的方案.
	*/
	pp.totalResource.Add(ssn.TotalResource)

	klog.V(4).Infof("The total resource is <%v>", pp.totalResource)
	for _, queue := range ssn.Queues {
		if len(queue.Queue.Spec.Guarantee.Resource) == 0 {
			continue
		}
		guarantee := api.NewResource(queue.Queue.Spec.Guarantee.Resource)
		pp.totalGuarantee.Add(guarantee)
	}
	klog.V(4).Infof("The total guarantee resource is <%v>", pp.totalGuarantee)
	// Build attributes for Queues.
	for _, job := range ssn.Jobs {
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		if _, found := pp.queueOpts[job.Queue]; !found {
			queue := ssn.Queues[job.Queue]
			attr := &queueAttr{
				queueID: queue.UID,
				name:    queue.Name,
				weight:  queue.Weight,

				deserved:  api.EmptyResource(),
				allocated: api.EmptyResource(),
				request:   api.EmptyResource(),
				elastic:   api.EmptyResource(),
				inqueue:   api.EmptyResource(),
				guarantee: api.EmptyResource(),
			}
			// queue 中声明的资源限制.
			// capability表示该queue内所有podgroup使用资源量之和的上限，它是一个硬约束
			if len(queue.Queue.Spec.Capability) != 0 {
				attr.capability = api.NewResource(queue.Queue.Spec.Capability)
				if attr.capability.MilliCPU <= 0 {
					attr.capability.MilliCPU = math.MaxFloat64
				}
				if attr.capability.Memory <= 0 {
					attr.capability.Memory = math.MaxFloat64
				}
			}
			if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
				attr.guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
			}
			realCapability := pp.totalResource.Clone().Sub(pp.totalGuarantee).Add(attr.guarantee)
			// 如果 queue的资源限制不为空,那么队列的资源限制为取两个最小的.
			if attr.capability == nil {
				attr.realCapability = realCapability
			} else {
				attr.realCapability = helpers.Min(realCapability, attr.capability)
			}
			pp.queueOpts[job.Queue] = attr
			klog.V(4).Infof("Added Queue <%s> attributes.", job.Queue)
		}

		attr := pp.queueOpts[job.Queue]
		// 这里的request是 已经运行的task和pending的task之和
		// allocated为已经调度分配的资源
		// 每一次调度都会计算下资源的使用情况。，当pod被删除的回收，task就被回收，那么这里分配的资源也就是0了。
		for status, tasks := range job.TaskStatusIndex {
			if api.AllocatedStatus(status) {
				for _, t := range tasks {
					attr.allocated.Add(t.Resreq)
					attr.request.Add(t.Resreq)
				}
			} else if status == api.Pending {
				for _, t := range tasks {
					attr.request.Add(t.Resreq)
				}
			}
		}

		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue {
			// 如果podgroup已经处于inqueue状态，则计算inqueue资源,就是podgroup中声明的minResources的值.
			attr.inqueue.Add(job.GetMinResources())
		}

		// calculate inqueue resource for running jobs
		// the judgement 'job.PodGroup.Status.Running >= job.PodGroup.Spec.MinMember' will work on cases such as the following condition:
		// Considering a Spark job is completed(driver pod is completed) while the podgroup keeps running, the allocated resource will be reserved again if without the judgement.
		if job.PodGroup.Status.Phase == scheduling.PodGroupRunning &&
			job.PodGroup.Spec.MinResources != nil &&
			int32(util.CalculateAllocatedTaskNum(job)) >= job.PodGroup.Spec.MinMember {
			inqueued := util.GetInqueueResource(job, job.Allocated)
			attr.inqueue.Add(inqueued)
		}
		attr.elastic.Add(job.GetElasticResources())
		klog.V(5).Infof("Queue %s allocated <%s> request <%s> inqueue <%s> elastic <%s>",
			attr.name, attr.allocated.String(), attr.request.String(), attr.inqueue.String(), attr.elastic.String())
	}

	for queueID, queueInfo := range ssn.Queues {
		if _, ok := pp.queueOpts[queueID]; !ok {
			metrics.UpdateQueueAllocated(queueInfo.Name, 0, 0)
		}
	}

	// Record metrics
	for _, attr := range pp.queueOpts {
		metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory)
		metrics.UpdateQueueRequest(attr.name, attr.request.MilliCPU, attr.request.Memory)
		metrics.UpdateQueueWeight(attr.name, attr.weight)
		queue := ssn.Queues[attr.queueID]
		metrics.UpdateQueuePodGroupInqueueCount(attr.name, queue.Queue.Status.Inqueue)
		metrics.UpdateQueuePodGroupPendingCount(attr.name, queue.Queue.Status.Pending)
		metrics.UpdateQueuePodGroupRunningCount(attr.name, queue.Queue.Status.Running)
		metrics.UpdateQueuePodGroupUnknownCount(attr.name, queue.Queue.Status.Unknown)
	}
	// remaining来自节点可以分配的资源的总量.
	remaining := pp.totalResource.Clone()
	meet := map[api.QueueID]struct{}{}
	/*
	deserved 需要计算，分为多次循环，
	每次循环尝试给 queue 一点资源，
	直到给queue 的资源满足request，
	这个queue的分配就结束.
	当queue分配足够的资源后,就不在进行总权重的计算了.

	*/
	for {
		// 初始化总权重
		totalWeight := int32(0)
		// 遍历队列选项，计算不含已满足队列的总权重
		for _, attr := range pp.queueOpts {
			// 这里表示队列的资源已经满足或者达到了最大分配值了.不能再继续分配了.
			if _, found := meet[attr.queueID]; found {
				continue
			}
			totalWeight += attr.weight
		}

		// If no queues, break
		// 如果没有待处理的队列，结束循环
		if totalWeight == 0 {
			klog.V(4).Infof("Exiting when total weight is 0")
			break
		}
		// 复制当前剩余资源量, remaining来自节点可以分配的资源的总量.
		oldRemaining := remaining.Clone()
		// Calculates the deserved of each Queue.
		// increasedDeserved is the increased value for attr.deserved of processed queues
		// decreasedDeserved is the decreased value for attr.deserved of processed queues
		// 计算每个队列应得的资源量
		increasedDeserved := api.EmptyResource() // 记录增加的资源量
		decreasedDeserved := api.EmptyResource() // 记录减少的资源量
		for _, attr := range pp.queueOpts {
			klog.V(4).Infof("Considering Queue <%s>: weight <%d>, total weight <%d>.",
				attr.name, attr.weight, totalWeight)
			if _, found := meet[attr.queueID]; found {
				continue
			}
			// 复制队列当前的应得资源
			oldDeserved := attr.deserved.Clone()
			// 计算新的应得资源量
			attr.deserved.Add(remaining.Clone().Multi(float64(attr.weight) / float64(totalWeight)))
			// 如果设置了实际能力限制，则应用限制
			if attr.realCapability != nil {
				// deserved中资源的标定,取 deserved 和 realCapability中最小的,如果没有的值,去infinity中的默认值..
				attr.deserved.MinDimensionResource(attr.realCapability, api.Infinity)
			}
			// 应用请求资源量的限制
			// deserved 的值和请求量取最小值
			attr.deserved.MinDimensionResource(attr.request, api.Zero)
			// 确保应得资源量不小于保证资源量,取两者之间的较大值
			attr.deserved = helpers.Max(attr.deserved, attr.guarantee)
			pp.updateShare(attr)
			klog.V(4).Infof("Format queue <%s> deserved resource to <%v>", attr.name, attr.deserved)
            // 判断请求量如果小于等于了deserved，则认为该队列满足条件,不在继续分配资源了.
			if attr.request.LessEqual(attr.deserved, api.Zero) {
				meet[attr.queueID] = struct{}{}
				klog.V(4).Infof("queue <%s> is meet", attr.name)
			} else if reflect.DeepEqual(attr.deserved, oldDeserved) {
				meet[attr.queueID] = struct{}{}
				klog.V(4).Infof("queue <%s> is meet cause of the capability", attr.name)
			}

			klog.V(4).Infof("The attributes of queue <%s> in proportion: deserved <%v>, realCapability <%v>, allocate <%v>, request <%v>, elastic <%v>, share <%0.2f>",
				attr.name, attr.deserved, attr.realCapability, attr.allocated, attr.request, attr.elastic, attr.share)
			// 计算资源量的增减
			increased, decreased := attr.deserved.Diff(oldDeserved, api.Zero)
			increasedDeserved.Add(increased)
			decreasedDeserved.Add(decreased)

			// Record metrics
			metrics.UpdateQueueDeserved(attr.name, attr.deserved.MilliCPU, attr.deserved.Memory)
		}
		// 更新剩余资源量
		remaining.Sub(increasedDeserved).Add(decreasedDeserved)
		klog.V(4).Infof("Remaining resource is  <%s>", remaining)
		if remaining.IsEmpty() || reflect.DeepEqual(remaining, oldRemaining) {
			klog.V(4).Infof("Exiting when remaining is empty or no queue has more resource request:  <%v>", remaining)
			break
		}
	}

	ssn.AddQueueOrderFn(pp.Name(), func(l, r interface{}) int {
		lv := l.(*api.QueueInfo)
		rv := r.(*api.QueueInfo)

		if pp.queueOpts[lv.UID].share == pp.queueOpts[rv.UID].share {
			return 0
		}

		if pp.queueOpts[lv.UID].share < pp.queueOpts[rv.UID].share {
			return -1
		}

		return 1
	})

	ssn.AddReclaimableFn(pp.Name(), func(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		var victims []*api.TaskInfo
		allocations := map[api.QueueID]*api.Resource{}
		// allocated 表示task已经占用的资源
		// deserved 按照权重比queue应分配的资源。
		for _, reclaimee := range reclaimees {
			// 找到要回收的的jobInfo
			job := ssn.Jobs[reclaimee.Job]
			// 通过jobInfo中的queue找到这个job对应的queue的资源属性。
			attr := pp.queueOpts[job.Queue]

			if _, found := allocations[job.Queue]; !found {
				// 要被抢占的task的queue已经占用的资源
				allocations[job.Queue] = attr.allocated.Clone()
			}
			// 抢占的task的queue已经分配的资源
			allocated := allocations[job.Queue]
			// 判断这个job对应的queue的分配资源是否小于抢占的task的资源需求，如果小于的话，这个queue的资源就
			// 不满足被抢占，这个queue就无法被抢占。
			if allocated.LessPartly(reclaimer.Resreq, api.Zero) {
				klog.V(3).Infof("Failed to allocate resource for Task <%s/%s> in Queue <%s>, not enough resource.",
					reclaimee.Namespace, reclaimee.Name, job.Queue)
				continue
			}
			// 已分配的资源是否小于应得资源,如果不小于应得的资源（表示已经分配的资源大于了应得的资源，表示占用的别人的资源），那么就可以尝试进行回收。
			// 这里也就说明了，只要已经分配的资源大于了应得的资源的queue中的task，资源才能被抢占。
			// 如果queue没有超载，那么不会被抢占。也就相当于是资源的归还了。。
			if !allocated.LessEqual(attr.deserved, api.Zero) {
				// 分配的资源减去需要被占用的资源。
				allocated.Sub(reclaimee.Resreq)
				// 这个任务将要被回收
				victims = append(victims, reclaimee)
			}
		}
		klog.V(4).Infof("Victims from proportion plugins are %+v", victims)
		return victims, util.Permit
	})

	ssn.AddOverusedFn(pp.Name(), func(obj interface{}) bool {
		queue := obj.(*api.QueueInfo)
		attr := pp.queueOpts[queue.UID]
        // allocated为已经调度分配的资源
		// 判断已分配资源是否小于等于应得资源，确定队列是否过载,如果大于就表示用超了.
		overused := attr.deserved.LessEqual(attr.allocated, api.Zero)
		metrics.UpdateQueueOverused(attr.name, overused)
		if overused {
			klog.V(3).Infof("Queue <%v>: deserved <%v>, allocated <%v>, share <%v>",
				queue.Name, attr.deserved, attr.allocated, attr.share)
		}

		return overused
	})

	ssn.AddAllocatableFn(pp.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		attr := pp.queueOpts[queue.UID]

		free, _ := attr.deserved.Diff(attr.allocated, api.Zero)
		allocatable := candidate.Resreq.LessEqual(free, api.Zero)
		if !allocatable {
			klog.V(3).Infof("Queue <%v>: deserved <%v>, allocated <%v>; Candidate <%v>: resource request <%v>",
				queue.Name, attr.deserved, attr.allocated, candidate.Name, candidate.Resreq)
		}

		return allocatable
	})

	ssn.AddJobEnqueueableFn(pp.Name(), func(obj interface{}) int {
		job := obj.(*api.JobInfo)
		queueID := job.Queue
		attr := pp.queueOpts[queueID]
		queue := ssn.Queues[queueID]
		// If no capability is set, always enqueue the job.
		if attr.realCapability == nil {
			klog.V(4).Infof("Capability of queue <%s> was not set, allow job <%s/%s> to Inqueue.",
				queue.Name, job.Namespace, job.Name)
			return util.Permit
		}

		if job.PodGroup.Spec.MinResources == nil {
			klog.V(4).Infof("job %s MinResources is null.", job.Name)
			return util.Permit
		}
		minReq := job.GetMinResources()

		klog.V(5).Infof("job %s min resource <%s>, queue %s capability <%s> allocated <%s> inqueue <%s> elastic <%s>",
			job.Name, minReq.String(), queue.Name, attr.realCapability.String(), attr.allocated.String(), attr.inqueue.String(), attr.elastic.String())
		// The queue resource quota limit has not reached
		r := minReq.Add(attr.allocated).Add(attr.inqueue).Sub(attr.elastic)
		rr := attr.realCapability.Clone()

		for name := range rr.ScalarResources {
			if _, ok := r.ScalarResources[name]; !ok {
				delete(rr.ScalarResources, name)
			}
		}

		inqueue := r.LessEqual(rr, api.Infinity)
		klog.V(5).Infof("job %s inqueue %v", job.Name, inqueue)
		if inqueue {
			attr.inqueue.Add(job.GetMinResources())
			return util.Permit
		}
		ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, string(scheduling.PodGroupUnschedulableType), "queue resource quota insufficient")
		return util.Reject
	})

	// Register event handlers.
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			job := ssn.Jobs[event.Task.Job]
			attr := pp.queueOpts[job.Queue]
			attr.allocated.Add(event.Task.Resreq)
			metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory)

			pp.updateShare(attr)

			klog.V(4).Infof("Proportion AllocateFunc: task <%v/%v>, resreq <%v>,  share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share)
		},
		DeallocateFunc: func(event *framework.Event) {
			job := ssn.Jobs[event.Task.Job]
			attr := pp.queueOpts[job.Queue]
			attr.allocated.Sub(event.Task.Resreq)
			metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory)

			pp.updateShare(attr)

			klog.V(4).Infof("Proportion EvictFunc: task <%v/%v>, resreq <%v>,  share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share)
		},
	})
}

func (pp *proportionPlugin) OnSessionClose(ssn *framework.Session) {
	pp.totalResource = nil
	pp.totalGuarantee = nil
	pp.queueOpts = nil
}

func (pp *proportionPlugin) updateShare(attr *queueAttr) {
	res := float64(0)

	// TODO(k82cn): how to handle fragment issues?
	for _, rn := range attr.deserved.ResourceNames() {
		share := helpers.Share(attr.allocated.Get(rn), attr.deserved.Get(rn))
		if share > res {
			res = share
		}
	}

	attr.share = res
	metrics.UpdateQueueShare(attr.name, attr.share)
}
