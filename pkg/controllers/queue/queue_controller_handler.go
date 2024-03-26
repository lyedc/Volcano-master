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

package queue

import (
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/controllers/apis"
)

func (c *queuecontroller) enqueue(req *apis.Request) {
	c.queue.Add(req)
}

func (c *queuecontroller) addQueue(obj interface{}) {
	queue := obj.(*schedulingv1beta1.Queue)

	req := &apis.Request{
		QueueName: queue.Name,

		Event:  busv1alpha1.OutOfSyncEvent,
		Action: busv1alpha1.SyncQueueAction,
	}

	c.enqueue(req)
}

func (c *queuecontroller) deleteQueue(obj interface{}) {
	queue, ok := obj.(*schedulingv1beta1.Queue)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v.", obj)
			return
		}
		queue, ok = tombstone.Obj.(*schedulingv1beta1.Queue)
		if !ok {
			klog.Errorf("Tombstone contained object that is not a Queue: %#v.", obj)
			return
		}
	}

	c.pgMutex.Lock()
	defer c.pgMutex.Unlock()
	delete(c.podGroups, queue.Name)
}

func (c *queuecontroller) updateQueue(_, _ interface{}) {
	// currently do not care about queue update
}

func (c *queuecontroller) addPodGroup(obj interface{}) {
	pg := obj.(*schedulingv1beta1.PodGroup)
	key, _ := cache.MetaNamespaceKeyFunc(obj)

	c.pgMutex.Lock()
	defer c.pgMutex.Unlock()

	if c.podGroups[pg.Spec.Queue] == nil {
		c.podGroups[pg.Spec.Queue] = make(map[string]struct{})
	}
	c.podGroups[pg.Spec.Queue][key] = struct{}{}

	req := &apis.Request{
		QueueName: pg.Spec.Queue,

		Event:  busv1alpha1.OutOfSyncEvent,
		Action: busv1alpha1.SyncQueueAction,
	}

	c.enqueue(req)
}

func (c *queuecontroller) updatePodGroup(old, new interface{}) {
	oldPG := old.(*schedulingv1beta1.PodGroup)
	newPG := new.(*schedulingv1beta1.PodGroup)

	// Note: we have no use case update PodGroup.Spec.Queue
	// So do not consider it here.
	if oldPG.Status.Phase != newPG.Status.Phase {
		c.addPodGroup(newPG)
	}
}

func (c *queuecontroller) deletePodGroup(obj interface{}) {
	pg, ok := obj.(*schedulingv1beta1.PodGroup)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v.", obj)
			return
		}
		pg, ok = tombstone.Obj.(*schedulingv1beta1.PodGroup)
		if !ok {
			klog.Errorf("Tombstone contained object that is not a PodGroup: %#v.", obj)
			return
		}
	}

	key, _ := cache.MetaNamespaceKeyFunc(obj)

	c.pgMutex.Lock()
	defer c.pgMutex.Unlock()

	delete(c.podGroups[pg.Spec.Queue], key)

	req := &apis.Request{
		QueueName: pg.Spec.Queue,

		Event:  busv1alpha1.OutOfSyncEvent,
		Action: busv1alpha1.SyncQueueAction,
	}

	c.enqueue(req)
}

/*
// Command 定义了命令结构体。
type Command struct {
	// TypeMeta 包含了对象类型的基本信息。
	metav1.TypeMeta   `json:",inline"`
	// ObjectMeta 包含了对象的元数据，比如名称、命名空间、标签等。
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Action 定义了将对目标对象执行的操作。
	Action string `json:"action,omitempty" protobuf:"bytes,2,opt,name=action"`

	// TargetObject 定义了此命令的目标对象。
	TargetObject *metav1.OwnerReference `json:"target,omitempty" protobuf:"bytes,3,opt,name=target"`

	// Reason 是一个唯一的、单个词的、驼峰命名的原因说明，用于描述这个命令的执行原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`

	// Message 是一个可读性好的、详细说明此命令执行原因的消息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,4,opt,name=message"`
}
*/

func (c *queuecontroller) addCommand(obj interface{}) {
	cmd, ok := obj.(*busv1alpha1.Command)
	if !ok {
		klog.Errorf("Obj %v is not command.", obj)
		return
	}

	c.commandQueue.Add(cmd)
}

func (c *queuecontroller) getPodGroups(key string) []string {
	c.pgMutex.RLock()
	defer c.pgMutex.RUnlock()

	if c.podGroups[key] == nil {
		return nil
	}
	podGroups := make([]string, 0, len(c.podGroups[key]))
	for pgKey := range c.podGroups[key] {
		podGroups = append(podGroups, pgKey)
	}

	return podGroups
}

func (c *queuecontroller) recordEventsForQueue(name, eventType, reason, message string) {
	queue, err := c.queueLister.Get(name)
	if err != nil {
		klog.Errorf("Get queue %s failed for %v.", name, err)
		return
	}

	c.recorder.Event(queue, eventType, reason, message)
}
