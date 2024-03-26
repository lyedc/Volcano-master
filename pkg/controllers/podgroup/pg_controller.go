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

package podgroup

import (
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	appinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	informerfactory "volcano.sh/apis/pkg/client/informers/externalversions"
	vcinformer "volcano.sh/apis/pkg/client/informers/externalversions"
	schedulinginformer "volcano.sh/apis/pkg/client/informers/externalversions/scheduling/v1beta1"
	schedulinglister "volcano.sh/apis/pkg/client/listers/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/controllers/framework"
	"volcano.sh/volcano/pkg/features"
	commonutil "volcano.sh/volcano/pkg/util"
)

func init() {
	framework.RegisterController(&pgcontroller{})
}

// pgcontroller the Podgroup pgcontroller type.
type pgcontroller struct {
	kubeClient kubernetes.Interface
	vcClient   vcclientset.Interface

	podInformer coreinformers.PodInformer
	pgInformer  schedulinginformer.PodGroupInformer
	rsInformer  appinformers.ReplicaSetInformer

	informerFactory   informers.SharedInformerFactory
	vcInformerFactory vcinformer.SharedInformerFactory

	// A store of pods
	podLister corelisters.PodLister
	podSynced func() bool

	// A store of podgroups
	pgLister schedulinglister.PodGroupLister
	pgSynced func() bool

	// A store of replicaset
	rsSynced func() bool

	queue workqueue.RateLimitingInterface

	schedulerNames []string
	workers        uint32

	// To determine whether inherit owner's annotations for pods when create podgroup
	inheritOwnerAnnotations bool
}

func (pg *pgcontroller) Name() string {
	return "pg-controller"
}

// Initialize create new Podgroup Controller.
func (pg *pgcontroller) Initialize(opt *framework.ControllerOption) error {
	pg.kubeClient = opt.KubeClient
	pg.vcClient = opt.VolcanoClient
	pg.workers = opt.WorkerThreadsForPG

	pg.queue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	pg.schedulerNames = make([]string, len(opt.SchedulerNames))
	// 从启动的参数中获取到调度器的名称。。。
	copy(pg.schedulerNames, opt.SchedulerNames)
	pg.inheritOwnerAnnotations = opt.InheritOwnerAnnotations

	pg.informerFactory = opt.SharedInformerFactory
	pg.podInformer = opt.SharedInformerFactory.Core().V1().Pods()
	pg.podLister = pg.podInformer.Lister()
	pg.podSynced = pg.podInformer.Informer().HasSynced
	pg.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: pg.addPod,
	})

	factory := informerfactory.NewSharedInformerFactory(pg.vcClient, 0)
	pg.vcInformerFactory = factory
	pg.pgInformer = factory.Scheduling().V1beta1().PodGroups()
	pg.pgLister = pg.pgInformer.Lister()
	pg.pgSynced = pg.pgInformer.Informer().HasSynced

	if utilfeature.DefaultFeatureGate.Enabled(features.WorkLoadSupport) {
		pg.rsInformer = pg.informerFactory.Apps().V1().ReplicaSets()
		pg.rsSynced = pg.rsInformer.Informer().HasSynced
		pg.rsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    pg.addReplicaSet,
			UpdateFunc: pg.updateReplicaSet,
		})
	}
	return nil
}

// Run start NewPodgroupController.
func (pg *pgcontroller) Run(stopCh <-chan struct{}) {
	pg.informerFactory.Start(stopCh)
	pg.vcInformerFactory.Start(stopCh)

	for informerType, ok := range pg.informerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("caches failed to sync: %v", informerType)
		}
	}
	for informerType, ok := range pg.vcInformerFactory.WaitForCacheSync(stopCh) {
		if !ok {
			klog.Errorf("caches failed to sync: %v", informerType)
			return
		}
	}

	for i := 0; i < int(pg.workers); i++ {
		go wait.Until(pg.worker, 0, stopCh)
	}

	klog.Infof("PodgroupController is running ...... ")
}

func (pg *pgcontroller) worker() {
	for pg.processNextReq() {
	}
}

func (pg *pgcontroller) processNextReq() bool {
	obj, shutdown := pg.queue.Get()
	if shutdown {
		klog.Errorf("Fail to pop item from queue")
		return false
	}
   // 这个队列中的对象是通过创建了一个Pod后，添加到队列中的。
	req := obj.(podRequest)
	defer pg.queue.Done(req)
	// 获取pod对象
	pod, err := pg.podLister.Pods(req.podNamespace).Get(req.podName)
	if err != nil {
		klog.Errorf("Failed to get pod by <%v> from cache: %v", req, err)
		return true
	}
    // 注意这里的schedulerName的判断操作。
	if !commonutil.Contains(pg.schedulerNames, pod.Spec.SchedulerName) {
		klog.V(5).Infof("pod %v/%v field SchedulerName is not matched", pod.Namespace, pod.Name)
		return true
	}
	// 如果pod已经有podgroup， 则不再处理
	if pod.Annotations != nil && pod.Annotations[scheduling.KubeGroupNameAnnotationKey] != "" {
		klog.V(5).Infof("pod %v/%v has created podgroup", pod.Namespace, pod.Name)
		return true
	}

	// normal pod use volcano
	// 为pod分配podgroup，通过pod的名称，给没有podgroup的pod创建podgroup
	if err := pg.createNormalPodPGIfNotExist(pod); err != nil {
		klog.Errorf("Failed to handle Pod <%s/%s>: %v", pod.Namespace, pod.Name, err)
		pg.queue.AddRateLimited(req)
		return true
	}

	// If no error, forget it.
	pg.queue.Forget(req)

	return true
}
