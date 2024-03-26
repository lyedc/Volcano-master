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

package scheduler

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/filewatcher"
	schedcache "volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
)

// Scheduler represents a "Volcano Scheduler".
// Scheduler watches for new unscheduled pods(PodGroup) in Volcano.
// It attempts to find nodes that can accommodate these pods and writes the binding information back to the API server.
type Scheduler struct {
	cache          schedcache.Cache
	schedulerConf  string
	fileWatcher    filewatcher.FileWatcher
	schedulePeriod time.Duration
	once           sync.Once

	mutex          sync.Mutex
	actions        []framework.Action
	plugins        []conf.Tier
	configurations []conf.Configuration
	metricsConf    map[string]string
	dumper         schedcache.Dumper
}

// NewScheduler returns a Scheduler
func NewScheduler(config *rest.Config, opt *options.ServerOption) (*Scheduler, error) {
	var watcher filewatcher.FileWatcher
	if opt.SchedulerConf != "" {
		var err error
		path := filepath.Dir(opt.SchedulerConf)
		watcher, err = filewatcher.NewFileWatcher(path)
		if err != nil {
			return nil, fmt.Errorf("failed creating filewatcher for %s: %v", opt.SchedulerConf, err)
		}
	}

	cache := schedcache.New(config, opt.SchedulerNames, opt.DefaultQueue, opt.NodeSelector, opt.NodeWorkerThreads, opt.IgnoredCSIProvisioners)
	scheduler := &Scheduler{
		schedulerConf:  opt.SchedulerConf,
		fileWatcher:    watcher,
		cache:          cache,
		schedulePeriod: opt.SchedulePeriod,
		dumper:         schedcache.Dumper{Cache: cache, RootDir: opt.CacheDumpFileDir},
	}

	return scheduler, nil
}

// Run initializes and starts the Scheduler. It loads the configuration,
// initializes the cache, and begins the scheduling process.
/*
会按照配置文件中的顺序执行action, action.Execute会调用plugin注册的方法, 对task、job进行处理，
最终会由backfill、alloc或者preempt动作中调用对应的方法添加到cache的BindFlowChannel中， 等待绑定
BindFlowChannel  会有sessionCache中的bind方法进行处理。最终调用k8s的bind方法进行绑定
*/
func (pc *Scheduler) Run(stopCh <-chan struct{}) {
	pc.loadSchedulerConf()
	go pc.watchSchedulerConf(stopCh)
	// Start cache for policy.
	pc.cache.SetMetricsConf(pc.metricsConf)
	pc.cache.Run(stopCh)
	pc.cache.WaitForCacheSync(stopCh)
	klog.V(2).Infof("Scheduler completes Initialization and start to run")
	go wait.Until(pc.runOnce, pc.schedulePeriod, stopCh)
	if options.ServerOpts.EnableCacheDumper {
		pc.dumper.ListenForSignal(stopCh)
	}
	go runSchedulerSocket()
}

// runOnce executes a single scheduling cycle. This function is called periodically
// as defined by the Scheduler's schedule period.
// 每次的调度执行都会开启一个session。。。调度工作将会在这个session中进行
func (pc *Scheduler) runOnce() {
	klog.V(4).Infof("Start scheduling ...")
	scheduleStartTime := time.Now()
	defer klog.V(4).Infof("End scheduling ...")

	pc.mutex.Lock()
	//action也是通过 framework.RegisterAction(reclaim.New()) 的init方法注册进来的，
	// 然后配置scheduler启动的配置文件决定采用哪些action
	actions := pc.actions
	plugins := pc.plugins
	configurations := pc.configurations
	pc.mutex.Unlock()

	// Load ConfigMap to check which action is enabled.
	// 动态watch配置， 所以每次调度都会重新加载配置
	conf.EnabledActionMap = make(map[string]bool)
	for _, action := range actions {
		conf.EnabledActionMap[action.Name()] = true
	}
	//framework中的主要对象其实是session, 一个调度周期会开启一个session,
	//调度工作将会在这个session中进行
	ssn := framework.OpenSession(pc.cache, plugins, configurations)
	defer func() {
		framework.CloseSession(ssn)
		metrics.UpdateE2eDuration(metrics.Duration(scheduleStartTime))
	}()
	// 执行actions
	for _, action := range actions {
		actionStartTime := time.Now()
		// 执行plugins，这里就是通过action触发plugins中的方法，过滤相关的pod是否可以被调度
		action.Execute(ssn)
		metrics.UpdateActionDuration(action.Name(), metrics.Duration(actionStartTime))
	}
}

func (pc *Scheduler) loadSchedulerConf() {
	klog.V(4).Infof("Start loadSchedulerConf ...")
	defer func() {
		actions, plugins := pc.getSchedulerConf()
		klog.V(2).Infof("Successfully loaded Scheduler conf, actions: %v, plugins: %v", actions, plugins)
	}()

	var err error
	pc.once.Do(func() {
		pc.actions, pc.plugins, pc.configurations, pc.metricsConf, err = unmarshalSchedulerConf(defaultSchedulerConf)
		if err != nil {
			klog.Errorf("unmarshal Scheduler config %s failed: %v", defaultSchedulerConf, err)
			panic("invalid default configuration")
		}
	})

	var config string
	if len(pc.schedulerConf) != 0 {
		confData, err := os.ReadFile(pc.schedulerConf)
		if err != nil {
			klog.Errorf("Failed to read the Scheduler config in '%s', using previous configuration: %v",
				pc.schedulerConf, err)
			return
		}
		config = strings.TrimSpace(string(confData))
	}

	actions, plugins, configurations, metricsConf, err := unmarshalSchedulerConf(config)
	if err != nil {
		klog.Errorf("Scheduler config %s is invalid: %v", config, err)
		return
	}

	pc.mutex.Lock()
	pc.actions = actions
	pc.plugins = plugins
	pc.configurations = configurations
	pc.metricsConf = metricsConf
	pc.mutex.Unlock()
}

func (pc *Scheduler) getSchedulerConf() (actions []string, plugins []string) {
	for _, action := range pc.actions {
		actions = append(actions, action.Name())
	}
	for _, tier := range pc.plugins {
		for _, plugin := range tier.Plugins {
			plugins = append(plugins, plugin.Name)
		}
	}
	return
}

func (pc *Scheduler) watchSchedulerConf(stopCh <-chan struct{}) {
	if pc.fileWatcher == nil {
		return
	}
	eventCh := pc.fileWatcher.Events()
	errCh := pc.fileWatcher.Errors()
	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				return
			}
			klog.V(4).Infof("watch %s event: %v", pc.schedulerConf, event)
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				pc.loadSchedulerConf()
				pc.cache.SetMetricsConf(pc.metricsConf)
			}
		case err, ok := <-errCh:
			if !ok {
				return
			}
			klog.Infof("watch %s error: %v", pc.schedulerConf, err)
		case <-stopCh:
			return
		}
	}
}
