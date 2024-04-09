/*
Copyright 2022 The Volcano Authors.

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

package usage

import (
	"fmt"
	"time"

	"volcano.sh/volcano/pkg/scheduler/metrics/source"

	"k8s.io/klog/v2"
	k8sFramework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName            = "usage"
	thresholdSection      = "thresholds"
	MetricsActiveTime     = 5 * time.Minute
	NodeUsageCPUExtend    = "the CPU load of the node exceeds the upper limit."
	NodeUsageMemoryExtend = "the memory load of the node exceeds the upper limit."
)

/*
   actions: "enqueue, allocate, backfill"
   tiers:
   - plugins:
     - name: usage
       enablePredicate: false  # If the value is false, new pod scheduling is not disabled when the node load reaches the threshold. If the value is true or left blank, new pod scheduling is disabled.
       arguments:
         usage.weight: 5
         cpu.weight: 1
         memory.weight: 1
         thresholds:
           cpu: 80
           mem: 80
*/

const AVG string = "average"

type usagePlugin struct {
	pluginArguments framework.Arguments
	usageWeight     int
	cpuWeight       int
	memoryWeight    int
	usageType       string
	cpuThresholds   float64
	memThresholds   float64
	period          string
}

// New function returns usagePlugin object
func New(args framework.Arguments) framework.Plugin {
	var plugin = &usagePlugin{
		pluginArguments: args,
		usageWeight:     5,
		cpuWeight:       1,
		memoryWeight:    1,
		usageType:       AVG,
		cpuThresholds:   80,
		memThresholds:   80,
		period:          source.NODE_METRICS_PERIOD,
	}
	args.GetInt(&plugin.usageWeight, "usage.weight")
	args.GetInt(&plugin.cpuWeight, "cpu.weight")
	args.GetInt(&plugin.memoryWeight, "memory.weight")

	argsValue, ok := plugin.pluginArguments[thresholdSection]
	if !ok {
		klog.Errorf("Failed to obtain thresholds information, usage plugin arguments is %v", plugin.pluginArguments)
		return plugin
	}

	thresholdArgs, ok := argsValue.(map[interface{}]interface{})
	if !ok {
		klog.Errorf("Failed to convert the thresholds information, thresholds args values is %v", argsValue)
		return plugin
	}
	for resourceName, threshold := range thresholdArgs {
		resource, _ := resourceName.(string)
		value, _ := threshold.(int)
		switch resource {
		case "cpu":
			plugin.cpuThresholds = float64(value)
		case "mem":
			plugin.memThresholds = float64(value)
		}
	}

	return plugin
}

func (up *usagePlugin) Name() string {
	return PluginName
}

func (up *usagePlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter usage plugin ...")
	defer func() {
		klog.V(5).Infof("Leaving usage plugin ...")
	}()

	if klog.V(4).Enabled() {
		for node, nodeInfo := range ssn.Nodes {
			klog.V(4).Infof("node:%v, cpu usage:%v, mem usage:%v, metrics time is %v",
				node, nodeInfo.ResourceUsage.CPUUsageAvg, nodeInfo.ResourceUsage.MEMUsageAvg, nodeInfo.ResourceUsage.MetricsTime)
		}
	}

	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) ([]*api.Status, error) {
		predicateStatus := make([]*api.Status, 0)
		usageStatus := &api.Status{}

		now := time.Now()
		// 判断如果节点的资源使用率数据没有更新，则认为该节点资源使用率数据无效，直接通过
		if up.period == "" || now.Sub(node.ResourceUsage.MetricsTime) > MetricsActiveTime {
			klog.V(4).Infof("The period(%s) is empty or the usage metrics data is not updated for more than %v minutes, "+
				"Usage plugin filter for task %s/%s on node %s pass, metrics time is %v. ", up.period, MetricsActiveTime, task.Namespace, task.Name, node.Name, node.ResourceUsage.MetricsTime)

			usageStatus.Code = api.Success
			predicateStatus = append(predicateStatus, usageStatus)
			return predicateStatus, nil
		}

		klog.V(4).Infof("predicateFn cpuUsageAvg:%v,predicateFn memUsageAvg:%v", up.cpuThresholds, up.memThresholds)
		// 判断节点的cpu 资源使用率是否超过阈值,现在默认的是10m中的一个平均时间
		if node.ResourceUsage.CPUUsageAvg[up.period] > up.cpuThresholds {
			klog.V(3).Infof("Node %s cpu usage %f exceeds the threshold %f", node.Name, node.ResourceUsage.CPUUsageAvg[up.period], up.cpuThresholds)
			usageStatus.Code = api.UnschedulableAndUnresolvable
			usageStatus.Reason = NodeUsageCPUExtend
			predicateStatus = append(predicateStatus, usageStatus)
			return predicateStatus, fmt.Errorf("Plugin %s predicates failed, because of %s", up.Name(), NodeUsageCPUExtend)
		}
		// 判断节点的cpu 资源使用率是否超过阈值,现在默认的是10m中的一个平均时间
		if node.ResourceUsage.MEMUsageAvg[up.period] > up.memThresholds {
			klog.V(3).Infof("Node %s mem usage %f exceeds the threshold %f", node.Name, node.ResourceUsage.MEMUsageAvg[up.period], up.memThresholds)
			// 超过阈值，就表示不能调度
			usageStatus.Code = api.UnschedulableAndUnresolvable
			usageStatus.Reason = NodeUsageMemoryExtend
			predicateStatus = append(predicateStatus, usageStatus)
			return predicateStatus, fmt.Errorf("Plugin %s predicates failed, because of %s", up.Name(), NodeUsageMemoryExtend)
		}

		klog.V(4).Infof("Usage plugin filter for task %s/%s on node %s pass.", task.Namespace, task.Name, node.Name)
		return predicateStatus, nil
	}
    // 打分函数
	nodeOrderFn := func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		score := 0.0
		now := time.Now()
		// 指标过期，得分为0，不参与得分计算
		if up.period == "" || now.Sub(node.ResourceUsage.MetricsTime) > MetricsActiveTime {
			klog.V(4).Infof("The period(%s) is empty or the usage metrics data is not updated for more than %v minutes, "+
				"Usage plugin score for task %s/%s on node %s is 0, metrics time is %v. ", up.period, MetricsActiveTime, task.Namespace, task.Name, node.Name, node.ResourceUsage.MetricsTime)
			return 0, nil
		}
        // 获取cpu使用的情况
		cpuUsage, exist := node.ResourceUsage.CPUUsageAvg[up.period]
		klog.V(4).Infof("Node %s cpu usage is %f.", node.Name, cpuUsage)
		if !exist {
			return 0, nil
		}
		// 计算cpu的得分情况100-cpu的使用情况，然后乘以cpu的权重值。
		cpuScore := (100 - cpuUsage) / 100 * float64(up.cpuWeight)

		memoryUsage, exist := node.ResourceUsage.MEMUsageAvg[up.period]
		klog.V(4).Infof("Node %s memory usage is %f.", node.Name, memoryUsage)
		if !exist {
			return 0, nil
		}
		// 计算内存的得分情况100-内存的使用情况，然后乘以内存的权重值。
		memoryScore := (100 - memoryUsage) / 100 * float64(up.memoryWeight)
		// 总得分 = cpu得分 + 内存得分 除以 权重值
		score = (cpuScore + memoryScore) / float64((up.cpuWeight + up.memoryWeight))
		// 最终得分
		score *= float64(k8sFramework.MaxNodeScore * int64(up.usageWeight))
		klog.V(4).Infof("Node %s score for task %s is %f.", node.Name, task.Name, score)
		return score, nil
	}

	ssn.AddPredicateFn(up.Name(), predicateFn)
	ssn.AddNodeOrderFn(up.Name(), nodeOrderFn)
}

func (up *usagePlugin) OnSessionClose(ssn *framework.Session) {}
