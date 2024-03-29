/*
Copyright 2021 The Volcano Authors.

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

package cpumanager

import (
	"fmt"
	"sort"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/utils/cpuset"
)

type cpuAccumulator struct {
	topo          *topology.CPUTopology
	details       topology.CPUDetails
	numCPUsNeeded int
	result        cpuset.CPUSet
}

func newCPUAccumulator(topo *topology.CPUTopology, availableCPUs cpuset.CPUSet, numCPUs int) *cpuAccumulator {
	return &cpuAccumulator{
		topo:          topo,
		details:       topo.CPUDetails.KeepOnly(availableCPUs),
		numCPUsNeeded: numCPUs,
		result:        cpuset.New(),
	}
}

func (a *cpuAccumulator) take(cpus cpuset.CPUSet) {
	a.result = a.result.Union(cpus)
	a.details = a.details.KeepOnly(a.details.CPUs().Difference(a.result))
	a.numCPUsNeeded -= cpus.Size()
}

// freeSockets Returns free socket IDs as a slice sorted by:
// - socket ID, ascending.
func (a *cpuAccumulator) freeSockets() []int {
	return a.details.Sockets().Intersection(a.details.CPUs()).List()
}

// freeCores Returns core IDs as a slice sorted by:
// - the number of whole available cores on the socket, ascending
// - socket ID, ascending
// - core ID, ascending
func (a *cpuAccumulator) freeCores() []int {
	socketIDs := a.details.Sockets().UnsortedList()
	sort.Slice(socketIDs,
		func(i, j int) bool {
			iCores := a.details.CoresInSockets(socketIDs[i]).Intersection(a.details.CPUs())
			jCores := a.details.CoresInSockets(socketIDs[j]).Intersection(a.details.CPUs())
			return iCores.Size() < jCores.Size() || socketIDs[i] < socketIDs[j]
		})

	coreIDs := []int{}
	for _, s := range socketIDs {
		coreIDs = append(coreIDs, a.details.CoresInSockets(s).Intersection(a.details.CPUs()).List()...)
	}
	return coreIDs
}

// freeCPUs Returns CPU IDs as a slice sorted by:
// - socket affinity with result
// - number of CPUs available on the same socket
// - number of CPUs available on the same core
// - socket ID.
// - core ID.
func (a *cpuAccumulator) freeCPUs() []int {
	result := []int{}
	cores := a.details.Cores().List()

	sort.Slice(
		cores,
		func(i, j int) bool {
			iCore := cores[i]
			jCore := cores[j]

			iCPUs := a.topo.CPUDetails.CPUsInCores(iCore).List()
			jCPUs := a.topo.CPUDetails.CPUsInCores(jCore).List()

			iSocket := a.topo.CPUDetails[iCPUs[0]].SocketID
			jSocket := a.topo.CPUDetails[jCPUs[0]].SocketID

			// Compute the number of CPUs in the result reside on the same socket
			// as each core.
			iSocketColoScore := a.topo.CPUDetails.CPUsInSockets(iSocket).Intersection(a.result).Size()
			jSocketColoScore := a.topo.CPUDetails.CPUsInSockets(jSocket).Intersection(a.result).Size()

			// Compute the number of available CPUs available on the same socket
			// as each core.
			iSocketFreeScore := a.details.CPUsInSockets(iSocket).Size()
			jSocketFreeScore := a.details.CPUsInSockets(jSocket).Size()

			// Compute the number of available CPUs on each core.
			iCoreFreeScore := a.details.CPUsInCores(iCore).Size()
			jCoreFreeScore := a.details.CPUsInCores(jCore).Size()

			return iSocketColoScore > jSocketColoScore ||
				iSocketFreeScore < jSocketFreeScore ||
				iCoreFreeScore < jCoreFreeScore ||
				iSocket < jSocket ||
				iCore < jCore
		})

	// For each core, append sorted CPU IDs to result.
	for _, core := range cores {
		result = append(result, a.details.CPUsInCores(core).List()...)
	}
	return result
}

func (a *cpuAccumulator) needs(n int) bool {
	return a.numCPUsNeeded >= n
}

func (a *cpuAccumulator) isSatisfied() bool {
	return a.numCPUsNeeded < 1
}

func (a *cpuAccumulator) isFailed() bool {
	return a.numCPUsNeeded > a.details.CPUs().Size()
}

// takeByTopology return the assigned cpuset
func takeByTopology(topo *topology.CPUTopology, availableCPUs cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
	acc := newCPUAccumulator(topo, availableCPUs, numCPUs)
	if acc.isSatisfied() {
		return acc.result, nil
	}
	if acc.isFailed() {
		return cpuset.New(), fmt.Errorf("not enough cpus available to satisfy request")
	}

	// Algorithm: topology-aware best-fit
	// 1. Acquire whole sockets, if available and the container requires at
	//    least a socket's-worth of CPUs.
	if acc.needs(acc.topo.CPUsPerSocket()) {
		for _, s := range acc.freeSockets() {
			klog.V(4).Infof("[cpumanager] takeByTopology: claiming socket [%d]", s)
			acc.take(acc.details.CPUsInSockets(s))
			if acc.isSatisfied() {
				return acc.result, nil
			}
			if !acc.needs(acc.topo.CPUsPerSocket()) {
				break
			}
		}
	}

	// 2. Acquire whole cores, if available and the container requires at least
	//    a core's-worth of CPUs.
	if acc.needs(acc.topo.CPUsPerCore()) {
		for _, c := range acc.freeCores() {
			klog.V(4).Infof("[cpumanager] takeByTopology: claiming core [%d]", c)
			acc.take(acc.details.CPUsInCores(c))
			if acc.isSatisfied() {
				return acc.result, nil
			}
			if !acc.needs(acc.topo.CPUsPerCore()) {
				break
			}
		}
	}

	// 3. Acquire single threads, preferring to fill partially-allocated cores
	//    on the same sockets as the whole cores we have already taken in this
	//    allocation.
	for _, c := range acc.freeCPUs() {
		klog.V(4).Infof("[cpumanager] takeByTopology: claiming CPU [%d]", c)
		if acc.needs(1) {
			acc.take(cpuset.New(c))
		}
		if acc.isSatisfied() {
			return acc.result, nil
		}
	}

	return cpuset.New(), fmt.Errorf("failed to allocate cpus")
}
