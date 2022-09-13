// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeSlice []*core.StoreInfo

func (a storeSlice) Len() int           { return len(a) }
func (a storeSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a storeSlice) Less(i, j int) bool { return a[i].GetRegionSize() < a[j].GetRegionSize() }

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 找出所有DownTime小于MaxStoreDownTime的store
	suitableStores := make(storeSlice, 0)
	stores := cluster.GetStores()
	for _, store := range stores {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	sort.Sort(suitableStores)
	if len(suitableStores) < 2 {
		return nil
	}
	i := len(suitableStores) - 1
	var region *core.RegionInfo
	// 根据regionSize从大到小枚举，选出一个最大的region
	for ; i > 0; i-- {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			break
		}
		cluster.GetFollowersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			break
		}
		cluster.GetLeadersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			break
		}
	}
	if region == nil {
		return nil
	}
	var srcStore, destStore *core.StoreInfo
	srcStore = suitableStores[i]
	storeIds := region.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}
	// 从小到大，找到目标store
	for j := 0; j < i; j++ {
		if _, ok := storeIds[suitableStores[j].GetID()]; !ok {
			destStore = suitableStores[j]
			break
		}
	}
	if destStore == nil {
		return nil
	}
	if srcStore.GetRegionSize()-destStore.GetRegionSize() < 2*region.GetApproximateSize() {
		return nil
	}
	newPeer, err := cluster.AllocPeer(destStore.GetID())
	if err != nil {
		return nil
	}
	desc := fmt.Sprintf("move-from-%d-to-%d", srcStore.GetID(), destStore.GetID())
	op, err := operator.CreateMovePeerOperator(desc, cluster, region, operator.OpBalance, srcStore.GetID(), destStore.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return op
}
