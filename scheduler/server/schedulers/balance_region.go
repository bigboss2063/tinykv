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

type StoreInfos []*core.StoreInfo

func (s StoreInfos) Len() int {
	return len(s)
}

func (s StoreInfos) Less(i, j int) bool {
	return s[i].GetRegionSize() < s[j].GetRegionSize()
}

func (s StoreInfos) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
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

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	storeDownTime := cluster.GetMaxStoreDownTime()
	suitAbleStores := StoreInfos{}
	for _, storeInfo := range cluster.GetStores() {
		if storeInfo.DownTime() < storeDownTime {
			suitAbleStores = append(suitAbleStores, storeInfo)
		}
	}
	sort.Sort(sort.Reverse(suitAbleStores))
	var selectRegion *core.RegionInfo
	var moveFromStore *core.StoreInfo
	for _, storeInfo := range suitAbleStores {
		cluster.GetPendingRegionsWithLock(storeInfo.GetID(), func(rc core.RegionsContainer) {
			selectRegion = rc.RandomRegion(nil, nil)
		})
		if selectRegion == nil {
			cluster.GetFollowersWithLock(storeInfo.GetID(), func(rc core.RegionsContainer) {
				selectRegion = rc.RandomRegion(nil, nil)
			})
		}
		if selectRegion == nil {
			cluster.GetLeadersWithLock(storeInfo.GetID(), func(rc core.RegionsContainer) {
				selectRegion = rc.RandomRegion(nil, nil)
			})
		}
		if selectRegion != nil {
			moveFromStore = storeInfo
			break
		}
	}
	if len(selectRegion.GetPeers()) < cluster.GetMaxReplicas() {
		return nil
	}
	if moveFromStore == nil {
		return nil
	}
	var moveToStore *core.StoreInfo
	storeIds := selectRegion.GetStoreIds()
	for i := len(suitAbleStores) - 1; i >= 0; i-- {
		if _, ok := storeIds[suitAbleStores[i].GetID()]; ok {
			continue
		}
		if suitAbleStores[i].GetID() != moveFromStore.GetID() {
			moveToStore = suitAbleStores[i]
			break
		}
	}
	if moveToStore == nil {
		return nil
	}
	if moveFromStore.GetRegionSize()-moveToStore.GetRegionSize() < 2*selectRegion.GetApproximateSize() {
		return nil
	}
	peer, err := cluster.AllocPeer(moveToStore.GetID())
	if err != nil {
		panic(err)
	}
	op, err := operator.CreateMovePeerOperator(fmt.Sprintf("move region %v from %v to %v", selectRegion.GetID(),
		moveFromStore.GetID(), moveToStore.GetID()), cluster, selectRegion, operator.OpBalance, moveFromStore.GetID(),
		moveToStore.GetID(), peer.GetId())
	if err != nil {
		panic(err)
	}
	return op
}
