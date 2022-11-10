package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"strconv"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	log.Debugf("%v begin handle a raft ready", d.Tag)
	defer func() {
		log.Debugf("%v end handle a raft ready", d.Tag)
	}()
	if d.stopped {
		log.Debugf("%v already stopped", d.Tag)
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		log.Debugf("%v there is no ready", d.Tag)
		return
	}
	ready := d.RaftGroup.Ready()
	applySnapResult, _ := d.peerStorage.SaveReadyState(&ready)
	if applySnapResult.Region != nil {
		// 更新 storeMeta 中的 region 信息
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.setRegion(applySnapResult.Region, d.peer)
		if len(applySnapResult.PrevRegion.Peers) > 0 {
			d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
		}
		item := d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
		log.Infof("%v insert regionItem to regionRanges %v", d.Tag, item)
		d.ctx.storeMeta.Unlock()
	}
	d.Send(d.ctx.trans, ready.Messages)
	if len(ready.CommittedEntries) > 0 {
		log.Debugf("%v has committed entries len %v", d.Tag, len(ready.CommittedEntries))
		kvWB := new(engine_util.WriteBatch)
		for _, entry := range ready.CommittedEntries {
			prop := d.findProposal(&entry)
			var txn *badger.Txn
			raftResp := newCmdResp()
			if entry.EntryType == pb.EntryType_EntryConfChange {
				if d.handleConfChange(&entry, raftResp, kvWB) {
					// 如果当前节点被 destroy 了就直接返回
					return
				}
			} else {
				raftReq := &raft_cmdpb.RaftCmdRequest{}
				_ = raftReq.Unmarshal(entry.Data)
				err := util.CheckRegionEpoch(raftReq, d.Region(), true)
				if err != nil {
					raftResp = ErrResp(err)
				} else {
					if len(raftReq.Requests) > 0 {
						txn = d.handleNormalRequest(raftReq, raftResp, kvWB, entry, prop)
					} else if raftReq.AdminRequest != nil {
						d.handleAdminRequest(raftReq, kvWB)
					}
				}
			}
			d.peerStorage.applyState.AppliedIndex = entry.Index
			err := kvWB.SetMeta(meta.ApplyStateKey(d.Region().Id), d.peerStorage.applyState)
			if err != nil {
				panic(err)
			}
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			log.Debugf("%v write applyState to disk", d.Tag)
			if prop != nil && prop.cb != nil {
				prop.cb.Txn = txn
				prop.cb.Done(raftResp)
				log.Debugf("%v finish to handle raft cmd", d.Tag)
			}
		}
	}
	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) findProposal(entry *pb.Entry) *proposal {
	var prop *proposal
	for len(d.proposals) > 0 {
		prop = d.proposals[0]
		if prop.index == entry.Index && prop.term == entry.Term {
			// 找到对应的 proposal 用于回复 callback
			log.Debugf("%v find proposal for index %v", d.Tag, entry.Index)
			d.proposals = d.proposals[1:]
			break
		}
		if prop.term < entry.Term || (prop.term == entry.Term && prop.index < entry.Index) {
			// 如果 proposal 过期了就返回一个过期的 Err
			d.proposals = d.proposals[1:]
			prop.cb.Done(ErrRespStaleCommand(entry.Term))
			prop = nil
			continue
		}
		d.proposals = d.proposals[1:]
		prop = nil
	}
	return prop
}

func (d *peerMsgHandler) handleNormalRequest(request *raft_cmdpb.RaftCmdRequest, raftResp *raft_cmdpb.RaftCmdResponse,
	kvWB *engine_util.WriteBatch, entry pb.Entry, prop *proposal) *badger.Txn {
	// Normal Request
	var txn *badger.Txn
	err := d.checkKeyInRegion(request)
	if err != nil {
		raftResp = ErrResp(err)
	}
	for _, req := range request.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			log.Debugf("%v prepare to apply a Get cmd index %v", d.Tag, entry.Index)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				panic(err)
			}
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			log.Debugf("%v apply a Get cmd index %v", d.Tag, entry.Index)
			if prop != nil {
				val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				if err != nil {
					panic(err)
				}
				raftResp.Responses = append(raftResp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{
						Value: val,
					},
				})
			}
		case raft_cmdpb.CmdType_Snap:
			d.peerStorage.applyState.AppliedIndex = entry.Index
			err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				panic(err)
			}
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			if prop != nil {
				txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				// 一定不能直接把引用传过去，否则传过去之后如果当前 region 的 key range 被修改的话就会出现错误！
				cloneRegion := metapb.Region{}
				_ = util.CloneMsg(d.Region(), &cloneRegion)
				raftResp.Responses = append(raftResp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: &cloneRegion},
				})
			}
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			// 更新 SizeDiffHint，当发现 SizeDiffHint 不超过分裂尺寸的八分之一时就不发送 SplitCheckTask 检查是否进行分裂
			// 这样可以使得扫描数据库来获取当前 ApproximateSize 的次数减少，提高性能
			d.SizeDiffHint += uint64(len(req.Put.Key) + len(req.Put.Value))
			if prop != nil {
				raftResp.Responses = append(raftResp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{},
				})
			}
		case raft_cmdpb.CmdType_Delete:
			val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Delete.Cf, req.Delete.Key)
			// 同 Put 操作
			d.SizeDiffHint -= uint64(len(req.Delete.Key) + len(val))
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
			if prop != nil {
				raftResp.Responses = append(raftResp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{},
				})
			}
		default:
			panic("err type of cmd!")
		}
	}
	return txn
}

func (d *peerMsgHandler) handleAdminRequest(request *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) {
	switch request.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		log.Debugf("%v receive a compact log cmd", d.Tag)
		d.peerStorage.applyState.TruncatedState.Index = request.AdminRequest.CompactLog.CompactIndex
		d.peerStorage.applyState.TruncatedState.Term = request.AdminRequest.CompactLog.CompactTerm
		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic(err)
		}
		d.ScheduleCompactLog(request.AdminRequest.CompactLog.CompactIndex)
	case raft_cmdpb.AdminCmdType_Split:
		log.Debugf("%v receive a split cmd", d.Tag)
		d.ctx.storeMeta.Lock()
		leftRegion := &metapb.Region{}
		err := util.CloneMsg(d.Region(), leftRegion)
		if err != nil {
			panic(err)
		}
		if engine_util.ExceedEndKey(request.AdminRequest.Split.SplitKey, leftRegion.EndKey) {
			return
		}
		rightRegion := &metapb.Region{}
		err = util.CloneMsg(d.Region(), rightRegion)
		if err != nil {
			panic(err)
		}
		leftRegion.RegionEpoch.Version++
		rightRegion.RegionEpoch.Version++
		rightRegion.Id = request.AdminRequest.Split.NewRegionId
		rightRegion.StartKey = request.AdminRequest.Split.SplitKey
		rightRegion.EndKey = leftRegion.EndKey
		leftRegion.EndKey = request.AdminRequest.Split.SplitKey
		for i, id := range request.AdminRequest.Split.NewPeerIds {
			rightRegion.Peers[i].Id = id
		}
		peer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, rightRegion)
		// 参考 maybeCreatePeer 会将 peer 注册进 router 后要发送一条 MsgTypeStart 来启动 peer
		d.ctx.router.register(peer)
		_ = d.ctx.router.send(rightRegion.Id, message.Msg{Type: message.MsgTypeStart})
		if err != nil {
			panic(err)
		}
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
		log.Infof("%v replaceOrInsert regionItem to regionRanges", d.Tag)
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})
		log.Infof("%v replaceOrInsert regionItem to regionRanges", peer.Tag)
		d.ctx.storeMeta.setRegion(leftRegion, d.peer)
		d.ctx.storeMeta.setRegion(rightRegion, peer)
		cloneRegion := &metapb.Region{}
		_ = util.CloneMsg(rightRegion, cloneRegion)
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
			Region:          cloneRegion,
			Peer:            peer.Meta,
			PendingPeers:    peer.CollectPendingPeers(),
			ApproximateSize: peer.ApproximateSize,
		}
		meta.WriteRegionState(kvWB, leftRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, rightRegion, rspb.PeerState_Normal)
		d.ctx.storeMeta.Unlock()
	}
}

func (d *peerMsgHandler) handleConfChange(entry *pb.Entry, raftResp *raft_cmdpb.RaftCmdResponse, kvWB *engine_util.WriteBatch) bool {
	cc := &pb.ConfChange{}
	_ = cc.Unmarshal(entry.Data)
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		d.ctx.storeMeta.Lock()
		region := d.Region()
		region.RegionEpoch.ConfVer++
		storeId, _ := strconv.Atoi(string(cc.Context))
		peer := &metapb.Peer{
			Id:      cc.NodeId,
			StoreId: uint64(storeId),
		}
		region.Peers = append(region.Peers, peer)
		d.ctx.storeMeta.setRegion(region, d.peer)
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		d.insertPeerCache(peer)
		d.ctx.storeMeta.Unlock()
	case pb.ConfChangeType_RemoveNode:
		if cc.NodeId == d.peer.PeerId() {
			d.destroyPeer()
			return true
		}
		d.ctx.storeMeta.Lock()
		region := d.Region()
		region.RegionEpoch.ConfVer++
		for i, p := range region.Peers {
			if p.Id == cc.NodeId {
				region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
				break
			}
		}
		d.ctx.storeMeta.setRegion(region, d.peer)
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		d.removePeerCache(cc.NodeId)
		d.ctx.storeMeta.Unlock()
	}
	d.RaftGroup.ApplyConfChange(*cc)
	cloneRegion := &metapb.Region{}
	_ = util.CloneMsg(d.Region(), cloneRegion)
	raftResp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer, ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: cloneRegion}}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	return false
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	log.Debugf("%v begin handle a msg", d.Tag)
	defer func() {
		log.Debugf("%v end handle a msg", d.Tag)
	}()
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// 如果不存在该 region，直接返回
	if _, ok := d.ctx.storeMeta.regions[msg.Header.RegionId]; !ok {
		cb.Done(ErrRespRegionNotFound(msg.Header.RegionId))
		return
	}
	prop := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			// leader transfer 不需要进行日志复制，直接调用 RawNode.TransferLeader 即可
			log.Debugf("%v receive a transfer leader cmd", d.Tag)
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			raftResp := newCmdResp()
			raftResp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(raftResp)
			return
		case raft_cmdpb.AdminCmdType_ChangePeer:
			// 当只剩下两个节点的时候，如果移除 leader，那么在 Unreliable 的情况下可能发生 request timeout
			// 假设现在只有 leader A，follower B 两个节点，A 收到了 remove A 的指令，先对 B 进行同步，收到 resp 后 commit 这条 remove 指令
			// 这时再去同步 B 的 commitIndex，但如果 A apply 了这条 remove 指令，但用于同步 B 的 commitIndex 的 rpc 丢掉了的话
			// 就会造成 A 已经把自己 destroy 了，但是 B 没有将 A remove，这时候它来发起选举就永远选不出 leader 了
			// 碰到这种情况就直接把这次 Conf Change 拒绝，并且将 leader 转移为 另一个节点
			if msg.AdminRequest.ChangePeer.ChangeType == pb.ConfChangeType_RemoveNode &&
				len(d.Region().Peers) == 2 && d.IsLeader() && msg.AdminRequest.ChangePeer.Peer.Id == d.PeerId() {
				log.Debugf("%v there is only two nodes, can't remove a leader", d.Tag)
				for _, peer := range d.Region().Peers {
					if peer.Id != d.PeerId() {
						d.RaftGroup.TransferLeader(peer.Id)
						log.Debugf("%v transfer leader to %v", d.Tag, peer.Id)
						// 发现 ChangePeer 命令是有 callback 的，需要及时回复，否则很容易超时
						cb.Done(ErrResp(fmt.Errorf("%v there is only two nodes, can't remove a leader", d.Tag)))
						return
					}
				}
			}
			// 根据 PendingConfIndex 上的注释，只有 applyIndex > PendingConfIndex 才能 propose Conf Change
			if d.RaftGroup.Raft.PendingConfIndex < d.peerStorage.AppliedIndex() {
				err := d.RaftGroup.ProposeConfChange(pb.ConfChange{
					ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
					NodeId:     msg.AdminRequest.ChangePeer.Peer.GetId(),
					Context:    []byte(strconv.Itoa(int(msg.AdminRequest.ChangePeer.Peer.GetStoreId()))),
				})
				if err != nil {
					panic(err)
				}
				d.proposals = append(d.proposals, prop)
			}
			return
		case raft_cmdpb.AdminCmdType_Split:
			if engine_util.ExceedEndKey(msg.AdminRequest.Split.SplitKey, d.Region().EndKey) {
				return
			}
		}
	}
	// key 不在 region 中，直接返回 ErrKeyNotInRegion
	err = d.checkKeyInRegion(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	_ = d.RaftGroup.Propose(data)
	d.proposals = append(d.proposals, prop)
}

func (d *peerMsgHandler) checkKeyInRegion(request *raft_cmdpb.RaftCmdRequest) error {
	for _, req := range request.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			return util.CheckKeyInRegion(req.Get.Key, d.Region())
		case raft_cmdpb.CmdType_Put:
			return util.CheckKeyInRegion(req.Put.Key, d.Region())
		case raft_cmdpb.CmdType_Delete:
			return util.CheckKeyInRegion(req.Delete.Key, d.Region())
		}
	}
	return nil
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
