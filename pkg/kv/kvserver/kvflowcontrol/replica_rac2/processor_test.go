// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replica_rac2

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testReplica struct {
	mu       syncutil.Mutex
	raftNode *testRaftNode
	b        *strings.Builder

	leaseholder roachpb.ReplicaID
}

var _ Replica = &testReplica{}

func newTestReplica(b *strings.Builder) *testReplica {
	r := &testReplica{
		b: b,
	}
	r.raftNode = &testRaftNode{
		b: b,
		r: r,
	}
	return r
}

func (r *testReplica) RaftMuAssertHeld() {
	fmt.Fprintf(r.b, " Replica.RaftMuAssertHeld\n")
}

func (r *testReplica) MuAssertHeld() {
	fmt.Fprintf(r.b, " Replica.MuAssertHeld\n")
}

func (r *testReplica) MuLock() {
	fmt.Fprintf(r.b, " Replica.MuLock\n")
	r.mu.Lock()
}

func (r *testReplica) MuUnlock() {
	fmt.Fprintf(r.b, " Replica.MuUnlock\n")
	r.mu.Unlock()
}

func (r *testReplica) RaftNodeMuLocked() RaftNode {
	fmt.Fprintf(r.b, " Replica.RaftNodeMuLocked\n")
	return r.raftNode
}

func (r *testReplica) LeaseholderMuLocked() roachpb.ReplicaID {
	fmt.Fprintf(r.b, " Replica.LeaseholderMuLocked\n")
	r.mu.AssertHeld()
	return r.leaseholder
}

type testRaftScheduler struct {
	b *strings.Builder
}

func (rs *testRaftScheduler) EnqueueRaftReady(id roachpb.RangeID) {
	fmt.Fprintf(rs.b, " RaftScheduler.EnqueueRaftReady(rangeID=%s)\n", id)
}

type testRaftNode struct {
	b *strings.Builder
	r *testReplica

	admitted          [raftpb.NumPriorities]uint64
	leader            roachpb.ReplicaID
	stableIndex       uint64
	nextUnstableIndex uint64
	term              uint64
}

func (rn *testRaftNode) EnablePingForAdmittedLaggingLocked() {
	rn.r.mu.AssertHeld()
	fmt.Fprintf(rn.b, " RaftNode.EnablePingForAdmittedLaggingLocked\n")
}

func (rn *testRaftNode) TermLocked() uint64 {
	rn.r.mu.AssertHeld()
	fmt.Fprintf(rn.b, " RaftNode.TermLocked() = %d\n", rn.term)
	return rn.term
}

func (rn *testRaftNode) LeaderLocked() roachpb.ReplicaID {
	rn.r.mu.AssertHeld()
	fmt.Fprintf(rn.b, " RaftNode.LeaderLocked() = %s\n", rn.leader)
	return rn.leader
}

func (rn *testRaftNode) StableIndexLocked() uint64 {
	rn.r.mu.AssertHeld()
	fmt.Fprintf(rn.b, " RaftNode.StableIndexLocked() = %d\n", rn.stableIndex)
	return rn.stableIndex
}

func (rn *testRaftNode) NextUnstableIndexLocked() uint64 {
	rn.r.mu.AssertHeld()
	fmt.Fprintf(rn.b, " RaftNode.NextUnstableIndexLocked() = %d\n", rn.nextUnstableIndex)
	return rn.nextUnstableIndex
}

func (rn *testRaftNode) GetAdmittedLocked() [raftpb.NumPriorities]uint64 {
	rn.r.mu.AssertHeld()
	fmt.Fprintf(rn.b, " RaftNode.GetAdmittedLocked = %s\n", admittedString(rn.admitted))
	return rn.admitted
}

func (rn *testRaftNode) SetAdmittedLocked(admitted [raftpb.NumPriorities]uint64) raftpb.Message {
	rn.r.mu.AssertHeld()
	// TODO(sumeer): set more fields.
	msg := raftpb.Message{
		Type: raftpb.MsgAppResp,
	}
	fmt.Fprintf(rn.b, " RaftNode.SetAdmittedLocked(%s) = %s\n",
		admittedString(admitted), msgString(msg))
	rn.admitted = admitted
	return msg
}

func (rn *testRaftNode) StepMsgAppRespForAdmittedLocked(msg raftpb.Message) error {
	rn.r.mu.AssertHeld()
	fmt.Fprintf(rn.b, " RaftNode.StepMsgAppRespForAdmittedLocked(%s)\n", msgString(msg))
	return nil
}

func (rn *testRaftNode) FollowerStateRaftMuLocked(
	replicaID roachpb.ReplicaID,
) rac2.FollowerStateInfo {
	rn.r.mu.AssertHeld()
	fmt.Fprintf(rn.b, " RaftNode.FollowerStateRaftMuLocked(%v)\n", replicaID)
	// TODO(kvoli,sumeerbhola): implement.
	return rac2.FollowerStateInfo{}
}

func admittedString(admitted [raftpb.NumPriorities]uint64) string {
	return fmt.Sprintf("[%d, %d, %d, %d]", admitted[0], admitted[1], admitted[2], admitted[3])
}

func msgString(msg raftpb.Message) string {
	return fmt.Sprintf("type: %s from: %d to: %d", msg.Type.String(), msg.From, msg.To)
}

type testAdmittedPiggybacker struct {
	b *strings.Builder
}

func (p *testAdmittedPiggybacker) Add(
	n roachpb.NodeID, m kvflowcontrolpb.PiggybackedAdmittedState,
) {
	fmt.Fprintf(p.b, " Piggybacker.Add(n%s, %s)\n", n, m)
}

type testACWorkQueue struct {
	b *strings.Builder
	// TODO(sumeer): test case that sets this to true.
	returnFalse bool
}

func (q *testACWorkQueue) Admit(ctx context.Context, entry EntryForAdmission) bool {
	fmt.Fprintf(q.b, " ACWorkQueue.Admit(%+v) = %t\n", entry, !q.returnFalse)
	return !q.returnFalse
}

type testRangeControllerFactory struct {
	b   *strings.Builder
	rcs []*testRangeController
}

func (f *testRangeControllerFactory) New(
	ctx context.Context, state rangeControllerInitState,
) rac2.RangeController {
	fmt.Fprintf(f.b, " RangeControllerFactory.New(replicaSet=%s, leaseholder=%s, nextRaftIndex=%d)\n",
		state.replicaSet, state.leaseholder, state.nextRaftIndex)
	rc := &testRangeController{b: f.b, waited: true}
	f.rcs = append(f.rcs, rc)
	return rc
}

type testRangeController struct {
	b              *strings.Builder
	waited         bool
	waitForEvalErr error
}

func (c *testRangeController) WaitForEval(
	ctx context.Context, pri admissionpb.WorkPriority,
) (bool, error) {
	errStr := "<nil>"
	if c.waitForEvalErr != nil {
		errStr = c.waitForEvalErr.Error()
	}
	fmt.Fprintf(c.b, " RangeController.WaitForEval(pri=%s) = (waited=%t err=%s)\n",
		pri.String(), c.waited, errStr)
	return c.waited, c.waitForEvalErr
}

func raftEventString(e rac2.RaftEvent) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[")
	for i := range e.Entries {
		prefix := " "
		if i == 0 {
			prefix = ""
		}
		fmt.Fprintf(&b, "%s%d", prefix, e.Entries[i].Index)
	}
	fmt.Fprintf(&b, "]")
	return b.String()
}

func (c *testRangeController) HandleRaftEventRaftMuLocked(
	ctx context.Context, e rac2.RaftEvent,
) error {
	fmt.Fprintf(c.b, " RangeController.HandleRaftEventRaftMuLocked(%s)\n", raftEventString(e))
	return nil
}

func (c *testRangeController) HandleSchedulerEventRaftMuLocked(ctx context.Context) error {
	panic("HandleSchedulerEventRaftMuLocked should not be called when no send-queues")
}

func (c *testRangeController) SetReplicasRaftMuLocked(
	ctx context.Context, replicas rac2.ReplicaSet,
) error {
	fmt.Fprintf(c.b, " RangeController.SetReplicasRaftMuLocked(%s)\n", replicas)
	return nil
}

func (c *testRangeController) SetLeaseholderRaftMuLocked(
	ctx context.Context, replica roachpb.ReplicaID,
) {
	fmt.Fprintf(c.b, " RangeController.SetLeaseholderRaftMuLocked(%s)\n", replica)
}

func (c *testRangeController) CloseRaftMuLocked(ctx context.Context) {
	fmt.Fprintf(c.b, " RangeController.CloseRaftMuLocked\n")
}

func TestProcessorBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var b strings.Builder
	var r *testReplica
	var sched testRaftScheduler
	var piggybacker testAdmittedPiggybacker
	var q testACWorkQueue
	var rcFactory testRangeControllerFactory
	var st *cluster.Settings
	var p *processorImpl
	tenantID := roachpb.MustMakeTenantID(4)
	reset := func(enabled EnabledWhenLeaderLevel) {
		b.Reset()
		r = newTestReplica(&b)
		sched = testRaftScheduler{b: &b}
		piggybacker = testAdmittedPiggybacker{b: &b}
		q = testACWorkQueue{b: &b}
		rcFactory = testRangeControllerFactory{b: &b}
		st = cluster.MakeTestingClusterSettings()
		kvflowcontrol.Mode.Override(ctx, &st.SV, kvflowcontrol.ApplyToElastic)
		p = NewProcessor(ProcessorOptions{
			NodeID:                 1,
			StoreID:                2,
			RangeID:                3,
			ReplicaID:              5,
			Replica:                r,
			RaftScheduler:          &sched,
			AdmittedPiggybacker:    &piggybacker,
			ACWorkQueue:            &q,
			RangeControllerFactory: &rcFactory,
			Settings:               st,
			EnabledWhenLeaderLevel: enabled,
			EvalWaitMetrics:        rac2.NewEvalWaitMetrics(),
		}).(*processorImpl)
		fmt.Fprintf(&b, "n%s,s%s,r%s: replica=%s, tenant=%s, enabled-level=%s\n",
			p.opts.NodeID, p.opts.StoreID, p.opts.RangeID, p.opts.ReplicaID, tenantID,
			enabledLevelString(p.mu.enabledWhenLeader))
	}
	builderStr := func() string {
		str := b.String()
		b.Reset()
		return str
	}
	printRaftState := func() {
		fmt.Fprintf(&b, "Raft: leader: %d leaseholder: %d stable: %d next-unstable: %d term: %d admitted: %s",
			r.raftNode.leader, r.leaseholder, r.raftNode.stableIndex, r.raftNode.nextUnstableIndex,
			r.raftNode.term, admittedString(r.raftNode.admitted))
	}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "processor"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "reset":
				enabledLevel := parseEnabledLevel(t, d)
				reset(enabledLevel)
				return builderStr()

			case "set-raft-state":
				if d.HasArg("admitted") {
					var arg string
					d.ScanArgs(t, "admitted", &arg)
					admitted := parseAdmitted(t, arg)
					r.raftNode.admitted = admitted
				}
				if d.HasArg("leader") {
					var leaderID int
					d.ScanArgs(t, "leader", &leaderID)
					r.raftNode.leader = roachpb.ReplicaID(leaderID)
				}
				if d.HasArg("stable-index") {
					var stableIndex uint64
					d.ScanArgs(t, "stable-index", &stableIndex)
					r.raftNode.stableIndex = stableIndex
				}
				if d.HasArg("next-unstable-index") {
					var nextUnstableIndex uint64
					d.ScanArgs(t, "next-unstable-index", &nextUnstableIndex)
					r.raftNode.nextUnstableIndex = nextUnstableIndex
				}
				if d.HasArg("my-leader-term") {
					var myLeaderTerm uint64
					d.ScanArgs(t, "my-leader-term", &myLeaderTerm)
					r.raftNode.term = myLeaderTerm
				}
				if d.HasArg("leaseholder") {
					var leaseholder int
					d.ScanArgs(t, "leaseholder", &leaseholder)
					r.leaseholder = roachpb.ReplicaID(leaseholder)
				}
				printRaftState()
				return builderStr()

			case "on-destroy":
				p.OnDestroyRaftMuLocked(ctx)
				return builderStr()

			case "set-enabled-level":
				enabledLevel := parseEnabledLevel(t, d)
				p.SetEnabledWhenLeaderRaftMuLocked(ctx, enabledLevel)
				return builderStr()

			case "get-enabled-level":
				enabledLevel := p.GetEnabledWhenLeader()
				fmt.Fprintf(&b, "enabled-level: %s\n", enabledLevelString(enabledLevel))
				return builderStr()

			case "on-desc-changed":
				desc := parseRangeDescriptor(t, d)
				p.OnDescChangedLocked(ctx, &desc, tenantID)
				return builderStr()

			case "handle-raft-ready-and-admit":
				var event rac2.RaftEvent
				if d.HasArg("entries") {
					var arg string
					d.ScanArgs(t, "entries", &arg)
					event.Entries = createEntries(t, parseEntryInfos(t, arg))
				}
				if len(event.Entries) > 0 {
					d.ScanArgs(t, "leader-term", &event.Term)
				}
				fmt.Fprintf(&b, "HandleRaftReady:\n")
				p.HandleRaftReadyRaftMuLocked(ctx, event)
				fmt.Fprintf(&b, ".....\n")
				if len(event.Entries) > 0 {
					fmt.Fprintf(&b, "AdmitRaftEntries:\n")
					destroyedOrV2 := p.AdmitRaftEntriesRaftMuLocked(ctx, event)
					fmt.Fprintf(&b, "destroyed-or-leader-using-v2: %t\n", destroyedOrV2)
				}
				return builderStr()

			case "enqueue-piggybacked-admitted":
				var from, to uint64
				d.ScanArgs(t, "from", &from)
				d.ScanArgs(t, "to", &to)
				msg := raftpb.Message{
					Type: raftpb.MsgAppResp,
					To:   raftpb.PeerID(to),
					From: raftpb.PeerID(from),
				}
				p.EnqueuePiggybackedAdmittedAtLeader(msg)
				return builderStr()

			case "process-piggybacked-admitted":
				p.ProcessPiggybackedAdmittedAtLeaderRaftMuLocked(ctx)
				return builderStr()

			case "side-channel":
				var usingV2 bool
				if d.HasArg("v2") {
					usingV2 = true
				}
				var leaderTerm uint64
				d.ScanArgs(t, "leader-term", &leaderTerm)
				var first, last uint64
				d.ScanArgs(t, "first", &first)
				d.ScanArgs(t, "last", &last)
				var lowPriOverride bool
				if d.HasArg("low-pri") {
					lowPriOverride = true
				}
				info := SideChannelInfoUsingRaftMessageRequest{
					UsingV2Protocol: usingV2,
					LeaderTerm:      leaderTerm,
					First:           first,
					Last:            last,
					LowPriOverride:  lowPriOverride,
				}
				p.SideChannelForPriorityOverrideAtFollowerRaftMuLocked(info)
				return builderStr()

			case "admitted-log-entry":
				var cb EntryForAdmissionCallbackState
				d.ScanArgs(t, "leader-term", &cb.Mark.Term)
				d.ScanArgs(t, "index", &cb.Mark.Index)
				var pri int
				d.ScanArgs(t, "pri", &pri)
				cb.Priority = raftpb.Priority(pri)
				p.AdmittedLogEntry(ctx, cb)
				return builderStr()

			case "set-flow-control-mode":
				var mode string
				d.ScanArgs(t, "mode", &mode)
				var modeVal kvflowcontrol.ModeT
				switch mode {
				case "apply-to-all":
					modeVal = kvflowcontrol.ApplyToAll
				case "apply-to-elastic":
					modeVal = kvflowcontrol.ApplyToElastic
				default:
					t.Fatalf("unknown mode: %s", mode)
				}
				kvflowcontrol.Mode.Override(ctx, &st.SV, modeVal)
				return builderStr()

			case "admit-for-eval":
				pri := parseAdmissionPriority(t, d)
				// The callee ignores the create time.
				admitted, err := p.AdmitForEval(ctx, pri, time.Time{})
				fmt.Fprintf(&b, "admitted: %t err: ", admitted)
				if err == nil {
					fmt.Fprintf(&b, "<nil>\n")
				} else {
					fmt.Fprintf(&b, "%s\n", err.Error())
				}
				return builderStr()

			case "set-wait-for-eval-return-values":
				rc := rcFactory.rcs[len(rcFactory.rcs)-1]
				d.ScanArgs(t, "waited", &rc.waited)
				rc.waitForEvalErr = nil
				if d.HasArg("err") {
					var errStr string
					d.ScanArgs(t, "err", &errStr)
					rc.waitForEvalErr = errors.Errorf("%s", errStr)
				}
				return builderStr()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func parseAdmissionPriority(t *testing.T, td *datadriven.TestData) admissionpb.WorkPriority {
	var priStr string
	td.ScanArgs(t, "pri", &priStr)
	for k, v := range admissionpb.WorkPriorityDict {
		if v == priStr {
			return k
		}
	}
	t.Fatalf("unknown priority %s", priStr)
	return admissionpb.NormalPri
}

func parseEnabledLevel(t *testing.T, td *datadriven.TestData) EnabledWhenLeaderLevel {
	if td.HasArg("enabled-level") {
		var str string
		td.ScanArgs(t, "enabled-level", &str)
		switch str {
		case "not-enabled":
			return NotEnabledWhenLeader
		case "v1-encoding":
			return EnabledWhenLeaderV1Encoding
		case "v2-encoding":
			return EnabledWhenLeaderV2Encoding
		default:
			t.Fatalf("unrecoginized level %s", str)
		}
	}
	return NotEnabledWhenLeader
}

func enabledLevelString(enabledLevel EnabledWhenLeaderLevel) string {
	switch enabledLevel {
	case NotEnabledWhenLeader:
		return "not-enabled"
	case EnabledWhenLeaderV1Encoding:
		return "v1-encoding"
	case EnabledWhenLeaderV2Encoding:
		return "v2-encoding"
	}
	return "unknown-level"
}

func parseAdmitted(t *testing.T, arg string) [raftpb.NumPriorities]uint64 {
	n := len(arg)
	require.LessOrEqual(t, 2, n)
	require.Equal(t, uint8('['), arg[0])
	require.Equal(t, uint8(']'), arg[n-1])
	parts := strings.Split(arg[1:n-1], ",")
	require.Equal(t, 4, len(parts))
	var admitted [raftpb.NumPriorities]uint64
	for i, part := range parts {
		val, err := strconv.Atoi(strings.TrimSpace(part))
		require.NoError(t, err)
		admitted[i] = uint64(val)
	}
	return admitted
}

func parseRangeDescriptor(t *testing.T, td *datadriven.TestData) roachpb.RangeDescriptor {
	var replicaStr string
	td.ScanArgs(t, "replicas", &replicaStr)
	parts := strings.Split(replicaStr, ",")
	var desc roachpb.RangeDescriptor
	for _, part := range parts {
		replica := parseReplicaDescriptor(t, strings.TrimSpace(part))
		desc.InternalReplicas = append(desc.InternalReplicas, replica)
	}
	return desc
}

// n<int>/s<int>/<int>{/<type>}
// Where type is {voter_full, non_voter}.
func parseReplicaDescriptor(t *testing.T, arg string) roachpb.ReplicaDescriptor {
	parts := strings.Split(arg, "/")
	require.LessOrEqual(t, 3, len(parts))
	require.GreaterOrEqual(t, 4, len(parts))
	ni, err := strconv.Atoi(strings.TrimPrefix(parts[0], "n"))
	require.NoError(t, err)
	store, err := strconv.Atoi(strings.TrimPrefix(parts[1], "s"))
	require.NoError(t, err)
	repl, err := strconv.Atoi(parts[2])
	require.NoError(t, err)
	typ := roachpb.VOTER_FULL
	if len(parts) == 4 {
		switch parts[3] {
		case "voter_full":
		case "non_voter":
			typ = roachpb.NON_VOTER
		default:
			t.Fatalf("unknown replica type %s", parts[3])
		}
	}
	var desc roachpb.ReplicaDescriptor
	desc.NodeID = roachpb.NodeID(ni)
	desc.StoreID = roachpb.StoreID(store)
	desc.ReplicaID = roachpb.ReplicaID(repl)
	desc.Type = typ
	return desc
}

type entryInfo struct {
	encoding   raftlog.EntryEncoding
	index      uint64
	term       uint64
	pri        raftpb.Priority
	createTime int64
	length     int
}

func createEntry(t *testing.T, info entryInfo) raftpb.Entry {
	cmdID := kvserverbase.CmdIDKey("11111111")
	var metaBuf []byte
	if info.encoding.UsesAdmissionControl() {
		meta := kvflowcontrolpb.RaftAdmissionMeta{
			AdmissionCreateTime: info.createTime,
		}
		isV2Encoding := info.encoding == raftlog.EntryEncodingStandardWithACAndPriority ||
			info.encoding == raftlog.EntryEncodingSideloadedWithACAndPriority
		if isV2Encoding {
			meta.AdmissionPriority = int32(info.pri)
		} else {
			meta.AdmissionOriginNode = 10
			require.Equal(t, raftpb.LowPri, info.pri)
			meta.AdmissionPriority = int32(raftpb.LowPri)
		}
		var err error
		metaBuf, err = protoutil.Marshal(&meta)
		require.NoError(t, err)
	}
	cmdBufPrefix := raftlog.EncodeCommandBytes(info.encoding, cmdID, nil, info.pri)
	paddingLen := info.length - len(cmdBufPrefix) - len(metaBuf)
	// Padding also needs to decode as part of the RaftCommand proto, so we
	// abuse the WriteBatch.Data field which is a byte slice. Since it is a
	// nested field it consumes two tags plus two lengths. We'll approximate
	// this as needing a maximum of 15 bytes, to be on the safe side.
	require.LessOrEqual(t, 15, paddingLen)
	cmd := kvserverpb.RaftCommand{
		WriteBatch: &kvserverpb.WriteBatch{Data: make([]byte, paddingLen)}}
	// Shrink by 1 on each iteration. This doesn't give us a guarantee that we
	// will get exactly paddingLen since the length of data affects the encoded
	// lengths, but it should usually work, and cause fewer questions when
	// looking at the testdata file.
	for cmd.Size() > paddingLen {
		cmd.WriteBatch.Data = cmd.WriteBatch.Data[:len(cmd.WriteBatch.Data)-1]
	}
	cmdBuf, err := protoutil.Marshal(&cmd)
	require.NoError(t, err)
	data := append(cmdBufPrefix, metaBuf...)
	data = append(data, cmdBuf...)
	return raftpb.Entry{
		Term:  info.term,
		Index: info.index,
		Type:  raftpb.EntryNormal,
		Data:  data,
	}
}

func createEntries(t *testing.T, infos []entryInfo) []raftpb.Entry {
	var entries []raftpb.Entry
	for _, info := range infos {
		entries = append(entries, createEntry(t, info))
	}
	return entries
}

// encoding, index, priority, create-time, length.
// <enc>/i<int>/t<int>/pri<int>/time<int>/len<int>
func parseEntryInfos(t *testing.T, arg string) []entryInfo {
	parts := strings.Split(arg, ",")
	var entries []entryInfo
	for _, part := range parts {
		entries = append(entries, parseEntryInfo(t, strings.TrimSpace(part)))
	}
	return entries
}

func parseEntryInfo(t *testing.T, arg string) entryInfo {
	parts := strings.Split(arg, "/")
	require.Equal(t, 6, len(parts))
	encoding := parseEntryEncoding(t, strings.TrimSpace(parts[0]))
	index, err := strconv.Atoi(strings.TrimPrefix(parts[1], "i"))
	require.NoError(t, err)
	term, err := strconv.Atoi(strings.TrimPrefix(parts[2], "t"))
	require.NoError(t, err)
	pri, err := strconv.Atoi(strings.TrimPrefix(parts[3], "pri"))
	require.NoError(t, err)
	createTime, err := strconv.Atoi(strings.TrimPrefix(parts[4], "time"))
	require.NoError(t, err)
	length, err := strconv.Atoi(strings.TrimPrefix(parts[5], "len"))
	require.NoError(t, err)
	return entryInfo{
		encoding:   encoding,
		index:      uint64(index),
		term:       uint64(term),
		pri:        raftpb.Priority(pri),
		createTime: int64(createTime),
		length:     length,
	}
}

func parseEntryEncoding(t *testing.T, arg string) raftlog.EntryEncoding {
	switch arg {
	case "v1":
		return raftlog.EntryEncodingStandardWithAC
	case "v2":
		return raftlog.EntryEncodingStandardWithACAndPriority
	case "v1-side":
		return raftlog.EntryEncodingSideloadedWithAC
	case "v2-side":
		return raftlog.EntryEncodingSideloadedWithACAndPriority
	case "none":
		return raftlog.EntryEncodingStandardWithoutAC
	default:
		t.Fatalf("unrecognized encoding string %s", arg)
	}
	return raftlog.EntryEncodingEmpty
}
