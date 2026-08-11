package checkpointer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	checkpointerpb "github.com/duongnghia222/langsmith-deployment-go/gen/checkpointer"
	engine_common "github.com/duongnghia222/langsmith-deployment-go/gen/engine_common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// writesIdxMap mirrors WRITES_IDX_MAP from langgraph.checkpoint.base.
// These sentinel channels always use fixed negative indices so that
// concurrent tasks never collide on them (stored as DO NOTHING on conflict).
// Keep verbatim: WRITES_IDX_MAP = {"__error__": -1, "__scheduled__": -2, "__interrupt__": -3, "__resume__": -4}
var writesIdxMap = map[string]int32{
	"__error__":     -1,
	"__scheduled__": -2,
	"__interrupt__": -3,
	"__resume__":    -4,
}

// tasksChannel is the channel name used to carry pending sends.
// Keep verbatim: TASKS = "__pregel_tasks"
const tasksChannel = "__pregel_tasks"

// channelValueProtoEncoding marks a checkpoint_writes blob that stores a
// proto-marshaled ChannelValue (used when the value oneof is Sends or Missing,
// which cannot be represented by a plain SerializedValue encoding+bytes pair).
const channelValueProtoEncoding = "__lsd_channel_value_pb__"

// Service implements checkpointerpb.CheckpointerServer.
type Service struct {
	checkpointerpb.UnimplementedCheckpointerServer
	store *Store
}

// NewService constructs a Service backed by the given Store.
func NewService(store *Store) *Service { return &Service{store: store} }

func (s *Service) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*checkpointerpb.Capabilities, error) {
	return &checkpointerpb.Capabilities{
		SupportsDeleteThread:  true,
		SupportsPrune:         true,
		SupportsDeleteForRuns: true,
		SupportsCopyThread:    true,
	}, nil
}

func (s *Service) Put(ctx context.Context, req *checkpointerpb.PutRequest) (*checkpointerpb.PutResponse, error) {
	cfg := req.GetConfig()
	threadID := cfg.GetThreadId()
	checkpointNS := cfg.GetCheckpointNs()
	cp := req.GetCheckpoint()

	checkpointID := cp.GetId()
	checkpointJSON, err := marshalCheckpoint(cp)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal checkpoint: %v", err)
	}
	metadataJSON, err := marshalMetadata(req.GetMetadata())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal metadata: %v", err)
	}

	var blobs []BlobInput
	for channel, cv := range cp.GetChannelValues() {
		if sv := cv.GetSerializedValue(); sv != nil {
			blobs = append(blobs, BlobInput{
				Channel:  channel,
				Version:  req.GetNewVersions()[channel],
				Encoding: sv.GetEncoding(),
				Blob:     sv.GetValue(),
			})
		}
	}

	// C4: parent_checkpoint_id is the incoming config's checkpoint_id
	// (Python aput: checkpoint_id = configurable.pop("checkpoint_id", None))
	parentCheckpointID := cfg.GetCheckpointId()

	// C5: run_id from config or metadata
	// Python aput: run_id = configurable.pop("run_id", None)
	runID := cfg.GetRunId()
	if runID == "" {
		runID = req.GetMetadata().GetRunId()
	}

	in := PutInput{
		ThreadID:           threadID,
		CheckpointNS:       checkpointNS,
		CheckpointID:       checkpointID,
		ParentCheckpointID: parentCheckpointID,
		RunID:              runID,
		CheckpointJSON:     checkpointJSON,
		MetadataJSON:       metadataJSON,
		Blobs:              blobs,
	}
	if err := s.store.Put(ctx, in); err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}

	nextCfg := cloneConfig(cfg)
	nextCfg.CheckpointId = &checkpointID
	return &checkpointerpb.PutResponse{NextConfig: nextCfg}, nil
}

func (s *Service) PutWrites(ctx context.Context, req *checkpointerpb.PutWritesRequest) (*emptypb.Empty, error) {
	cfg := req.GetConfig()
	in := PutWritesInput{
		ThreadID:     cfg.GetThreadId(),
		CheckpointNS: cfg.GetCheckpointNs(),
		CheckpointID: cfg.GetCheckpointId(),
		TaskID:       req.GetTaskId(),
		TaskPath:     req.GetTaskPath(),
	}
	// C6: apply WRITES_IDX_MAP sentinel indices, mirroring Python's _dump_writes:
	//   WRITES_IDX_MAP.get(channel, idx)
	for i, w := range req.GetWrites() {
		encoding, blob, err := encodeWriteValue(w.GetValue())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode write idx=%d: %v", i, err)
		}
		idx := int32(i)
		if sentinel, ok := writesIdxMap[w.GetChannel()]; ok {
			idx = sentinel
		}
		in.Writes = append(in.Writes, WriteInput{
			Idx:      idx,
			Channel:  w.GetChannel(),
			Encoding: encoding,
			Blob:     blob,
		})
	}
	if err := s.store.PutWrites(ctx, in); err != nil {
		return nil, status.Errorf(codes.Internal, "put_writes: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) List(ctx context.Context, req *checkpointerpb.ListRequest) (*checkpointerpb.ListResponse, error) {
	cfg := req.GetConfig()
	threadID := cfg.GetThreadId()
	checkpointNS := cfg.GetCheckpointNs()

	var before string
	if bc := req.GetBefore(); bc != nil {
		before = bc.GetCheckpointId()
	}

	// C7: pass filter_json to store.List so Python's "metadata @> $n::jsonb" clause fires
	filterJSON := req.GetFilterJson()

	rows, err := s.store.List(ctx, threadID, checkpointNS, before, req.Limit, filterJSON)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}

	resp := &checkpointerpb.ListResponse{}
	for _, row := range rows {
		// C8: load blobs+writes per row via the full rowToTuple path
		tuple, err := rowToTuple(row)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "row to tuple: %v", err)
		}
		resp.CheckpointTuples = append(resp.CheckpointTuples, tuple)
	}
	return resp, nil
}

func (s *Service) GetTuple(ctx context.Context, req *checkpointerpb.GetTupleRequest) (*checkpointerpb.GetTupleResponse, error) {
	cfg := req.GetConfig()
	threadID := cfg.GetThreadId()
	checkpointNS := cfg.GetCheckpointNs()
	checkpointID := cfg.GetCheckpointId()

	row, err := s.store.GetTuple(ctx, threadID, checkpointNS, checkpointID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get_tuple: %v", err)
	}
	if row == nil {
		return &checkpointerpb.GetTupleResponse{}, nil
	}
	tuple, err := rowToTuple(row)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "row to tuple: %v", err)
	}
	return &checkpointerpb.GetTupleResponse{CheckpointTuple: tuple}, nil
}

func (s *Service) DeleteThread(ctx context.Context, req *checkpointerpb.DeleteThreadRequest) (*emptypb.Empty, error) {
	if err := s.store.DeleteThread(ctx, req.GetThreadId()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete_thread: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) DeleteForRuns(ctx context.Context, req *checkpointerpb.DeleteForRunsRequest) (*emptypb.Empty, error) {
	if err := s.store.DeleteForRuns(ctx, req.GetRunIds()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete_for_runs: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) CopyThread(ctx context.Context, req *checkpointerpb.CopyThreadRequest) (*emptypb.Empty, error) {
	if err := s.store.CopyThread(ctx, req.GetFromThreadId(), req.GetToThreadId()); err != nil {
		return nil, status.Errorf(codes.Internal, "copy_thread: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) Prune(ctx context.Context, req *checkpointerpb.PruneRequest) (*emptypb.Empty, error) {
	if err := s.store.Prune(ctx, req.GetThreadIds(), int32(req.GetStrategy())); err != nil {
		return nil, status.Errorf(codes.Internal, "prune: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// marshalCheckpoint converts a proto Checkpoint to the JSONB stored in the
// checkpoints.checkpoint column.  Python stores the full checkpoint dict
// (minus channel_values, which go to checkpoint_blobs) keyed exactly as:
//
//	v, id, ts, channel_versions, versions_seen, updated_channels
//
// channel_versions is load-bearing: Python SELECT_SQL joins blobs via
//
//	jsonb_each_text(checkpoint -> 'channel_versions')
func marshalCheckpoint(cp *engine_common.Checkpoint) ([]byte, error) {
	if cp == nil {
		return []byte("{}"), nil
	}
	// channel_versions: map[string]string on proto; stored as-is.
	channelVersions := map[string]any{}
	for k, v := range cp.GetChannelVersions() {
		channelVersions[k] = v
	}
	// versions_seen: map[string]*ChannelVersions; store as map[string]map[string]string.
	versionsSeen := map[string]any{}
	for node, cv := range cp.GetVersionsSeen() {
		inner := map[string]string{}
		for k, v := range cv.GetChannelVersions() {
			inner[k] = v
		}
		versionsSeen[node] = inner
	}
	m := map[string]any{
		"v":                cp.GetV(),
		"id":               cp.GetId(),
		"ts":               cp.GetTs(),
		"channel_versions": channelVersions,
		"versions_seen":    versionsSeen,
		"updated_channels": cp.GetUpdatedChannels(),
	}
	return json.Marshal(m)
}

// sourceIntToString maps CheckpointMetadata_CheckpointSource enum values to
// the string representation Python stores in metadata JSON.
// Mirror SOURCE_MAP in api/langgraph_grpc_common/conversion/checkpoint.py.
var sourceIntToString = map[engine_common.CheckpointMetadata_CheckpointSource]string{
	engine_common.CheckpointMetadata_input:  "input",
	engine_common.CheckpointMetadata_loop:   "loop",
	engine_common.CheckpointMetadata_update: "update",
	engine_common.CheckpointMetadata_fork:   "fork",
	// CheckpointMetadata_unknown → "" (omitted below)
}

// sourceStringToInt is the inverse of sourceIntToString; used when reading metadata back.
var sourceStringToInt = map[string]engine_common.CheckpointMetadata_CheckpointSource{
	"input":  engine_common.CheckpointMetadata_input,
	"loop":   engine_common.CheckpointMetadata_loop,
	"update": engine_common.CheckpointMetadata_update,
	"fork":   engine_common.CheckpointMetadata_fork,
}

func marshalMetadata(md *engine_common.CheckpointMetadata) ([]byte, error) {
	if md == nil {
		return []byte("{}"), nil
	}
	m := map[string]any{
		"step":    md.GetStep(),
		"parents": md.GetParents(),
	}
	// source: store as string to match Python's CheckpointMetadata dict convention
	if src, ok := sourceIntToString[md.GetSource()]; ok {
		m["source"] = src
	}
	// run_id: store if present
	if runID := md.GetRunId(); runID != "" {
		m["run_id"] = runID
	}
	// extras: orjson-serialized map[string]bytes → decode each value as JSON before storing
	for k, v := range md.GetExtras() {
		var decoded any
		if err := json.Unmarshal(v, &decoded); err == nil {
			m[k] = decoded
		} else {
			// fall back to raw bytes as base64 (should not occur in practice)
			m[k] = v
		}
	}
	return json.Marshal(m)
}

func cloneConfig(cfg *engine_common.EngineRunnableConfig) *engine_common.EngineRunnableConfig {
	if cfg == nil {
		return &engine_common.EngineRunnableConfig{}
	}
	threadID := cfg.GetThreadId()
	ns := cfg.GetCheckpointNs()
	return &engine_common.EngineRunnableConfig{
		ThreadId:     &threadID,
		CheckpointNs: &ns,
	}
}

func rowToTuple(row *CheckpointRow) (*engine_common.CheckpointTuple, error) {
	if row == nil {
		return nil, nil
	}
	threadID := row.ThreadID
	ns := row.CheckpointNS
	cid := row.CheckpointID

	cfg := &engine_common.EngineRunnableConfig{
		ThreadId:     &threadID,
		CheckpointNs: &ns,
		CheckpointId: &cid,
	}

	// C1: decode checkpoint JSON and populate proto Checkpoint fields
	var cpMap map[string]any
	if err := json.NewDecoder(bytes.NewReader(row.CheckpointJSON)).Decode(&cpMap); err != nil {
		return nil, fmt.Errorf("decode checkpoint json: %w", err)
	}

	cp := &engine_common.Checkpoint{
		Id: cid,
	}
	if v, ok := cpMap["v"]; ok {
		switch n := v.(type) {
		case float64:
			cp.V = uint64(n)
		}
	}
	if ts, ok := cpMap["ts"]; ok {
		if s, ok := ts.(string); ok {
			cp.Ts = s
		}
	}

	// channel_versions: load-bearing for blob filtering below
	storedChannelVersions := map[string]string{}
	if cv, ok := cpMap["channel_versions"]; ok {
		if cvMap, ok := cv.(map[string]any); ok {
			cp.ChannelVersions = make(map[string]string, len(cvMap))
			for k, val := range cvMap {
				if s, ok := val.(string); ok {
					storedChannelVersions[k] = s
					cp.ChannelVersions[k] = s
				}
			}
		}
	}

	// versions_seen
	if vs, ok := cpMap["versions_seen"]; ok {
		if vsMap, ok := vs.(map[string]any); ok {
			cp.VersionsSeen = make(map[string]*engine_common.ChannelVersions, len(vsMap))
			for node, inner := range vsMap {
				if innerMap, ok := inner.(map[string]any); ok {
					chVer := &engine_common.ChannelVersions{
						ChannelVersions: make(map[string]string, len(innerMap)),
					}
					for k, v := range innerMap {
						if s, ok := v.(string); ok {
							chVer.ChannelVersions[k] = s
						}
					}
					cp.VersionsSeen[node] = chVer
				}
			}
		}
	}

	// updated_channels
	if uc, ok := cpMap["updated_channels"]; ok {
		if ucSlice, ok := uc.([]any); ok {
			for _, item := range ucSlice {
				if s, ok := item.(string); ok {
					cp.UpdatedChannels = append(cp.UpdatedChannels, s)
				}
			}
		}
	}

	// C1: reconstruct channel_values from blobs, mirroring Python _load_blobs:
	// only channels listed in channel_versions (matching version) belong to this
	// checkpoint; skip blobs with type 'empty'.
	cp.ChannelValues = make(map[string]*engine_common.ChannelValue)
	for _, b := range row.Blobs {
		// only include blobs whose channel+version matches this checkpoint's channel_versions
		expectedVersion, inCheckpoint := storedChannelVersions[b.Channel]
		if !inCheckpoint || b.Version != expectedVersion {
			continue
		}
		// skip 'empty' blobs (Python _load_blobs: if t.decode() != "empty")
		if b.Encoding == "empty" {
			continue
		}
		cv, err := decodeWriteValue(b.Encoding, b.Blob)
		if err != nil {
			return nil, fmt.Errorf("decode blob channel=%s: %w", b.Channel, err)
		}
		cp.ChannelValues[b.Channel] = cv
	}

	// C9: pending_sends — merge parent checkpoint's __pregel_tasks writes into
	// channel_values[tasksChannel], mirroring Python _load_checkpoint:
	//   checkpoint["pending_sends"] = [serde.loads_typed((c, b)) for c, b in pending_sends or []]
	// Since the proto Checkpoint has no pending_sends field, we merge them into
	// channel_values under tasksChannel as a Sends value, matching how Python
	// _load_checkpoint injects them into channel_values for the graph engine.
	// Ordering: ORDER BY task_id, idx (preserved by the SQL query in store).
	if len(row.PendingSends) > 0 {
		// Each PendingSend is a (type, blob) pair; decode as ChannelValue
		// and collect into a Sends oneof, then store under tasksChannel.
		var sends []*engine_common.Send
		for _, ps := range row.PendingSends {
			cv, err := decodeWriteValue(ps.Encoding, ps.Blob)
			if err != nil {
				return nil, fmt.Errorf("decode pending_send: %w", err)
			}
			// A pending send blob may itself be a Sends oneof (proto-encoded) or a
			// SerializedValue. In either case, wrap it as the existing channel value.
			if psends := cv.GetSends(); psends != nil {
				sends = append(sends, psends.GetSends()...)
			} else if sv := cv.GetSerializedValue(); sv != nil {
				// Encode as a synthetic Send with no node (raw serialized value)
				sends = append(sends, &engine_common.Send{
					Node: "",
					Arg:  sv,
				})
			}
		}
		if len(sends) > 0 {
			cp.ChannelValues[tasksChannel] = &engine_common.ChannelValue{
				Val: &engine_common.ChannelValue_Sends{
					Sends: &engine_common.Sends{Sends: sends},
				},
			}
		}
	}

	// C3+C4: decode metadata JSON and populate proto CheckpointMetadata
	var mdMap map[string]any
	if err := json.NewDecoder(bytes.NewReader(row.MetadataJSON)).Decode(&mdMap); err != nil {
		return nil, fmt.Errorf("decode metadata json: %w", err)
	}
	md := &engine_common.CheckpointMetadata{}

	// source: stored as string in JSON; map back to proto enum
	if src, ok := mdMap["source"]; ok {
		if srcStr, ok := src.(string); ok {
			if srcEnum, ok := sourceStringToInt[srcStr]; ok {
				md.Source = srcEnum
			}
		}
		// numeric source (legacy) — stored as float64 by JSON decoder
		if srcNum, ok := src.(float64); ok {
			md.Source = engine_common.CheckpointMetadata_CheckpointSource(int32(srcNum))
		}
	}
	if step, ok := mdMap["step"]; ok {
		if n, ok := step.(float64); ok {
			md.Step = int32(n)
		}
	}
	if parents, ok := mdMap["parents"]; ok {
		if parMap, ok := parents.(map[string]any); ok {
			md.Parents = make(map[string]string, len(parMap))
			for k, v := range parMap {
				if s, ok := v.(string); ok {
					md.Parents[k] = s
				}
			}
		}
	}
	if runID, ok := mdMap["run_id"]; ok {
		if s, ok := runID.(string); ok && s != "" {
			md.RunId = &s
		}
	}
	// extras: re-encode any non-standard metadata keys as JSON bytes
	knownKeys := map[string]bool{"source": true, "step": true, "parents": true, "run_id": true}
	for k, v := range mdMap {
		if knownKeys[k] {
			continue
		}
		if encoded, err := json.Marshal(v); err == nil {
			if md.Extras == nil {
				md.Extras = make(map[string][]byte)
			}
			md.Extras[k] = encoded
		}
	}

	var pendingWrites []*engine_common.PendingWrite
	for _, w := range row.Writes {
		taskID := w.TaskID
		cv, err := decodeWriteValue(w.Encoding, w.Blob)
		if err != nil {
			return nil, fmt.Errorf("decode write task=%s idx=%d: %w", taskID, w.Idx, err)
		}
		pendingWrites = append(pendingWrites, &engine_common.PendingWrite{
			TaskId:  taskID,
			Channel: w.Channel,
			Value:   cv,
		})
	}

	tuple := &engine_common.CheckpointTuple{
		Config:        cfg,
		Checkpoint:    cp,
		Metadata:      md,
		PendingWrites: pendingWrites,
	}

	if row.ParentCheckpointID != "" {
		parentCID := row.ParentCheckpointID
		tuple.ParentConfig = &engine_common.EngineRunnableConfig{
			ThreadId:     &threadID,
			CheckpointNs: &ns,
			CheckpointId: &parentCID,
		}
	}

	return tuple, nil
}

// encodeWriteValue converts a ChannelValue oneof into the (encoding, blob) pair
// stored in checkpoint_writes. SerializedValue is stored verbatim; Sends and
// Missing are proto-marshaled with a sentinel encoding so the oneof can be
// reconstructed on read-back. The blob column is NOT NULL in postgres, so we
// always return a non-nil byte slice — proto3 deserializes empty bytes as a
// nil []byte, but langgraph's serializer routinely emits ("null", b"") for
// None-valued writes (e.g. branch:to:* control channels), which pgx would
// otherwise persist as SQL NULL and violate the NOT NULL constraint.
func encodeWriteValue(cv *engine_common.ChannelValue) (string, []byte, error) {
	if cv != nil {
		if sv := cv.GetSerializedValue(); sv != nil {
			blob := sv.GetValue()
			if blob == nil {
				blob = []byte{}
			}
			return sv.GetEncoding(), blob, nil
		}
		if cv.GetVal() != nil {
			b, err := proto.Marshal(cv)
			if err != nil {
				return "", nil, fmt.Errorf("marshal channel value: %w", err)
			}
			if b == nil {
				b = []byte{}
			}
			return channelValueProtoEncoding, b, nil
		}
	}
	return "", []byte{}, nil
}

// decodeWriteValue is the inverse of encodeWriteValue.
func decodeWriteValue(encoding string, blob []byte) (*engine_common.ChannelValue, error) {
	if encoding == channelValueProtoEncoding {
		cv := &engine_common.ChannelValue{}
		if err := proto.Unmarshal(blob, cv); err != nil {
			return nil, fmt.Errorf("unmarshal channel value: %w", err)
		}
		return cv, nil
	}
	return &engine_common.ChannelValue{
		Val: &engine_common.ChannelValue_SerializedValue{
			SerializedValue: &engine_common.SerializedValue{
				Encoding: encoding,
				Value:    blob,
			},
		},
	}, nil
}
