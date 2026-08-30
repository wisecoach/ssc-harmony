package api

// ============================================================================
// ModuleStatus — SSC 各模块运行期状态快照
//
// 专供实验结束后的资源释放分析：调用监控 gRPC 服务（SSCMonitorService）一次性
// 导出所有模块当前维护的数据量。如果实验结束后某模块的计数不为 0，说明该模块
// 仍有数据残留、资源未释放干净。
// ============================================================================

// RetrySchedulerStatus 重试调度器内部各池/索引的条目数。
type RetrySchedulerStatus struct {
	RetryPool       int `json:"retry_pool"`
	PassivePool     int `json:"passive_pool"`
	StaleTxs        int `json:"stale_txs"`
	Signals         int `json:"signals"`
	Patches         int `json:"patches"`
	OnChainPatches  int `json:"on_chain_patches"`
	LocalPatches    int `json:"local_patches"`
	KeyIndex        int `json:"key_index"`
	Subscriber      int `json:"subscriber"`
	TxSubKeys       int `json:"tx_sub_keys"`
	ConsumedPatches int `json:"consumed_patches"`
	ReSimInFlight   int `json:"re_sim_in_flight"`
	WoundedRetryTxs int `json:"wounded_retry_txs"`
	LockWait        int `json:"lock_wait"`
}

// StateLockStatus 状态锁管理器各全局 sync.Map 的条目数。
type StateLockStatus struct {
	GlobalLocked      int `json:"global_locked"`
	GlobalRLocked     int `json:"global_r_locked"`
	GlobalFinishedTxs int `json:"global_finished_txs"`
	GlobalLockStart   int `json:"global_lock_start"`
}

// TempLockViewStatus 临时锁视图各 sync.Map 的条目数。
type TempLockViewStatus struct {
	WriteLocks int `json:"write_locks"`
	ReadLocks  int `json:"read_locks"`
	TxSets     int `json:"tx_sets"`
	Wounded    int `json:"wounded"`
}

// SimulatorStatus 模拟器各存储的条目数。
type SimulatorStatus struct {
	SimStates         int `json:"sim_states"`
	CallStatesWaiting int `json:"call_states_waiting"`
	SimuChMap         int `json:"simu_ch_map"`
	PendingReqsMap    int `json:"pending_reqs_map"`
	QueueLen          int `json:"queue_len"`
}

// VerifierStatus 验证器各存储的条目数。
type VerifierStatus struct {
	ExecutionVerifyContexts int `json:"execution_verify_contexts"`
	TxLockedSimNum          int `json:"tx_locked_sim_num"`
}

// TimerStatus 定时器管理器各存储的条目数。
type TimerStatus struct {
	Txs                int `json:"txs"`
	Sp1Buckets         int `json:"sp1_buckets"`
	PoolTimeoutBuckets int `json:"pool_timeout_buckets"`
}

// DAGStatus DAG / ChainPatch 使用情况（累计值，不随日志差分重置）。
type DAGStatus struct {
	ChainTxDetected      int `json:"chain_tx_detected"`       // VerifySimulation 判定为链式交易（跳过锁检查）次数
	RetryCommitPatchHit  int `json:"retry_commit_patch_hit"`  // RetryCommit 被 DAG Patch 救回次数
	RetryCommitPatchMiss int `json:"retry_commit_patch_miss"` // RetryCommit Patch 匹配但消费失败次数
}

// ModuleStatus 汇总 sscService 及所有子模块的数据量。
type ModuleStatus struct {
	// sscService 顶层存储
	TxStates     int `json:"tx_states"`
	CommitStates int `json:"commit_states"`
	TxTraces     int `json:"tx_traces"`
	InternalPool int `json:"internal_pool"`

	RetryScheduler RetrySchedulerStatus `json:"retry_scheduler"`
	StateLock      StateLockStatus      `json:"state_lock"`
	TempLockView   TempLockViewStatus   `json:"temp_lock_view"`
	Simulator      SimulatorStatus      `json:"simulator"`
	Verifier       VerifierStatus       `json:"verifier"`
	Timer          TimerStatus          `json:"timer"`
	DAG            DAGStatus            `json:"dag"`
}
