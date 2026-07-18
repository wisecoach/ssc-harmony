package ssc

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
)

type txInfo struct {
	txHash        common.Hash
	epochs        []api.Epoch
	blockNum      uint64
	originShardId uint32
	poolTimeout   uint64
	sp1           uint64
}

func NewTimerManager(config *api.TimeoutConfig, service *sscService) *CXTTimerManager {
	return &CXTTimerManager{
		lock:                   lm.NewMutex(),
		service:                service,
		selfShard:              service.SelfShard,
		bkNum2txForSp1:         make(map[uint64]map[common.Hash]struct{}),
		bkNum2txForPoolTimeout: make(map[uint64]map[common.Hash]struct{}),
		txs:                    make(map[common.Hash]txInfo),
		config:                 config,
	}
}

type CXTTimerManager struct {
	lock                   lm.Mutex
	service                *sscService
	selfShard              uint32
	bkNum2txForSp1         map[uint64]map[common.Hash]struct{}
	bkNum2txForPoolTimeout map[uint64]map[common.Hash]struct{}
	txs                    map[common.Hash]txInfo
	config                 *api.TimeoutConfig
	blockNum               uint64
}

// StartPoolTimer
// from HandleSimulateRequest/HandleCXTCall to signSimulationCommit
func (c *CXTTimerManager) StartPoolTimer(txHash common.Hash, epochs []api.Epoch, blockNum uint64, originShardId uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.config.PoolTimeout+blockNum < c.blockNum {
		return
	}

	if _, exists := c.txs[txHash]; !exists {
		c.service.stats.PoolTimerStarted.Add(1)
		poolTimeout := blockNum + c.config.PoolTimeout
		utils.SSCLogger().Debug().Str("txHash", txHash.String()).Msgf("start pool timer for cxt, which will timeout at block %d committed, [%d->%d]", poolTimeout, blockNum, poolTimeout)
		if c.bkNum2txForPoolTimeout[poolTimeout] == nil {
			c.bkNum2txForPoolTimeout[poolTimeout] = map[common.Hash]struct{}{}
		}
		c.bkNum2txForPoolTimeout[poolTimeout][txHash] = struct{}{}
		tx, exists := c.txs[txHash]
		if exists {
			tx.poolTimeout = poolTimeout
			c.txs[txHash] = tx
		} else {
			c.txs[txHash] = txInfo{
				txHash:        txHash,
				blockNum:      blockNum,
				originShardId: originShardId,
				poolTimeout:   poolTimeout,
				epochs:        epochs,
			}
		}
	}
}

func (c *CXTTimerManager) removePoolTx(txHash common.Hash) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	tx, exists := c.txs[txHash]
	if !exists {
		return false
	}
	c.service.stats.PoolTimerRemoved.Add(1)
	poolTimeout := tx.poolTimeout
	if txs, exists := c.bkNum2txForPoolTimeout[poolTimeout]; exists {
		delete(txs, txHash)
		if len(txs) == 0 {
			delete(c.bkNum2txForPoolTimeout, poolTimeout)
		}
		return true
	}
	return false
}

func (c *CXTTimerManager) StartSp1Timer(txHash common.Hash, epochs []api.Epoch, blockNum uint64, originShardId uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if originShardId == c.selfShard {
		c.service.stats.Sp1TimerStarted.Add(1)
		sp1 := blockNum + c.config.Sp1
		utils.SSCLogger().Debug().Str("txHash", txHash.String()).Msgf("start sp1 timer for cxt, which will timeout at block %d committed", sp1)
		if c.bkNum2txForSp1[sp1] == nil {
			c.bkNum2txForSp1[sp1] = map[common.Hash]struct{}{}
		}
		c.bkNum2txForSp1[sp1][txHash] = struct{}{}
		c.txs[txHash] = txInfo{
			txHash:        txHash,
			blockNum:      blockNum,
			originShardId: originShardId,
			sp1:           sp1,
			epochs:        epochs,
		}
	}
}

func (c *CXTTimerManager) OnBlockCommitted(blockNum uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.blockNum = blockNum

	utils.SSCLogger().Info().
		Int("txs", len(c.txs)).
		Int("sp1Txs", len(c.bkNum2txForSp1[blockNum])).
		Int("poolTxs", len(c.bkNum2txForPoolTimeout[blockNum])).
		Msg("[CXTTimerManager] OnBlockCommitted")

	if txs, exists := c.bkNum2txForSp1[blockNum]; exists {
		for hash := range txs {
			txInfo, exists := c.txs[hash]
			if !exists {
				continue
			}
			c.service.stats.Sp1TimerFired.Add(1)
			utils.SSCLogger().Debug().Str("txHash", hash.String()).Msgf("cxt sp1 timeout at block %d committed", blockNum)
			go c.service.handleTxSp1Timeout(txInfo)
			delete(c.txs, hash)
		}
		delete(c.bkNum2txForSp1, blockNum)
	}

	if txs, exists := c.bkNum2txForPoolTimeout[blockNum]; exists {
		for hash := range txs {
			txInfo, exists := c.txs[hash]
			if !exists {
				continue
			}
			c.service.stats.PoolTimerFired.Add(1)
			utils.SSCLogger().Debug().Str("txHash", hash.String()).Msgf("cxt pool timeout at block %d committed, close the transaction", blockNum)
			go c.service.handleTxPoolTimeout(txInfo)
			delete(c.txs, hash)
		}
		delete(c.bkNum2txForPoolTimeout, blockNum)
	}

}

func (c *CXTTimerManager) GetTimeoutConfig() *api.TimeoutConfig {
	return c.config
}

func (c *CXTTimerManager) RemoveTx(txHash common.Hash) {
	t0 := time.Now()
	c.lock.Lock()
	defer c.lock.Unlock()

	c.service.stats.Sp1TimerRemoved.Add(1)
	txInfo, exists := c.txs[txHash]
	if !exists {
		return
	}
	if txs, exists := c.bkNum2txForSp1[txInfo.sp1]; exists {
		delete(txs, txHash)
		if len(txs) == 0 {
			delete(c.bkNum2txForSp1, txInfo.sp1)
		}
	}
	delete(c.txs, txHash)

	utils.SSCLogger().Debug().Str("txHash", txHash.Hex()).
		Str("duration", time.Since(t0).String()).
		Msg("CXTTimerManager.RemoveTx timing")
}
