package ssc

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/harmony-one/harmony/ssc/lm"
)

type txInfo struct {
	txHash        common.Hash
	blockNum      uint64
	originShardId uint32
	poolTimeout   uint64
	sp1           uint64
}

func NewTimerManager(config *api.TimeoutConfig, service *sscService) *CXTTimerManager {
	return &CXTTimerManager{
		lock:                   lm.NewMutex(),
		service:                service,
		selfShard:              0,
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
}

func (c *CXTTimerManager) StartPoolTimer(txHash common.Hash, blockNum uint64, originShardId uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.selfShard == originShardId {
		poolTimeout := blockNum + c.config.PoolTimeout
		utils.SSCLogger().Info().Str("txHash", txHash.String()).Msgf("start pool timer for cxt, which will timeout at block %d committed, [%d->%d]", poolTimeout, blockNum, poolTimeout)
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
			}
		}
	}
}

func (c *CXTTimerManager) removePoolTx(txHash common.Hash) bool {
	tx, exists := c.txs[txHash]
	if !exists {
		return false
	}
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

func (c *CXTTimerManager) StartTimer(txHash common.Hash, blockNum uint64, originShardId uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if originShardId == c.selfShard {
		c.removePoolTx(txHash)
		sp1 := blockNum + c.config.Sp1
		utils.SSCLogger().Info().Str("txHash", txHash.String()).Msgf("start timer for cxt, which will timeout at block %d committed", sp1)
		if c.bkNum2txForSp1[sp1] == nil {
			c.bkNum2txForSp1[sp1] = map[common.Hash]struct{}{}
		}
		c.bkNum2txForSp1[sp1][txHash] = struct{}{}
		c.txs[txHash] = txInfo{
			txHash:        txHash,
			blockNum:      blockNum,
			originShardId: originShardId,
			sp1:           sp1,
		}
	}
}

func (c *CXTTimerManager) BlockCommitted(blockNum uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if txs, exists := c.bkNum2txForSp1[blockNum]; exists {
		for hash, _ := range txs {
			txInfo, exists := c.txs[hash]
			if !exists {
				continue
			}
			utils.SSCLogger().Debug().Str("txHash", hash.String()).Msgf("cxt timeout at block %d committed, start to resimulation", blockNum)
			go c.service.handleTxSp1Timeout(txInfo.txHash)
			delete(c.txs, hash)
		}
		delete(c.bkNum2txForSp1, blockNum)
	}

	if txs, exists := c.bkNum2txForPoolTimeout[blockNum]; exists {
		for hash, _ := range txs {
			txInfo, exists := c.txs[hash]
			if !exists {
				continue
			}
			utils.SSCLogger().Debug().Str("txHash", hash.String()).Msgf("cxt pool timeout at block %d committed, close the transaction", blockNum)
			go c.service.handleTxPoolTimeout(txInfo.txHash, blockNum)
			delete(c.txs, hash)
		}
		delete(c.bkNum2txForPoolTimeout, blockNum)
	}

}

func (c *CXTTimerManager) RemoveTx(txHash common.Hash) {
	c.lock.Lock()
	defer c.lock.Unlock()

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
}
