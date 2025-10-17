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
	sp1           uint64
}

func NewTimerManager(config *api.TimeoutConfig, service *sscService) *CXTTimerManager {
	return &CXTTimerManager{
		lock:           lm.NewMutex(),
		service:        service,
		selfShard:      0,
		bkNum2txForSp1: make(map[uint64]map[common.Hash]struct{}),
		txs:            make(map[common.Hash]txInfo),
		config:         config,
	}
}

type CXTTimerManager struct {
	lock           lm.Mutex
	service        *sscService
	selfShard      uint32
	bkNum2txForSp1 map[uint64]map[common.Hash]struct{}
	txs            map[common.Hash]txInfo
	config         *api.TimeoutConfig
}

func (c *CXTTimerManager) StartTimer(txHash common.Hash, blockNum uint64, originShardId uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if originShardId == c.selfShard {
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
			utils.SSCLogger().Info().Str("txHash", hash.String()).Msgf("cxt timeout at block %d committed, start to recall proof", blockNum)
			go c.service.handleTxSp1Timeout(txInfo.txHash)
			delete(c.txs, hash)
		}
		delete(c.bkNum2txForSp1, blockNum)
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
