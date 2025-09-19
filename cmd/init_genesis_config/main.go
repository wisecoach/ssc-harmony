package main

import (
	"fmt"
	common2 "github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/internal/common"
	"github.com/harmony-one/harmony/internal/genesis"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/ssc"
	"github.com/harmony-one/harmony/ssc/api"
	"gopkg.in/yaml.v2"
	"math"
	"math/big"
	"os"
)

func buildGenesisConfig() {
	genesis := core.Genesis{}

	sscCommittees := make([]*api.ShardSimulateCommittee, 0)

	shard0Members := make([]*api.Member, 0)
	shard0Member := &api.Member{
		Address:   common.MustBech32ToAddress("one13c9xmt4f73t8ct73csrmnptz6ydd2t9fx08llk"),
		Stake:     big.NewInt(1000000000),
		Endpoint:  "http://localhost:9500",
		BLSPubKey: common2.Hex2Bytes("9035f9fafa7afe0d7237de9becff06365aaefdd46f937489354c86d4a72b485e386e25ec2b5bbfcc2dec5b1bb90ac711"),
	}
	shard0Members = append(shard0Members, shard0Member)
	shard0 := &api.ShardSimulateCommittee{
		ShardID:   0,
		Epoch:     0,
		Members:   shard0Members,
		Number:    1,
		Threshold: 1,
	}
	shard1Members := make([]*api.Member, 0)
	shard1Member := &api.Member{
		Address:   common.MustBech32ToAddress("one105clp5lulaqq96x64cvjt5fw2yh0he0frdlqdj"),
		Stake:     big.NewInt(1000000000),
		Endpoint:  "http://localhost:9540",
		BLSPubKey: common2.Hex2Bytes("4baf0b53e9c9c70c67a57dd68ce1d955424d7253f5be572d3872730c533233373ed435c054aacdd591a1aef8757d3508"),
	}
	shard1Members = append(shard1Members, shard1Member)
	shard1 := &api.ShardSimulateCommittee{
		ShardID:   1,
		Epoch:     0,
		Members:   shard1Members,
		Number:    1,
		Threshold: 1,
	}
	shard2Members := make([]*api.Member, 0)
	shard2Member := &api.Member{
		Address:   common.MustBech32ToAddress("one14gtzsk0kz6p5ne6uku4yy2q6438352fzhzf2ef"),
		Stake:     big.NewInt(1000000000),
		Endpoint:  "http://localhost:9580",
		BLSPubKey: common2.Hex2Bytes("e5d2d5003a0e60cefaa40d3fd8a5338974bf0cd4d2c3e5a7f4468d7f09e443ea406d408352a0df736206ca8eefd55b0e"),
	}
	shard2Members = append(shard2Members, shard2Member)
	shard2 := &api.ShardSimulateCommittee{
		ShardID:   2,
		Epoch:     0,
		Members:   shard2Members,
		Number:    1,
		Threshold: 1,
	}
	shard3Members := make([]*api.Member, 0)
	shard3Member := &api.Member{
		Address:   common.MustBech32ToAddress("one10tkmez3662chu04cpww74m24gntfau0xxscnu3"),
		Stake:     big.NewInt(1000000000),
		Endpoint:  "http://localhost:9620",
		BLSPubKey: common2.Hex2Bytes("9dbd0e1068292794d4f648e4cf90e40c58854a73a22d334bc5a06f4f96dd878380169dcf2d9d176890c6ad5e575a7c02"),
	}
	shard3Members = append(shard3Members, shard3Member)
	shard3 := &api.ShardSimulateCommittee{
		ShardID:   3,
		Epoch:     0,
		Members:   shard3Members,
		Number:    1,
		Threshold: 1,
	}
	sscCommittees = append(sscCommittees, shard0)
	sscCommittees = append(sscCommittees, shard1)
	sscCommittees = append(sscCommittees, shard2)
	sscCommittees = append(sscCommittees, shard3)
	genesis.SSCConfig = &api.ShardSimulateCommitteeConfig{Committees: sscCommittees}
	genesis.GenesisAccountsDir = ".hmy/expr_accounts"

	bytes, _ := yaml.Marshal(genesis)
	println(string(bytes))

	file, err := os.OpenFile("./test/genesis_config.yaml", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer file.Close()
	_, err = file.Write(bytes)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}

func selectShard() {
	accounts := make(map[uint32][]genesis.DeployAccount)
	for i, account := range genesis.ExprHarmonyAccounts {
		blsKeyBytes := common2.Hex2Bytes(account.BLSPublicKey)
		address := utils.GetAddressFromBLSPubKeyBytes(blsKeyBytes)
		shardNum := 4
		shardBits := int(math.Ceil(math.Log2(float64(shardNum))))
		shardId := ssc.BitsToUint32(address, shardBits)
		fmt.Printf("account %d, address: %s, actualShardId: %d, shardID: %d\n", i, address.Hex(), shardId, account.ShardID)
		accounts[shardId] = append(accounts[shardId], account)
	}
	for i := 0; i < 4; i++ {
		shardId := uint32(i)
		deployAccounts := accounts[shardId]
		for k, account := range deployAccounts {
			fmt.Printf("%s .hmy/new_validator_bls/%s.key %d 127.0.0.1 %d\n", account.Address, account.BLSPublicKey, shardId, int(shardId)*40+k*2+9000)
		}
	}
	cnt := 0
	for i := 0; i < 4; i++ {
		shardId := uint32(i)
		deployAccounts := accounts[shardId]
		for _, account := range deployAccounts {
			fmt.Printf("{Index: \" %d \", Address: \"%s\", EthAddr: \"%s\", BLSPublicKey: \"%s\", ShardID: %d},\n",
				cnt, account.Address, account.EthAddr, account.BLSPublicKey, account.ShardID)
			cnt++
		}
	}
}

func convertBeth2Eth() {
	for _, account := range genesis.ExprHarmonyAccounts {
		fmt.Println(common.MustBech32ToAddress(account.Address))
	}
}

func main() {
	convertBeth2Eth()
	// buildGenesisConfig()
	// selectShard()
}
