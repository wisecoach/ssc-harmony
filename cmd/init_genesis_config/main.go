package main

import (
	"fmt"
	common2 "github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core"
	"github.com/harmony-one/harmony/internal/common"
	"github.com/harmony-one/harmony/ssc/api"
	"gopkg.in/yaml.v2"
	"math/big"
	"os"
)

func main() {
	genesis := core.Genesis{}

	sscCommittees := make([]*api.ShardSimulateCommittee, 0)

	shard0Members := make([]*api.Member, 0)
	shard0Member := &api.Member{
		Address:   common.MustBech32ToAddress("one1pdv9lrdwl0rg5vglh4xtyrv3wjk3wsqket7zxy"),
		Stake:     big.NewInt(1000000000),
		Endpoint:  "http://localhost:9000",
		BLSPubKey: common2.Hex2Bytes("65f55eb3052f9e9f632b2923be594ba77c55543f5c58ee1454b9cfd658d25e06373b0f7d42a19c84768139ea294f6204"),
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
		Address:   common.MustBech32ToAddress("one1m6m0ll3q7ljdqgmth2t5j7dfe6stykucpj2nr5"),
		Stake:     big.NewInt(1000000000),
		Endpoint:  "http://localhost:9002",
		BLSPubKey: common2.Hex2Bytes("40379eed79ed82bebfb4310894fd33b6a3f8413a78dc4d43b98d0adc9ef69f3285df05eaab9f2ce5f7227f8cb920e809"),
	}
	shard1Members = append(shard1Members, shard1Member)
	shard1 := &api.ShardSimulateCommittee{
		ShardID:   1,
		Epoch:     0,
		Members:   shard1Members,
		Number:    1,
		Threshold: 1,
	}
	sscCommittees = append(sscCommittees, shard0)
	sscCommittees = append(sscCommittees, shard1)
	genesis.SSCConfig = &api.ShardSimulateCommitteeConfig{Committees: sscCommittees}

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
