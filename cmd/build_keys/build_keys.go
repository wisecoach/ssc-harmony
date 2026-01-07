package main

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/harmony-one/harmony/accounts/keystore"
	"github.com/harmony-one/harmony/internal/blsgen"
	common2 "github.com/harmony-one/harmony/internal/common"
	"github.com/harmony-one/harmony/internal/genesis"
	"github.com/harmony-one/harmony/ssc"
	"github.com/harmony-one/harmony/ssc/api"
	"github.com/pborman/uuid"
	"github.com/pelletier/go-toml"
	"gopkg.in/yaml.v2"
)

type Validator struct {
	Index        int
	Address      string // account HMY address
	EthAddr      string // account ETH address
	BLSPublicKey string // account public BLS key
	ShardID      uint32 // shardID of the account
	EcdsaKeyPath string
	BLSKeyPATH   string
}

func build_client_keys() {
	for i := 0; i < 1024; i++ {
		privateKey, _ := ecdsa.GenerateKey(crypto.S256(), rand.Reader)

		// 从私钥派生出公钥和地址
		publicKey := privateKey.PublicKey
		address := crypto.PubkeyToAddress(publicKey)

		hash := common.BigToHash(privateKey.D)
		fmt.Printf("Private Key: %x\n", hash)
		fmt.Printf("Public Key: %x\n", hex.EncodeToString(crypto.FromECDSAPub(&publicKey)))
		fmt.Printf("Ethereum Address: %s\n", address.Hex())

		hexAddr, _ := strings.CutPrefix(strings.ToLower(address.Hex()), "0x")
		if len(hexAddr) != 40 {
			panic("Invalid address")
			fmt.Printf("Invalid address: %s\n", address.Hex())
		}
		crypto.SaveECDSA(fmt.Sprintf(".hmy/expr_accounts/%s.key", hexAddr), privateKey)
	}
}

func build_validators() {

	blsOutput := ".hmy/validator_bls"
	ecdsaOutput := ".hmy/validator_ecdsa"
	os.MkdirAll(blsOutput, fs.ModeDir)
	os.MkdirAll(ecdsaOutput, fs.ModeDir)

	deployAccounts := make([]*genesis.DeployAccount, 0)
	shard2validators := make(map[uint32][]*Validator)
	shard2validatorMap := make(map[uint32]map[common.Address]*Validator)
	finishedShards := 0
	shardNum := 32
	validatorPerShard := 10
	for i := range shardNum {
		shard2validators[uint32(i)] = make([]*Validator, 0)
		shard2validatorMap[uint32(i)] = make(map[common.Address]*Validator)
	}

	for finishedShards < shardNum {
		privateKey, _ := ecdsa.GenerateKey(crypto.S256(), rand.Reader)
		key := newKeyFromECDSA(privateKey)
		addr := key.Address
		oneAddr, _ := common2.AddressToBech32(addr)
		shardId := getShard(addr, shardNum)
		if _, exists := shard2validatorMap[shardId][addr]; !exists {
			ecdsaFile := fmt.Sprintf(".hmy/validator_ecdsa/%s.key", oneAddr)
			writeFile(ecdsaFile, key)
			blsKey, blsFileName, _ := blsgen.GenBLSKeyWithPassPhrase("")
			blsFile := fmt.Sprintf(".hmy/validator_bls/%s", blsFileName)
			os.Rename(blsFileName, blsFile)

			validator := &Validator{
				Address:      oneAddr,
				EthAddr:      addr.Hex(),
				BLSPublicKey: common.Bytes2Hex(blsKey.GetPublicKey().Serialize()),
				ShardID:      shardId,
				EcdsaKeyPath: ecdsaFile,
				BLSKeyPATH:   blsFile,
			}

			shard2validatorMap[shardId][addr] = validator
			shard2validators[shardId] = append(shard2validators[shardId], validator)

			if len(shard2validators[shardId]) == validatorPerShard {
				finishedShards++
			}
		}
	}
	cnt := 0
	for _, validators := range shard2validators {
		for _, validator := range validators {
			validator.Index = cnt
			account := &genesis.DeployAccount{
				Index:        strconv.Itoa(validator.Index),
				Address:      validator.Address,
				EthAddr:      validator.EthAddr,
				BLSPublicKey: validator.BLSPublicKey,
				ShardID:      validator.ShardID,
			}
			deployAccounts = append(deployAccounts, account)
			cnt++
		}
	}
	writeFile(".hmy/expr_deploy_accounts.json", deployAccounts)
	writeFile(".hmy/validators.json", shard2validators)
}

func getShard(addr common.Address, shardNum int) uint32 {
	shardBits := int(math.Ceil(math.Log2(float64(shardNum))))
	shardId := ssc.BitsToUint32(addr, shardBits)
	return shardId
}

func writeFile(path string, data any) {
	b, _ := json.Marshal(data)
	file, _ := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	defer file.Close()

	// 将字符串写入文件
	_, _ = file.Write(b)
}

func writeYaml(path string, data any) {
	b, _ := yaml.Marshal(data)
	file, _ := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	defer file.Close()

	// 将字符串写入文件
	_, _ = file.Write(b)
}

func newKeyFromECDSA(privateKeyECDSA *ecdsa.PrivateKey) *keystore.Key {
	id := uuid.NewRandom()
	key := &keystore.Key{
		ID:         id,
		Address:    crypto.PubkeyToAddress(privateKeyECDSA.PublicKey),
		PrivateKey: privateKeyECDSA,
	}
	return key
}

// func main() {
// 	build_validators()
// }

type ShardConfig struct {
	name      string // test name
	shard     uint32 // shard num
	validator int    // validator num per shard
	ssc       int    // ssc member num per shard
	delay     uint64
}

func buildConfig(config ShardConfig) {
	dev_path := fmt.Sprintf("test/configs/%s/shard=%d_validator=%d_ssc=%d_delay=%d", "dev", config.shard, config.validator, config.ssc, config.delay)
	local_path := fmt.Sprintf("test/configs/%s/shard=%d_validator=%d_ssc=%d_delay=%d", "local", config.shard, config.validator, config.ssc, config.delay)
	os.MkdirAll(dev_path, 0755)
	os.MkdirAll(local_path, 0755)

	shard2validators := make(map[uint32][]*Validator)
	file, _ := os.Open(".hmy/validators.json")
	defer file.Close()
	json.NewDecoder(file).Decode(&shard2validators)

	type DevServers struct {
		IPs []string `json:"ips"`
	}
	devServers := new(DevServers)
	file, _ = os.Open("../dev_servers.json")
	defer file.Close()
	json.NewDecoder(file).Decode(&devServers)

	local_launch_config_lines := make([]string, 0)
	dev_launch_config_lines := make([]string, 0)
	for i := uint32(0); i < config.shard; i++ {
		for j := 0; j < config.validator; j++ {
			v := shard2validators[i][j]
			local_launch_config_lines = append(local_launch_config_lines, fmt.Sprintf("%s %s %s %d %s %d",
				v.Address, v.EthAddr, v.BLSKeyPATH, v.ShardID, "127.0.0.1", 9000+40*int(i)+j*2))
			dev_launch_config_lines = append(dev_launch_config_lines, fmt.Sprintf("%s %s %s %d %s %d",
				v.Address, v.EthAddr, v.BLSKeyPATH, v.ShardID, devServers.IPs[i], 9000+40*int(i)+j*2))
		}
	}
	os.WriteFile(local_path+"/"+"launch_config_local.txt", []byte(strings.Join(local_launch_config_lines, "\n")), 0644)
	os.WriteFile(dev_path+"/"+"launch_config_dev.txt", []byte(strings.Join(dev_launch_config_lines, "\n")), 0644)

	localConfig := &api.ShardSimulateCommitteeConfig{}
	devConfig := &api.ShardSimulateCommitteeConfig{}
	localSSC := make([]*api.ShardSimulateCommittee, 0)
	devSSC := make([]*api.ShardSimulateCommittee, 0)
	for i := uint32(0); i < config.shard; i++ {
		localSSCMembers := make([]*api.Member, 0)
		devSSCMembers := make([]*api.Member, 0)
		timeout := &api.TimeoutConfig{
			Sp1:         config.delay,
			PoolTimeout: config.delay,
		}
		for j := 0; j < config.ssc; j++ {
			s := shard2validators[i][j]
			localSSCMembers = append(localSSCMembers, &api.Member{
				Address:   common.HexToAddress(s.EthAddr),
				Stake:     new(big.Int).SetUint64(1000000000000000000),
				Endpoint:  fmt.Sprintf("http://127.0.0.1:%d", 9500+40*int(i)+j*2),
				BLSPubKey: s.BLSPublicKey,
			})
			devSSCMembers = append(devSSCMembers, &api.Member{
				Address:   common.HexToAddress(s.EthAddr),
				Stake:     new(big.Int).SetUint64(1000000000000000000),
				Endpoint:  fmt.Sprintf("http://%s:%d", devServers.IPs[i], 9500+40*int(i)+j*2),
				BLSPubKey: s.BLSPublicKey,
			})
		}
		localSSC = append(localSSC, &api.ShardSimulateCommittee{
			ShardID:   i,
			Epoch:     0,
			Members:   localSSCMembers,
			Number:    len(localSSCMembers),
			Threshold: int(math.Ceil(float64((2*len(localSSCMembers) + 1) / 3))),
		})
		devSSC = append(devSSC, &api.ShardSimulateCommittee{
			ShardID:   i,
			Epoch:     0,
			Members:   devSSCMembers,
			Number:    len(devSSCMembers),
			Threshold: int(math.Ceil(float64((2*len(devSSCMembers) + 1) / 3))),
		})
		localConfig.Timeout = timeout
		devConfig.Timeout = timeout
	}
	localConfig.Committees = localSSC
	devConfig.Committees = devSSC
	type GenesisConfig struct {
		SSCConfig           *api.ShardSimulateCommitteeConfig `json:"ssc_config" yaml:"ssc_config"`
		GenesisAccountsDir  string                            `json:"genesis_accounts_dir" yaml:"genesis_accounts_dir"`
		ContractDeployerDir string                            `json:"contract_deployer_dir" yaml:"contract_deployer_dir"`
	}
	local_genesis := &GenesisConfig{
		SSCConfig:           localConfig,
		GenesisAccountsDir:  ".hmy/expr_accounts",
		ContractDeployerDir: ".hmy/contract_deploy_accounts",
	}
	dev_genesis := &GenesisConfig{
		SSCConfig:           devConfig,
		GenesisAccountsDir:  ".hmy/expr_accounts",
		ContractDeployerDir: ".hmy/contract_deploy_accounts",
	}
	writeYaml(local_path+"/"+"genesis_config_local.yaml", local_genesis)
	writeYaml(dev_path+"/"+"genesis_config_dev.yaml", dev_genesis)

	localTomlConfig, _ := toml.LoadFile("test/configs/local/default_config_local.toml")
	devTomlConfig, _ := toml.LoadFile("test/configs/dev/default_config_dev.toml")
	localGeneral := localTomlConfig.Get("General").(*toml.Tree)
	devGeneral := devTomlConfig.Get("General").(*toml.Tree)
	localGeneral.Set("GenesisConfigFile", local_path+"/"+"genesis_config_local.yaml")
	devGeneral.Set("GenesisConfigFile", dev_path+"/"+"genesis_config_dev.yaml")
	localToml, _ := os.Create(local_path + "/" + "default_config_local.toml")
	devToml, _ := os.Create(dev_path + "/" + "default_config_dev.toml")
	localTomlConfig.Set("General", localGeneral)
	localTomlConfig.WriteTo(localToml)
	devTomlConfig.Set("General", devGeneral)
	devTomlConfig.WriteTo(devToml)
}

func main() {
	configs := []ShardConfig{
		{
			name:      "基准测试",
			shard:     4,
			validator: 4,
			ssc:       1,
			delay:     10,
		},
		{
			name:      "基准测试，时延5",
			shard:     4,
			validator: 4,
			ssc:       1,
			delay:     5,
		},
		{
			name:      "基准测试，时延20",
			shard:     4,
			validator: 4,
			ssc:       1,
			delay:     20,
		},
		{
			name:      "不同分片数_2",
			shard:     2,
			validator: 4,
			ssc:       1,
			delay:     10,
		},
		{
			name:      "不同分片数_8",
			shard:     8,
			validator: 4,
			ssc:       1,
			delay:     10,
		},
		{
			name:      "不同分片数_16",
			shard:     16,
			validator: 4,
			ssc:       1,
			delay:     10,
		},
		{
			name:      "不同分片数_32",
			shard:     32,
			validator: 4,
			ssc:       1,
			delay:     10,
		},
		{
			name:      "安全性测试",
			shard:     2,
			validator: 10,
			ssc:       4,
			delay:     10,
		},
	}
	for _, config := range configs {
		buildConfig(config)
	}
}

func main1() {
	build_client_keys()
}
