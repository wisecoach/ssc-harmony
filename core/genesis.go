// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"github.com/harmony-one/harmony/ssc/api"
	"gopkg.in/yaml.v2"

	"github.com/ethereum/go-ethereum/common"
	ethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	blockfactory "github.com/harmony-one/harmony/block/factory"
	"github.com/harmony-one/harmony/core/genesis"
	"github.com/harmony-one/harmony/internal/params"
	"github.com/harmony-one/harmony/staking/slash"

	"github.com/harmony-one/harmony/common/denominations"
	"github.com/harmony-one/harmony/core/rawdb"
	"github.com/harmony-one/harmony/core/state"
	"github.com/harmony-one/harmony/core/types"
	nodeconfig "github.com/harmony-one/harmony/internal/configs/node"
	shardingconfig "github.com/harmony-one/harmony/internal/configs/sharding"
	"github.com/harmony-one/harmony/internal/utils"
	"github.com/harmony-one/harmony/shard"
)

// no go:generate gencodec -type Genesis -field-override genesisSpecMarshaling -out gen_genesis.go
// no go:generate gencodec -type GenesisAccount -field-override genesisAccountMarshaling -out gen_genesis_account.go

var errGenesisNoConfig = errors.New("genesis has no chain configuration")

const (
	// GenesisEpoch is the number of the genesis epoch.
	GenesisEpoch = 0
	// GenesisONEToken is the initial total number of ONE in the genesis block for mainnet.
	GenesisONEToken = 12600000000
	// ContractDeployerInitFund is the initial fund for the contract deployer account in testnet/devnet.
	ContractDeployerInitFund = 10000000000
	// InitFreeFund is the initial fund for permissioned accounts for testnet/devnet/
	InitFreeFund = 100000000
)

var (
	// GenesisFund is the initial total number of ONE (in atto) in the genesis block for mainnet.
	GenesisFund = new(big.Int).Mul(big.NewInt(GenesisONEToken), big.NewInt(denominations.One))
)

// Genesis specifies the header fields, state of a genesis block. It also defines hard
// fork switch-over blocks through the chain configuration.
type Genesis struct {
	Config         *params.ChainConfig  `json:"config" yaml:"config,omitempty"`
	Factory        blockfactory.Factory `json:"-" yaml:"-"`
	Nonce          uint64               `json:"nonce" yaml:"nonce,omitempty"`
	ShardID        uint32               `json:"shardID" yaml:"shardID,omitempty"`
	Timestamp      uint64               `json:"timestamp" yaml:"timestamp,omitempty"`
	ExtraData      []byte               `json:"extraData" yaml:"extraData,omitempty"`
	GasLimit       uint64               `json:"gasLimit" yaml:"gasLimit,omitempty"       gencodec:"required"`
	Mixhash        common.Hash          `json:"mixHash" yaml:"mixHash,omitempty"`
	Coinbase       common.Address       `json:"coinbase" yaml:"coinbase,omitempty"`
	Alloc          GenesisAlloc         `json:"alloc" yaml:"alloc,omitempty"          gencodec:"required"`
	ShardStateHash common.Hash          `json:"shardStateHash" yaml:"shardStateHash,omitempty" gencodec:"required"`
	ShardState     shard.State          `json:"shardState" yaml:"shardState,omitempty"     gencodec:"required"`

	SSCConfig           *api.ShardSimulateCommitteeConfig `json:"ssc_config" yaml:"ssc_config"`
	GenesisAccountsDir  string                            `json:"genesis_accounts_dir" yaml:"genesis_accounts_dir"`
	ContractDeployerDir string                            `json:"contract_deployer_dir" yaml:"contract_deployer_dir"`

	// These fields are used for consensus tests. Please don't use them
	// in actual genesis blocks.
	Number     uint64      `json:"number"`
	GasUsed    uint64      `json:"gasUsed"`
	ParentHash common.Hash `json:"parentHash"`
}

// NewGenesisSpec creates a new genesis spec for the given network type and shard ID.
// Note that the shard state is NOT initialized.
func NewGenesisSpec(netType nodeconfig.NetworkType, shardID uint32, configPath string) *Genesis {
	genesisAlloc := make(GenesisAlloc)
	chainConfig := params.ChainConfig{}
	gasLimit := params.GenesisGasLimit

	switch netType {
	case nodeconfig.Mainnet:
		chainConfig = *params.MainnetChainConfig
		if shardID == 0 {
			foundationAddress := common.HexToAddress("0xE25ABC3f7C3d5fB7FB81EAFd421FF1621A61107c")
			genesisAlloc[foundationAddress] = GenesisAccount{Balance: GenesisFund}
		}
	case nodeconfig.Testnet:
		chainConfig = *params.TestnetChainConfig
	case nodeconfig.Pangaea:
		chainConfig = *params.PangaeaChainConfig
	case nodeconfig.Partner:
		chainConfig = *params.PartnerChainConfig
	case nodeconfig.Stressnet:
		chainConfig = *params.StressnetChainConfig
	case nodeconfig.Localnet:
		chainConfig = *params.LocalnetChainConfig
	default: // all other types share testnet config
		chainConfig = *params.TestChainConfig
	}

	// All non-mainnet chains get test accounts
	if netType != nodeconfig.Mainnet {
		gasLimit = params.TestGenesisGasLimit
		// Smart contract deployer account used to deploy initial smart contract

		contractDeployerAddress := crypto.PubkeyToAddress(genesis.ContractDeployerKey.PublicKey)
		utils.Logger().Info().Msgf("use netType: %v, contractDeployer: %v\n", netType, contractDeployerAddress)
		contractDeployerFunds := big.NewInt(ContractDeployerInitFund)
		contractDeployerFunds = contractDeployerFunds.Mul(
			contractDeployerFunds, big.NewInt(denominations.One),
		)
		genesisAlloc[contractDeployerAddress] = GenesisAccount{Balance: contractDeployerFunds}

		sscSubmitterAddress := crypto.PubkeyToAddress(genesis.SSCSubmitterKey.PublicKey)
		utils.Logger().Info().Msgf("use netType: %v, sscSubmitter: %v\n", netType, sscSubmitterAddress)
		sscSubmitterFunds := big.NewInt(ContractDeployerInitFund)
		sscSubmitterFunds = sscSubmitterFunds.Mul(
			sscSubmitterFunds, big.NewInt(denominations.One),
		)
		genesisAlloc[sscSubmitterAddress] = GenesisAccount{Balance: sscSubmitterFunds}

		// Localnet only testing account
		if netType == nodeconfig.Localnet {
			// PK: 1f84c95ac16e6a50f08d44c7bde7aff8742212fda6e4321fde48bf83bef266dc
			testAddress := common.HexToAddress("0xA5241513DA9F4463F1d4874b548dFBAC29D91f34")
			genesisAlloc[testAddress] = GenesisAccount{Balance: contractDeployerFunds}
		}
	}

	gen := &Genesis{
		Config:    &chainConfig,
		Factory:   blockfactory.NewFactory(&chainConfig),
		Alloc:     genesisAlloc,
		ShardID:   shardID,
		GasLimit:  gasLimit,
		Timestamp: 1561734000, // GMT: Friday, June 28, 2019 3:00:00 PM. PST: Friday, June 28, 2019 8:00:00 AM
		ExtraData: []byte("Harmony for One and All. Open Consensus for 10B."),
		SSCConfig: nil,
	}

	if len(configPath) > 0 {
		data, err := os.ReadFile(configPath)
		if err == nil {
			if strings.HasSuffix(configPath, "yaml") || strings.HasSuffix(configPath, "yml") {
				_ = yaml.Unmarshal(data, gen)
			}
			genesisScheduleInstance := shard.Schedule.InstanceForEpoch(big.NewInt(GenesisEpoch))
			if genesisScheduleInstance.UseSameAccountEachShard() {
				committeeShard0 := gen.SSCConfig.Committees[0]
				gen.SSCConfig.Committees = make([]*api.ShardSimulateCommittee, 0, int(genesisScheduleInstance.NumShards()))
				gen.SSCConfig.Committees = append(gen.SSCConfig.Committees, committeeShard0)
				for i := 1; i < int(genesisScheduleInstance.NumShards()); i++ {
					committee := &api.ShardSimulateCommittee{
						ShardID:   uint32(i),
						Epoch:     0,
						Members:   make([]*api.Member, 0, committeeShard0.Number),
						Number:    committeeShard0.Number,
						Threshold: committeeShard0.Threshold,
					}
					for _, member := range committeeShard0.Members {
						committee.Members = append(committee.Members, &api.Member{
							Address:   member.Address,
							Stake:     member.Stake,
							Endpoint:  strings.Replace(member.Endpoint, "9500", fmt.Sprintf("%d", 9500+i*40), 1),
							BLSPubKey: member.BLSPubKey,
						})
					}
					gen.SSCConfig.Committees = append(gen.SSCConfig.Committees, committee)
				}
			}
			if len(gen.ContractDeployerDir) > 0 {
				_ = filepath.WalkDir(gen.ContractDeployerDir, func(path string, d os.DirEntry, err error) error {
					if strings.HasSuffix(d.Name(), ".key") {
						addr := common.HexToAddress(strings.TrimSuffix(d.Name(), ".key"))
						gen.Alloc[addr] = GenesisAccount{
							Balance: big.NewInt(InitFreeFund).Mul(big.NewInt(InitFreeFund), big.NewInt(denominations.One)),
						}
						utils.Logger().Info().Msgf("genesis account: %s, addr: %s, balance: %s", path, addr, gen.Alloc[addr].Balance.String())
					}
					return nil
				})
			}
			if len(gen.GenesisAccountsDir) > 0 {
				_ = filepath.WalkDir(gen.GenesisAccountsDir, func(path string, d os.DirEntry, err error) error {
					if strings.HasSuffix(d.Name(), ".key") {
						addr := common.HexToAddress(strings.TrimSuffix(d.Name(), ".key"))
						gen.Alloc[addr] = GenesisAccount{
							Balance: big.NewInt(InitFreeFund).Mul(big.NewInt(InitFreeFund), big.NewInt(denominations.One)),
						}
						utils.Logger().Info().Msgf("genesis account: %s", path)
					}
					return nil
				})
			}
		}
	}

	return gen
}

// GenesisAlloc specifies the initial state that is part of the genesis block.
type GenesisAlloc map[common.Address]GenesisAccount

// UnmarshalJSON is to deserialize the data into GenesisAlloc.
func (ga *GenesisAlloc) UnmarshalJSON(data []byte) error {
	m := make(map[common.UnprefixedAddress]GenesisAccount)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*ga = make(GenesisAlloc)
	for addr, a := range m {
		(*ga)[common.Address(addr)] = a
	}
	return nil
}

// GenesisAccount is an account in the state of the genesis block.
type GenesisAccount struct {
	Code       []byte                      `json:"code,omitempty"`
	Storage    map[common.Hash]common.Hash `json:"storage,omitempty"`
	Balance    *big.Int                    `json:"balance" gencodec:"required"`
	Nonce      uint64                      `json:"nonce,omitempty"`
	PrivateKey []byte                      `json:"secretKey,omitempty"` // for tests
}

// field type overrides for gencodec
type genesisSpecMarshaling struct {
	Nonce      math.HexOrDecimal64
	Timestamp  math.HexOrDecimal64
	ExtraData  hexutil.Bytes
	GasLimit   math.HexOrDecimal64
	GasUsed    math.HexOrDecimal64
	Number     math.HexOrDecimal64
	Difficulty *math.HexOrDecimal256
	Alloc      map[common.UnprefixedAddress]GenesisAccount
}

type genesisAccountMarshaling struct {
	Code       hexutil.Bytes
	Balance    *math.HexOrDecimal256
	Nonce      math.HexOrDecimal64
	Storage    map[storageJSON]storageJSON
	PrivateKey hexutil.Bytes
}

// storageJSON represents a 256 bit byte array, but allows less than 256 bits when
// unmarshaling from hex.
type storageJSON common.Hash

func (h *storageJSON) UnmarshalText(text []byte) error {
	text = bytes.TrimPrefix(text, []byte("0x"))
	if len(text) > 64 {
		return fmt.Errorf("too many hex characters in storage key/value %q", text)
	}
	offset := len(h) - len(text)/2 // pad on the left
	if _, err := hex.Decode(h[offset:], text); err != nil {
		return fmt.Errorf("invalid hex storage key/value %q", text)
	}
	return nil
}

func (h storageJSON) MarshalText() ([]byte, error) {
	return hexutil.Bytes(h[:]).MarshalText()
}

// GenesisMismatchError is raised when trying to overwrite an existing
// genesis block with an incompatible one.
type GenesisMismatchError struct {
	Stored, New common.Hash
}

func (e *GenesisMismatchError) Error() string {
	return fmt.Sprintf("database already contains an incompatible genesis block (have %x, new %x)", e.Stored[:8], e.New[:8])
}

func (g *Genesis) configOrDefault(ghash common.Hash) *params.ChainConfig {
	switch {
	case g != nil:
		return g.Config
	default:
		return params.AllProtocolChanges
	}
}

// ToBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func (g *Genesis) ToBlock(db ethdb.Database) *types.Block {
	if db == nil {
		utils.Logger().Error().Msg("db should be initialized")
		os.Exit(1)
	}
	statedb, _ := state.New(common.Hash{}, state.NewDatabase(db), nil, nil)
	// write alloc of genesis account
	for addr, account := range g.Alloc {
		statedb.AddBalance(addr, account.Balance)
		statedb.SetCode(addr, account.Code, false)
		statedb.SetNonce(addr, account.Nonce)
		for key, value := range account.Storage {
			statedb.SetState(addr, key, value)
		}
		if err := rawdb.WritePreimages(
			statedb.Database().DiskDB(), map[ethCommon.Hash][]byte{
				crypto.Keccak256Hash(addr.Bytes()): addr.Bytes(),
			},
		); err != nil {
			utils.Logger().Error().Err(err).Msg("Failed to store preimage")
			os.Exit(1)
		}
	}
	// write initial ssc committees
	statedb.SetSSCConfig(g.SSCConfig)

	root := statedb.IntermediateRoot(false)
	shardStateBytes, err := shard.EncodeWrapper(g.ShardState, false)
	if err != nil {
		utils.Logger().Error().Err(err).Msg("failed to rlp-serialize genesis shard state")
		os.Exit(1)
	}
	head := g.Factory.NewHeader(common.Big0).With().
		Number(new(big.Int).SetUint64(g.Number)).
		ShardID(g.ShardID).
		Time(new(big.Int).SetUint64(g.Timestamp)).
		ParentHash(g.ParentHash).
		Extra(g.ExtraData).
		GasLimit(g.GasLimit).
		GasUsed(g.GasUsed).
		MixDigest(g.Mixhash).
		Coinbase(g.Coinbase).
		Root(root).
		ShardStateHash(g.ShardStateHash).
		ShardState(shardStateBytes).
		Header()
	statedb.Commit(false)
	statedb.Database().TrieDB().Commit(root, true)

	utils.Logger().Info().Msgf("Genesis block created: %s, coinbase: %s", head.Hash().Hex(), head.Coinbase().Hex())

	return types.NewBlock(head, nil, nil, nil, nil, nil)
}

// Commit writes the block and state of a genesis specification to the database.
// The block is committed as the canonical head block.
func (g *Genesis) Commit(db ethdb.Database) (*types.Block, error) {
	block := g.ToBlock(db)
	if block.Number().Sign() != 0 {
		return nil, fmt.Errorf("can't commit genesis block with number > 0")
	}

	if err := rawdb.WriteBlock(db, block); err != nil {
		return nil, err
	}
	if err := rawdb.WriteReceipts(db, block.Hash(), block.NumberU64(), nil); err != nil {
		return nil, err
	}
	if err := rawdb.WriteCanonicalHash(db, block.Hash(), block.NumberU64()); err != nil {
		return nil, err
	}
	if err := rawdb.WriteHeadBlockHash(db, block.Hash()); err != nil {
		return nil, err
	}
	if err := rawdb.WriteHeadHeaderHash(db, block.Hash()); err != nil {
		return nil, err
	}

	err := rawdb.WriteShardStateBytes(db, block.Header().Epoch(), block.Header().ShardState())

	if err != nil {
		utils.Logger().Error().Err(err).Msg("Failed to store genesis shard state")
	}

	config := g.Config
	if config == nil {
		config = params.AllProtocolChanges
	}
	rawdb.WriteChainConfig(db, block.Hash(), config)
	return block, nil
}

// MustCommit writes the genesis block and state to db, panicking on error.
// The block is committed as the canonical head block.
func (g *Genesis) MustCommit(db ethdb.Database) *types.Block {
	block, err := g.Commit(db)
	if err != nil {
		panic(err)
	}
	rawdb.WriteBlockRewardAccumulator(db, big.NewInt(0), 0)
	data, err := rlp.EncodeToBytes(slash.Records{})
	if err != nil {
		panic(err)
	}
	if err := rawdb.WritePendingSlashingCandidates(db, data); err != nil {
		panic(err)
	}
	return block
}

// GetGenesisSpec for a given shard
func GetGenesisSpec(shardID uint32) *Genesis {
	if shard.Schedule.GetNetworkID() == shardingconfig.MainNet {
		return NewGenesisSpec(nodeconfig.Mainnet, shardID, "")
	}
	if shard.Schedule.GetNetworkID() == shardingconfig.LocalNet {
		return NewGenesisSpec(nodeconfig.Localnet, shardID, "")
	}
	return NewGenesisSpec(nodeconfig.Testnet, shardID, "")
}

// GetInitialFunds for a given shard
func GetInitialFunds(shardID uint32) *big.Int {
	spec, total := GetGenesisSpec(shardID), big.NewInt(0)
	for _, account := range spec.Alloc {
		total = new(big.Int).Add(account.Balance, total)
	}
	return total
}
