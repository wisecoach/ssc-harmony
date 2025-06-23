package main

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/harmony-one/harmony/accounts/keystore"
	"github.com/harmony-one/harmony/internal/blsgen"
	common2 "github.com/harmony-one/harmony/internal/common"
	"github.com/pborman/uuid"
	"os"
	"strings"
)

func build_client_keys() {

	for i := 0; i < 50; i++ {
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
	mapping := make(map[string]string)

	for i := 0; i < 50; i++ {
		privateKey, _ := ecdsa.GenerateKey(crypto.S256(), rand.Reader)
		key := newKeyFromECDSA(privateKey)
		json, _ := key.MarshalJSON()
		oneAddr, _ := common2.AddressToBech32(key.Address)
		writeFile(fmt.Sprintf(".hmy/new_validator_ecdsa/%s.key", oneAddr), json)

		blsKeyWithPassPhrase, s, err := blsgen.GenBLSKeyWithPassPhrase("")
		if err != nil {
			return
		}
		fmt.Printf("generate blsKey, key:%s, file:%s", blsKeyWithPassPhrase, s)
		os.Rename(s, fmt.Sprintf(".hmy/new_validator_bls/%s", s))
		mapping[oneAddr] = s
	}
	mappingBytes, _ := json.Marshal(mapping)
	writeFile(".hmy/mapping.json", mappingBytes)
}

func writeFile(path string, content []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// 将字符串写入文件
	_, err = file.Write(content)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}
	return nil

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

func main() {
	build_validators()
}
