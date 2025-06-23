package main

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/harmony-one/harmony/core/vm"
	"math/big"
	"strconv"
	"strings"
)

func main1() {
	value := big.NewInt(2684270475)
	hex := common.Bytes2Hex(value.Lsh(value, 224).Bytes())
	println(hex)
}

func main3() {
	hex := "0x95a759428f9a8b4bc02e20086085f32b7a440463"
	address := common.HexToAddress(hex)
	b := address.Big()
	println(b)
}

func main() {
	hex := "60806040526004361061001e5760003560e01c80638d21219314610023575b600080fd5b6100756004803603606081101561003957600080fd5b81019080803563ffffffff169060200190929190803563ffffffff169060200190929190803563ffffffff169060200190929190505050610077565b005b60008163ffffffff161161008a5761019f565b60006001840190508263ffffffff168163ffffffff16106100aa57600090505b60008163ffffffff166f63726f73732d7368617264000000000001905060008054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16638d2121936002348161010d57fe5b04838588600189036040518663ffffffff1660e01b815260040180856fffffffffffffffffffffffffffffffff1681526020018463ffffffff1681526020018363ffffffff1681526020018263ffffffff1681526020019450505050506000604051808303818588803b15801561018357600080fd5b505af1158015610197573d6000803e3d6000fd5b505050505050505b50505056fea2646970667358221220ae92433c3cc439a69fbf060cfbac599c28b66c8bc148c18925fc947211e6c3f464736f6c63430007060033"
	bytes := common.Hex2Bytes(hex)
	opCodes := make([]string, 0)
	i := 0
	for i < len(bytes) {
		opCode := vm.OpCode(bytes[i])
		opCodes = append(opCodes, opCode.String())
		i++
		if value, found := strings.CutPrefix(opCode.String(), "PUSH"); found {
			pushLen, _ := strconv.Atoi(value)
			hexValue := "0x" + common.Bytes2Hex(bytes[i:i+pushLen])
			opCodes = append(opCodes, hexValue)
			for j := 0; j < pushLen-1; j++ {
				opCodes = append(opCodes, "")
			}
			i += pushLen
		}
	}
	println(strings.Join(opCodes, "\n"))
}
