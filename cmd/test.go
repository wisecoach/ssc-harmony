package main

import "github.com/ethereum/go-ethereum/common/hexutil"

func main() {
	a := []byte{0x63, 0x72, 0x6f, 0x73, 0x73, 0x2d, 0x73, 0x68, 0x61, 0x72, 0x64}
	b := uint64(637874<<32 + 10)
	encodeUint64 := hexutil.EncodeUint64(b)
	println(string(a))
	println(b)
	println(string(encodeUint64))
}
