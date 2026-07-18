#!/usr/bin/env bash
# Local build environment for BLS/MCL libraries
export CGO_CFLAGS="-I$PWD/bls/include -I$PWD/mcl/include"
export CGO_LDFLAGS="-L$PWD/bls/lib -L$PWD/mcl/lib -lbls384_256 -lcrypto -lstdc++ -lmcl"
export LD_LIBRARY_PATH="$PWD/bls/lib:$PWD/mcl/lib:/usr/lib/x86_64-linux-gnu"
