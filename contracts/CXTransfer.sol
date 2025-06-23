pragma solidity >=0.4.22 <0.8.0;

interface TransferContract {
    function transfer(uint128 shardId, uint32 current, uint32 n, uint32 count) payable external;
}

contract CXTransferContract {
    TransferContract target;

    constructor(address targetAddress) {
        target = TransferContract(targetAddress);
    }

    function transfer(uint32 current, uint32 n, uint32 count) payable public {
        if (count <= 0) {
            return;
        }
        uint32 next = current + 1;
        if (next >= n) {
            next = 0;
        }
        uint128 shardId = uint128(0x63726f73732d73686172640000000000) + uint128(next);
        target.transfer{value: msg.value/2}(shardId, next, n, count - 1);
    }
}

