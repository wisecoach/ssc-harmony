// SPDX-License-Identifier: MIT
pragma solidity >=0.7.0 <0.8.0;
pragma experimental ABIEncoderV2;

    struct InternalTx {
        address from;
        address to;
        uint index;
        uint parentIndex;
        uint shardId;
        string[] states;
    }

interface CTXSampleContract {
    function simulate(InternalTx[] memory txs, uint index) external returns (uint);
}

contract TestContract {
    address[] private targetAddresses;
    mapping(address => CTXSampleContract) private contracts;
    mapping(string => uint) private states;


    constructor(address[] memory addresses) {
        targetAddresses = addresses;
        for (uint i = 0; i < targetAddresses.length; i++) {
            contracts[targetAddresses[i]] = CTXSampleContract(targetAddresses[i]);
        }
    }

    /**
     * txs          所有内部调用交易tx
     * index        当前正常执行的内部交易序号
     */
    function simulate(InternalTx[] memory txs, uint index) external returns (uint) {
        // 执行完毕
        if (index >= txs.length) {
            return index;
        }

        // 执行当前内部交易
        for (uint i = 0; i < txs[index].states.length; i++) {                       // i < xxx 127 i++ 232
            states[txs[index].states[i]] = 1;                                       // 226
        }

        // 递归执行后续的内部交易
        uint nextIndex = index + 1;
        while (nextIndex < txs.length)
        {
            InternalTx memory itx = txs[nextIndex];
            // 如果是需要调用的内部交易，并设置递归调用后的索引
            if (itx.parentIndex == index) {
                CTXSampleContract cc = contracts[targetAddresses[itx.shardId]];
                nextIndex = cc.simulate(txs, nextIndex);                 // 538-548
            } else {
                // 如果不再需要调用，则跳出循环
                break;
            }
        }

        for (uint i = 0; i < txs[index].states.length; i++) {                       // i++ 730
            delete states[txs[index].states[i]];                                    // 725
        }
        return nextIndex;
    }
}
