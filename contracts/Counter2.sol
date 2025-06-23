pragma solidity >=0.4.22 <0.8.0;

interface Counter {
    function incrementCount(bytes16 shardId) external returns (uint256);
    function getCount(bytes16 shardId) external view returns (uint256);
}

contract CrossCounter {
    uint256 private cross_cnt = 1;
    Counter target;

    constructor(address targetAddress) {
        target = Counter(targetAddress);
    }

    function crossCnt() public {
        bytes16 shardId = 0x63726f73732d73686172640000000000;
        uint256 ret = target.incrementCount(shardId);
        cross_cnt += ret;
    }

    function getCount() public view returns (uint256) {
        bytes16 shardId = 0x63726f73732d73686172640000000000;
        uint256 targetCnt = target.getCount(shardId);
        uint256 ret = cross_cnt << 5;
        ret += targetCnt;
        return ret;
    }
}
