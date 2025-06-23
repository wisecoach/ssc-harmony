pragma solidity >=0.4.22 <0.8.0;

contract Counter {
    uint256 private count = 0;
    uint256 moneyStored = 0;

    function addCounter(uint256 a) public {
        count += a;
    }

    function incrementCounter() public returns (uint256) {
        count += 1;
        return count;
    }

    function decrementCounter() public returns (uint256) {
        count -= 1;
        return count;
    }

    function addMoney() payable public {
        moneyStored += msg.value;
    }

    function getCount() public view returns (uint256) {
        return count;
    }

    function getMoneyStored() public view returns (uint256){
        return moneyStored;
    }
}