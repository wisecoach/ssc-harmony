package api

import (
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/harmony-one/harmony/internal/utils"
)

func NewCallNode(txHash common.Hash, callIndex CallIndex, shardId uint32) *CallNode {
	return &CallNode{
		TxHash:    txHash,
		CallIndex: callIndex,
		ShardId:   shardId,
		Children:  make([]*CallNode, 0),
		DoneCh:    make(chan struct{}),
	}
}

type CallNode struct {
	TxHash    common.Hash
	CallIndex CallIndex
	ShardId   uint32        // 所属分片
	Children  []*CallNode   // 子节点（按 CallIndex 顺序）
	DoneCh    chan struct{} `json:"-"` // 用于等待该节点及其子树完成
	Once      sync.Once     `json:"-"` // 确保 DoneCh 只关闭一次
}

func (n *CallNode) ToData() *CallNodeData {
	children := make([]*CallNodeData, 0, len(n.Children))
	for _, child := range n.Children {
		children = append(children, child.ToData())
	}
	return &CallNodeData{
		CallIndex: n.CallIndex,
		ShardId:   n.ShardId,
		Children:  children,
	}
}

func (n *CallNode) FromData(node *CallNodeData) {
	n.DoneCh = make(chan struct{})
	if node == nil {
		return
	}
	children := make([]*CallNode, 0, len(node.Children))
	for _, child := range node.Children {
		childNode := new(CallNode)
		childNode.FromData(child)
		children = append(children, childNode)
	}
	n.CallIndex = node.CallIndex
	n.ShardId = node.ShardId
	n.Children = children
}

func (n *CallNode) Done() {
	n.Once.Do(func() {
		close(n.DoneCh)
	})
}

func (n *CallNode) Wait(shardId uint32) {
	var wg sync.WaitGroup
	var walk func(*CallNode)
	waitCnt := 0
	walk = func(cn *CallNode) {
		if n.ShardId == shardId && cn != n { // 排除自己
			waitCnt++
			wg.Add(1)
			go func(nn *CallNode) {
				defer wg.Done()
				<-nn.DoneCh
			}(cn)
		}
		for _, child := range cn.Children {
			walk(child)
		}
	}
	walk(n)
	utils.SSCLogger().Debug().Str("txHash", n.TxHash.Hex()).Msgf("Waiting for %d nodes", waitCnt)
	wg.Wait()
}

func NewCallForest() *CallForest {
	return &CallForest{
		mu:      sync.RWMutex{},
		trees:   make([]*CallNode, 0),
		nodeMap: make(map[string]*CallNode),
	}
}

type CallForest struct {
	mu      sync.RWMutex
	trees   []*CallNode          // 多棵树（无公共祖先）
	nodeMap map[string]*CallNode // key = CallIndex.String() → 快速查找
}

func (f *CallForest) Insert(subtree *CallNode) {
	f.mu.Lock()
	defer f.mu.Unlock()

	utils.SSCLogger().Debug().Str("txHash", subtree.TxHash.Hex()).Msgf("Inserting subtree: %v", subtree.ToData())

	// 递归插入所有节点到 nodeMap
	var walk func(*CallNode)
	walk = func(n *CallNode) {
		key := n.CallIndex.ToString()
		if existing, ok := f.nodeMap[key]; ok {
			// 合并：将新子树的 children 合并到 existing
			existing.Children = mergeChildren(existing.Children, n.Children)
		} else {
			f.nodeMap[key] = n
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(subtree)

	// 尝试将新子树与现有树合并
	f.tryMergeTrees(subtree)
}

func (f *CallForest) tryMergeTrees(newRoot *CallNode) {
	// 检查 newRoot 是否是某棵树的后代
	for i, root := range f.trees {
		if isAncestor(root.CallIndex, newRoot.CallIndex) {
			// newRoot 属于 root 的子树 → 插入到 root 中
			addChildByIndex(root, newRoot)
			return
		}
		if isAncestor(newRoot.CallIndex, root.CallIndex) {
			// root 是 newRoot 的后代 → newRoot 成为新根
			addChildByIndex(newRoot, root)
			f.trees[i] = newRoot
			return
		}
	}
	// 无法合并 → 新增一棵树
	f.trees = append(f.trees, newRoot)
}

func (f *CallForest) GetSubtreeForShard(rootCI CallIndex, shardId uint32) *CallNode {
	f.mu.RLock()
	defer f.mu.RUnlock()

	rootKey := rootCI.ToString()
	root := f.nodeMap[rootKey]
	if root == nil {
		return nil
	}

	// 深拷贝子树，只保留 shardId 匹配的节点（或其祖先路径）
	return cloneSubtreeForShard(root, shardId)
}

func (f *CallForest) ExtractSubtree(ci CallIndex) *CallNode {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := ci.ToString()
	if node, ok := f.nodeMap[key]; ok {
		return deepCopy(node)
	}
	return nil
}

// isAncestor 判断 a 是否是 b 的祖先（前缀）
func isAncestor(a, b CallIndex) bool {
	if len(a) >= len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// addChildByIndex 将 child 插入到 parent 的正确位置（按 CallIndex 路径）
func addChildByIndex(parent, child *CallNode) {
	path := child.CallIndex[len(parent.CallIndex):] // 剩余路径
	current := parent
	for _, idx := range path[:len(path)-1] {
		// 找到或创建中间节点（可能缺失）
		found := false
		for _, c := range current.Children {
			if len(c.CallIndex) > len(current.CallIndex) &&
				c.CallIndex[len(current.CallIndex)] == idx {
				current = c
				found = true
				break
			}
		}
		if !found {
			// 创建占位节点（OriginShard 可设为空或继承）
			newNode := &CallNode{
				CallIndex: append([]int(nil), current.CallIndex...),
				ShardId:   math.MaxUint32,
			}
			newNode.CallIndex = append(newNode.CallIndex, idx)
			current.Children = append(current.Children, newNode)
			current = newNode
		}
	}
	// 最后一级：插入 child
	current.Children = append(current.Children, child)
}

// mergeChildren 合并两组子节点，按 CallIndex 去重并递归合并
func mergeChildren(a, b []*CallNode) []*CallNode {
	if len(a) == 0 {
		return deepCopySlice(b)
	}
	if len(b) == 0 {
		return deepCopySlice(a)
	}

	// 使用 map 按 CallIndex 快速索引
	nodeMap := make(map[string]*CallNode)

	// 先插入 a
	for _, node := range a {
		key := node.CallIndex.ToString()
		nodeMap[key] = deepCopy(node) // 避免修改原始树
	}

	// 再合并 b
	for _, node := range b {
		key := node.CallIndex.ToString()
		if existing, ok := nodeMap[key]; ok {
			// 递归合并子节点
			existing.Children = mergeChildren(existing.Children, node.Children)
		} else {
			nodeMap[key] = deepCopy(node)
		}
	}

	// 转为 slice 并按 CallIndex 字典序排序（可选，但推荐）
	result := make([]*CallNode, 0, len(nodeMap))
	for _, node := range nodeMap {
		result = append(result, node)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CallIndex.Compare(result[j].CallIndex) < 0
	})

	return result
}

// deepCopy 深拷贝 CallNode（用于避免副作用）
func deepCopy(node *CallNode) *CallNode {
	if node == nil {
		return nil
	}
	copy := &CallNode{
		CallIndex: append([]int(nil), node.CallIndex...),
		ShardId:   node.ShardId,
		DoneCh:    nil, // 注意：不复制 DoneCh！每个 Forest 应有自己的同步机制
	}
	copy.Children = deepCopySlice(node.Children)
	return copy
}

func deepCopySlice(nodes []*CallNode) []*CallNode {
	if nodes == nil {
		return nil
	}
	copies := make([]*CallNode, len(nodes))
	for i, n := range nodes {
		copies[i] = deepCopy(n)
	}
	return copies
}

// cloneSubtreeForShard 从 root 开始，克隆出包含所有 shardId 节点及其必要祖先路径的子树
func cloneSubtreeForShard(root *CallNode, shardId uint32) *CallNode {
	if root == nil {
		return nil
	}

	// 先深度优先收集所有目标节点的 CallIndex
	var targetIndices []CallIndex
	var collectTargets func(*CallNode)
	collectTargets = func(n *CallNode) {
		if n.ShardId == shardId {
			targetIndices = append(targetIndices, n.CallIndex)
		}
		for _, child := range n.Children {
			collectTargets(child)
		}
	}
	collectTargets(root)

	if len(targetIndices) == 0 {
		// 没有本 shard 节点，但可能 root 本身需要保留？根据语义决定
		// 在 waitFor 场景中，如果 root 不属于本 shard，也不应等待
		// 所以返回 nil
		return nil
	}

	// 构建一个集合便于查找
	targetSet := make(map[string]bool)
	for _, ci := range targetIndices {
		targetSet[ci.ToString()] = true
	}

	// 递归构建新树：只要子树中包含目标节点，就保留该路径
	var build func(*CallNode) *CallNode
	build = func(n *CallNode) *CallNode {
		key := n.CallIndex.ToString()
		isTarget := targetSet[key]

		// 检查任意后代是否是目标（剪枝）
		hasTargetDescendant := isTarget
		if !hasTargetDescendant {
			for _, child := range n.Children {
				if subtreeHasTarget(child, targetSet) {
					hasTargetDescendant = true
					break
				}
			}
		}

		if !hasTargetDescendant {
			return nil
		}

		// 保留该节点
		newNode := &CallNode{
			CallIndex: append([]int(nil), n.CallIndex...),
			ShardId:   n.ShardId,
			// 注意：不复制 DoneCh，这是静态结构
		}

		// 递归处理子节点
		for _, child := range n.Children {
			clonedChild := build(child)
			if clonedChild != nil {
				newNode.Children = append(newNode.Children, clonedChild)
			}
		}

		return newNode
	}

	return build(root)
}

// subtreeHasTarget 辅助函数：判断子树中是否存在目标节点
func subtreeHasTarget(n *CallNode, targetSet map[string]bool) bool {
	key := n.CallIndex.ToString()
	if targetSet[key] {
		return true
	}
	for _, child := range n.Children {
		if subtreeHasTarget(child, targetSet) {
			return true
		}
	}
	return false
}
