package zero

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
)

const (
	TopWeight   = 100
	minReplicas = 100
	prime       = 16777619
)

type (
	Func func(data []byte) uint64

	ConsistentHash struct {
		// 哈希函数
		hashFunc Func
		// 虚拟节点放大因子
		// 确定node的虚拟节点数量
		replicas int
		// 虚拟节点列表
		keys []uint64
		// 虚拟节点到物理节点的映射
		ring map[uint64][]interface{}
		// 物理节点映射，快速判断是否存在node
		nodes map[string]struct{}
		// 读写锁
		lock sync.RWMutex
	}
)

func NewConsistentHash() *ConsistentHash {
	return NewCustomConsistentHash(minReplicas, Hash)
}
func NewCustomConsistentHash(replicas int, fn Func) *ConsistentHash {
	if replicas < minReplicas {
		replicas = minReplicas
	}

	if fn == nil {
		fn = Hash
	}

	return &ConsistentHash{
		replicas: replicas,
		hashFunc: fn,
		ring:     make(map[uint64][]interface{}),
		nodes:    make(map[string]struct{}),
	}
}

// 扩容操作，增加物理节点
func (h *ConsistentHash) Add(node string) {
	h.AddWithReplicas(node, h.replicas)
}

// 扩容操作，增加物理节点
func (h *ConsistentHash) AddWithReplicas(node string, replicas int) {
	// 支持可重复添加
	// 先执行删除操作
	h.Remove(node)

	if replicas > h.replicas {
		replicas = h.replicas
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	// 添加node map映射
	h.addNode(node)
	for i := 0; i < replicas; i++ {
		hash := h.hashFunc([]byte(node + strconv.Itoa(i)))
		// 添加虚拟节点
		h.keys = append(h.keys, hash)
		// 映射虚拟节点-真实节点
		// 注意hashFunc可能会出现hash冲突，所以采用的是追加操作
		// 虚拟节点-真实节点的映射对应的其实是个数组
		// 一个虚拟节点可能对应多个真实节点，当然概率很小
		h.ring[hash] = append(h.ring[hash], node)
	}
	//排序
	//后面会使用二分查找虚拟节点
	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

// 按权重添加节点
// 通过权重来计算方法因子， 最终控制虚拟节点的数量
// 权重越高，虚拟节点越多
func (h *ConsistentHash) AddWithWeight(node string, weight int) {
	replicas := h.replicas * weight / TopWeight
	h.AddWithReplicas(node, replicas)
}

// 根据V顺时针找到最近的虚拟节点
// 再通过虚拟节点映射找到真实节点
func (h *ConsistentHash) Get(v string) (interface{}, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	// 如果还没有物理节点
	if len(h.ring) == 0 {
		return nil, false
	}
	// 计算哈希值
	hash := h.hashFunc([]byte(v))
	// 二分查找
	// 因为每次添加节点后虚拟节点都会重新排序
	// 所以查找到的第一个节点就是我们的目标节点
	// 取余则可以实现环形列表的效果，顺时针查找节点
	index := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	}) % len(h.keys)

	// 虚拟节点->物理节点映射
	nodes := h.ring[h.keys[index]]
	switch len(nodes) {
	case 0:
		return nil, false
	case 1:
		return nodes[0], true
	//存在多个真实节点意味着这出现hash冲突
	default:
		innerIndex := h.hashFunc([]byte(innerRepr(v)))
		pos := int(innerIndex % uint64(len(nodes)))
		return nodes[pos], true
	}
}

// 删除物理节点
func (h *ConsistentHash) Remove(node string) {
	h.lock.Lock()
	defer h.lock.Unlock()

	//	节点不存在
	if !h.containsNode(node) {
		return
	}
	// 移除虚拟节点映射
	for i := 0; i < h.replicas; i++ {
		hash := h.hashFunc([]byte(node + strconv.Itoa(i)))
		// 二分查找到第一个虚拟节点
		index := sort.Search(len(h.keys), func(i int) bool {
			return h.keys[i] >= hash
		})

		if index < len(h.keys) && h.keys[index] == hash {
			h.keys = append(h.keys[:index], h.keys[index+1:]...)
		}
		//虚拟节点删除映射
		h.removeRingNode(hash, node)
	}
	//删除真实节点
	h.removeNode(node)
}

// 删除虚拟-真实节点映射关系
// hash -虚拟节点
// node - 真实节点
func (h *ConsistentHash) removeRingNode(hash uint64, node string) {
	if nodes, ok := h.ring[hash]; ok {
		newNodes := nodes[:0]

		for _, x := range nodes {
			if x != node {
				newNodes = append(newNodes, x)
			}
		}

		if len(newNodes) > 0 {
			h.ring[hash] = newNodes
		} else {
			delete(h.ring, hash)
		}
	}
}

func (h *ConsistentHash) addNode(node string) {
	h.nodes[node] = struct{}{}
}

// 判断节点是否已存在
func (h *ConsistentHash) containsNode(node string) bool {
	_, ok := h.nodes[node]
	return ok
}

// 删除node
func (h *ConsistentHash) removeNode(node string) {
	delete(h.nodes, node)
}

// 可以理解为确定node字符串的序列化方法
// 在遇到hash冲突时需要重新对key进行hash计算
// 为了减少冲突的改率前面追加一个质数 prime来减少冲突的改率
func innerRepr(v interface{}) string {
	return fmt.Sprintf("%d:%v", prime, v)
}
