package minikv

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// level-(n)	 nil
//
// level-(1)    Head <-> Node0 <-----------> Node2 <-----------> Node4 <-----------> nil
// 				           |				   |
// level-(0)    Head <-> Node0 <-> Node1 <-> Node2 <-> Node3 <-> Node4 <-> Node5 <-> nil

type Node struct {
	KV   *KeyValue // 存储任何类型
	Prev *Node     // 同层前节点
	Next *Node     // 同层后节点
	Down *Node     // 下层同节点
}

type SkipList struct {
	Level       int
	HeadNodeArr []*Node
	MaxLevel    int
}

// 初始化跳表
func NewSkipList(MaxLevel ...int) *SkipList {
	// leveldb 使用 12
	maxLevel := 12
	if len(MaxLevel) != 0 {
		maxLevel = MaxLevel[0]
	}
	list := new(SkipList)
	list.Level = -1
	list.MaxLevel = maxLevel
	list.HeadNodeArr = make([]*Node, maxLevel)
	rand.Seed(time.Now().UnixNano())
	return list
}

func compare(a, b []byte) int {
	return strings.Compare(string(a), string(b))
}

// 从顶部开始找起， 相当于一棵树，从树的根节点找起
func (list SkipList) HasNode(kv *KeyValue) *Node {
	if list.Level >= 0 {
		level := list.Level
		node := list.HeadNodeArr[level].Next
		for node != nil {
			if compare(node.KV.GetKey(), kv.GetKey()) == 0 {
				return node
			} else if compare(node.KV.GetKey(), kv.GetKey()) > 0 {
				// 如果节点的值大于传入的值，就应该返回上个节点并进入下一层
				if node.Prev.Down == nil {
					if level-1 >= 0 { // 初始化头部节点没有相互链接
						node = list.HeadNodeArr[level-1].Next
					} else { // 最后一层
						node = nil
					}
				} else {
					node = node.Prev.Down
				}
				level -= 1
			} else if compare(node.KV.GetKey(), kv.GetKey()) < 0 {
				// 如果节点的值小于传入的值就进入下一个节点，如果下一个节点是 nil，说明本层已经查完了，进入下一层，且从下一层的头部开始
				node = node.Next
				if node == nil {
					level -= 1
					if level >= 0 {
						// 如果不是最底层继续进入到下一层
						node = list.HeadNodeArr[level].Next
					}
				}
			}
		}
	}
	return nil
}

// 删除节点
func (list *SkipList) DeleteNode(kv *KeyValue) {
	//todo 如果顶层只有一个节点删除后会不会异常？
	node := list.HasNode(kv)
	if node == nil {
		return
	}
	for node != nil {
		prevNode := node.Prev
		nextNode := node.Next

		prevNode.Next = nextNode
		if nextNode != nil {
			nextNode.Prev = prevNode
		}
		node = node.Down
	}
}

// 添加数据到跳表中
func (list *SkipList) AddNode(kv *KeyValue) {
	// 如果包含相同的数据，就返回，不用添加了
	if list.HasNode(kv) != nil {
		return
	}
	headNodeInsertPositionArr := make([]*Node, list.MaxLevel)

	// 如果不包含数据，就查找每一层的插入位置
	if list.Level >= 0 {
		// 只有层级在大于等于 0 的时候在进行循环判断，如果层级小于 0 说明是没有任何数据
		level := list.Level
		node := list.HeadNodeArr[level].Next
		for node != nil && level >= 0 {
			if compare(node.KV.GetKey(), kv.GetKey()) > 0 {
				// 如果节点的值大于传入的值，就应该返回上个节点并进入下一层
				headNodeInsertPositionArr[level] = node.Prev
				if node.Prev.Down == nil {
					if level-1 >= 0 {
						node = list.HeadNodeArr[level-1].Next
					} else {
						node = nil
					}
				} else {
					node = node.Prev.Down
				}
				level -= 1
			} else if compare(node.KV.GetKey(), kv.GetKey()) < 0 {
				// 如果节点的值小于传入的值就进入下一个节点，如果下一个节点是 nil，说明本层已经查完了，进入下一层，且从下一层的头部开始
				if node.Next == nil {
					headNodeInsertPositionArr[level] = node
					level -= 1
					if level >= 0 {
						// 如果不是最底层继续进入到下一层
						node = list.HeadNodeArr[level].Next
					}
				} else {
					node = node.Next
				}
			}
		}
	}

	list.InsertValue(kv, headNodeInsertPositionArr)
}

func (list *SkipList) InsertValue(kv *KeyValue, headNodeInsertPositionArr []*Node) {
	// 插入最底层
	node := new(Node)
	node.KV = kv
	if list.Level < 0 {
		// 如果是空的就插入最底层数据
		list.Level = 0
		list.HeadNodeArr[0] = new(Node)
		list.HeadNodeArr[0].Next = node
		node.Prev = list.HeadNodeArr[0]
	} else {
		// 如果不是空的，就插入每一层
		rootNode := headNodeInsertPositionArr[0]
		nextNode := rootNode.Next

		rootNode.Next = node

		node.Prev = rootNode
		node.Next = nextNode

		if nextNode != nil {
			nextNode.Prev = node
		}

		currentLevel := 1
		for randLevel() && currentLevel <= list.Level+1 && currentLevel < list.MaxLevel {
			// 通过摇点 和 顶层判断是否创建新层，顶层判断有两种判断，一、不能超过预定的最高层，二、不能比当前层多出过多层，也就是说最多只能增加1层
			if headNodeInsertPositionArr[currentLevel] == nil {
				rootNode = new(Node)
				list.HeadNodeArr[currentLevel] = rootNode
			} else {
				rootNode = headNodeInsertPositionArr[currentLevel]
			}

			nextNode = rootNode.Next

			upNode := new(Node)
			upNode.KV = kv
			upNode.Down = node
			upNode.Prev = rootNode
			upNode.Next = nextNode

			rootNode.Next = upNode
			if nextNode != nil {
				nextNode.Prev = upNode
			}

			node = upNode
			// 增加层数
			currentLevel++
		}
		// 这里注意，要更新树才加树，不然没过的也将树变成 0 层
		if currentLevel-1 == list.Level+1 { // 如果加了高度
			list.Level = currentLevel - 1
		}
	}
}

// 通过抛硬币决定是否加入下一层，1/4 参考 leveldb
// todo why?
func randLevel() bool {
	return rand.Intn(4) == 0
}

//
func PrintSkipList(list *SkipList) {
	fmt.Println("====================start=============== " + strconv.Itoa(list.Level))
	for i := list.Level; i >= 0; i-- {
		node := list.HeadNodeArr[i].Next
		fmt.Print("level " + strconv.Itoa(i) + "\t")
		for node != nil {
			fmt.Print(string(node.KV.GetKey()))
			fmt.Print(" <-> ")
			node = node.Next
		}
		fmt.Println("nil")
	}
	fmt.Println("====================end===============")
	fmt.Println()
}
