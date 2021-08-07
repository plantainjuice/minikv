package test

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

const MaxLevel = 7

type Node struct {
	Value int   // 存储值
	Prev  *Node // 同层前节点
	Next  *Node // 同层后节点
	Down  *Node // 下层同节点
}

type SkipList struct {
	Level       int
	HeadNodeArr []*Node
}

// 初始化跳表
func NewSkipList() *SkipList {
	list := new(SkipList)
	list.Level = -1                            // 设置层级别
	list.HeadNodeArr = make([]*Node, MaxLevel) // 初始化头节点数组
	rand.Seed(time.Now().UnixNano())
	return list
}

// level-(n)	 nil
//
// level-(1)    Head <-> Node0 <-----------> Node2 <-----------> Node4 <-----------> nil
// 				           |				   |
// level-(0)    Head <-> Node0 <-> Node1 <-> Node2 <-> Node3 <-> Node4 <-> Node5 <-> nil

// 从顶部开始找起， 相当于一棵树，从树的根节点找起
func (list SkipList) HasNode(value int) *Node {
	if list.Level >= 0 {
		level := list.Level
		node := list.HeadNodeArr[level].Next
		for node != nil {
			if node.Value == value {
				return node
			} else if node.Value > value {
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
			} else if node.Value < value {
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
func (list *SkipList) DeleteNode(value int) {

	node := list.HasNode(value)
	if node == nil {
		return
	}
	// 如果有节点就删除
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
func (list *SkipList) AddNode(value int) {
	// 如果包含相同的数据，就返回，不用添加了
	if list.HasNode(value) != nil {
		return
	}
	headNodeInsertPositionArr := make([]*Node, MaxLevel)

	// 如果不包含数据，就查找每一层的插入位置
	if list.Level >= 0 {
		// 只有层级在大于等于 0 的时候在进行循环判断，如果层级小于 0 说明是没有任何数据
		level := list.Level
		node := list.HeadNodeArr[level].Next
		for node != nil && level >= 0 {
			if node.Value > value {
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
			} else if node.Value < value {
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

	list.InsertValue(value, headNodeInsertPositionArr)
}

func (list *SkipList) InsertValue(value int, headNodeInsertPositionArr []*Node) {
	// 插入最底层
	node := new(Node)
	node.Value = value
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
		for randLevel() && currentLevel <= list.Level+1 && currentLevel < MaxLevel {
			// 通过摇点 和 顶层判断是否创建新层，顶层判断有两种判断，一、不能超过预定的最高层，二、不能比当前层多出过多层，也就是说最多只能增加1层
			if headNodeInsertPositionArr[currentLevel] == nil {
				rootNode = new(Node)
				list.HeadNodeArr[currentLevel] = rootNode
			} else {
				rootNode = headNodeInsertPositionArr[currentLevel]
			}

			nextNode = rootNode.Next

			upNode := new(Node)
			upNode.Value = value
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

// 通过抛硬币决定是否加入下一层
func randLevel() bool {
	randNum := rand.Intn(2)
	return randNum == 1
}

func checkSeq(list *SkipList) bool {
	for i := list.Level; i >= 0; i-- {
		node := list.HeadNodeArr[i].Next
		last := node
		first := true
		for node != nil {
			if first {
				first = false
			} else {
				if last.Value >= node.Value {
					return false
				}
				last = node
			}
			node = node.Next
		}
	}
	return true
}

func checkLoop(list *SkipList) bool {
	for i := list.Level; i >= 0; i-- {
		node := list.HeadNodeArr[i].Next
		flag := node
		first := false
		for node != nil {
			if !first && node == flag {
				first = true
			} else if first && node == flag {
				return true
			}
			node = node.Next
		}
	}
	return false
}

func printSkipList(list *SkipList) {
	fmt.Println("====================start=============== " + strconv.Itoa(list.Level))
	for i := list.Level; i >= 0; i-- {
		node := list.HeadNodeArr[i].Next
		fmt.Print("level " + strconv.Itoa(i) + "\t")
		for node != nil {
			fmt.Print(strconv.Itoa(node.Value) + " <-> ")
			node = node.Next
		}
		fmt.Println("nil")
	}
	fmt.Println("====================end===============")
	fmt.Println()
}

func TestSkipList(t *testing.T) {
	list := NewSkipList()
	for i := 0; i < 250; i++ {
		insert := rand.Intn(1000)
		t.Log(insert)
		list.AddNode(insert)
		printSkipList(list)
		if !checkSeq(list) {
			t.Fatal("seq error")
			break
		}

		if checkLoop((list)) {
			t.Fatal("loop error")
			break
		}
	}
}
