package main

import "container/heap"

const (
	queueCompactMinHead = 64
)

type OrderQueue struct {
	items []*Order
	head  int
}

func NewOrderQueue() *OrderQueue {
	// Complexity: O(1) time, O(1) additional space.
	return &OrderQueue{items: make([]*Order, 0), head: 0}
}

func (q *OrderQueue) Push(order *Order) {
	// Complexity: amortized O(1) time for append; O(1) additional space unless growth triggers reallocation.
	if q == nil {
		return
	}
	q.items = append(q.items, order)
}

func (q *OrderQueue) PeekFront() *Order {
	// Complexity: O(1) time, O(1) space.
	if q == nil || q.Len() == 0 {
		return nil
	}
	return q.items[q.head]
}

func (q *OrderQueue) PopFront() (*Order, bool) {
	// Complexity: amortized O(1) time; worst-case O(n) when compaction runs (n = active elements).
	// Space: O(1) normally; worst-case O(n) temporary allocation during compaction.
	if q == nil || q.Len() == 0 {
		return nil, false
	}
	order := q.items[q.head]
	q.items[q.head] = nil
	q.head++
	q.compactIfNeeded()
	return order, true
}

func (q *OrderQueue) Len() int {
	// Complexity: O(1) time, O(1) space.
	if q == nil {
		return 0
	}
	return len(q.items) - q.head
}

func (q *OrderQueue) compactIfNeeded() {
	// Complexity: O(1) when no compaction (or full-drain reset), O(n) when compacting (n = active elements copied).
	// Space: O(1) without compaction; O(n) additional space during compaction allocation.
	if q == nil {
		return
	}
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
		return
	}
	if q.head >= queueCompactMinHead && q.head*2 >= len(q.items) {
		active := make([]*Order, len(q.items)-q.head)
		copy(active, q.items[q.head:])
		q.items = active
		q.head = 0
	}
}

type Heap interface {
	heap.Interface
	Push(x interface{})
	Pop() interface{}
	Peak() interface{}
}

type MinHeap []float64

func (h MinHeap) Len() int { return len(h) }

func (h MinHeap) Less(i, j int) bool { return h[i] < h[j] }

func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) { *h = append(*h, x.(float64)) }

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *MinHeap) Peak() interface{} {
	n := len(*h)
	if n == 0 {
		return nil
	}
	return (*h)[0]
}

type MaxHeap []float64

func (h MaxHeap) Len() int { return len(h) }

func (h MaxHeap) Less(i, j int) bool { return h[i] > h[j] }

func (h MaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MaxHeap) Push(x interface{}) { *h = append(*h, x.(float64)) }

func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *MaxHeap) Peak() interface{} {
	n := len(*h)
	if n == 0 {
		return nil
	}
	return (*h)[0]
}

type Orderbook struct {
	BestBid        *MaxHeap
	BestAsk        *MinHeap
	PriceToVolume  map[float64]float64
	openOrderCount int
	// indexes + containers
	PriceToBuyOrders  map[float64]*OrderQueue
	PriceToSellOrders map[float64]*OrderQueue
}

func NewOrderBook() *Orderbook {
	o := new(Orderbook)
	o.BestBid = &MaxHeap{}
	o.BestAsk = &MinHeap{}
	o.PriceToBuyOrders = make(map[float64]*OrderQueue)
	o.PriceToSellOrders = make(map[float64]*OrderQueue)
	return o
}

func (o *Orderbook) AddOrder(order *Order, orderAction string) {
	// Add order in orderbook
	// Rules:
	// - Hashmap of orders are indexes used to assess price in heaps exist
	// - Orders are added at the end of the list of orders
	price := order.Price

	if orderAction == "BUY" {
		val, ok := o.PriceToBuyOrders[price]
		if ok {
			val.Push(order)
		} else {
			heap.Push(o.BestBid, price)
			q := NewOrderQueue()
			q.Push(order)
			o.PriceToBuyOrders[price] = q
		}
		o.openOrderCount++
	}
	if orderAction == "SELL" {
		val, ok := o.PriceToSellOrders[price]
		if ok {
			val.Push(order)
		} else {
			heap.Push(o.BestAsk, price)
			q := NewOrderQueue()
			q.Push(order)
			o.PriceToSellOrders[price] = q
		}
		o.openOrderCount++
	}
}

func (o *Orderbook) decrementOpenOrderCount() {
	if o == nil {
		return
	}
	if o.openOrderCount > 0 {
		o.openOrderCount--
	}
}

func (o *Orderbook) OpenOrderCount() int {
	if o == nil {
		return 0
	}

	return o.openOrderCount
}
