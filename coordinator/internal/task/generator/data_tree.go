package generator

import (
	"github.com/emirpasic/gods/v2/trees/redblacktree"
)

type Entry struct {
	Key   string
	Value string
}

type DataTree struct {
	data *redblacktree.Tree[string, string]
}

func (d *DataTree) Put(key string, value string) {
	d.data.Put(key, value)
}
func (d *DataTree) Get(key string) (string, bool) {
	return d.data.Get(key)
}

func (d *DataTree) GetFloor(key string) (*Entry, bool) {
	node, found := d.data.Floor(key)
	if found {
		return &Entry{
			Key:   node.Key,
			Value: node.Value,
		}, true
	}
	return nil, found
}

func (d *DataTree) GetCeiling(key string) (*Entry, bool) {
	node, found := d.data.Ceiling(key)
	if found {
		return &Entry{
			Key:   node.Key,
			Value: node.Value,
		}, true
	}
	return nil, found
}

func (d *DataTree) GetHigher(key string) (*Entry, bool) {
	node, found := d.data.Ceiling(key)
	if !found {
		return nil, false
	}
	if node.Key == key {
		iterator := d.data.IteratorAt(node)
		if iterator.Next() {
			return &Entry{
				Key:   iterator.Key(),
				Value: iterator.Value(),
			}, true
		}
		return nil, false
	}
	return &Entry{
		Key:   node.Key,
		Value: node.Value,
	}, true
}

func (d *DataTree) GetLower(key string) (*Entry, bool) {
	node, found := d.data.Floor(key)
	if !found {
		return nil, false
	}
	if node.Key == key {
		iterator := d.data.IteratorAt(node)
		if iterator.Prev() {
			return &Entry{
				Key:   iterator.Key(),
				Value: iterator.Value(),
			}, true
		}
		return nil, false
	}
	return &Entry{
		Key:   node.Key,
		Value: node.Value,
	}, true
}

func (d *DataTree) Delete(key string) {
	d.data.Remove(key)
}

func (d *DataTree) DeleteRange(keyStart string, keyEnd string) {
	for {
		node, found := d.data.Ceiling(keyStart)
		if !found {
			break
		}
		if d.data.Comparator(node.Key, keyEnd) >= 0 {
			break
		}
		d.data.Remove(node.Key)
	}
}

func (d *DataTree) List(keyStart, keyEnd string) []string {
	node, found := d.data.Ceiling(keyStart)
	if !found {
		return nil
	}

	var keys []string
	it := d.data.IteratorAt(node)
	comp := d.data.Comparator

	for {
		currKey := it.Key()
		if comp(currKey, keyEnd) >= 0 {
			break
		}
		keys = append(keys, currKey)
		if !it.Next() {
			break
		}
	}
	return keys
}

func (d *DataTree) RangeScan(keyStart, keyEnd string) []*Entry {
	node, found := d.data.Ceiling(keyStart)
	if !found {
		return nil
	}

	var records []*Entry
	it := d.data.IteratorAt(node)
	comp := d.data.Comparator

	for {
		currKey := it.Key()
		if comp(currKey, keyEnd) >= 0 {
			break
		}
		currentValue := it.Value()

		records = append(records, &Entry{
			Key:   currKey,
			Value: currentValue,
		})
		if !it.Next() {
			break
		}
	}
	return records
}

func NewDataTree() *DataTree {
	tree := redblacktree.New[string, string]()
	return &DataTree{
		data: tree,
	}
}
