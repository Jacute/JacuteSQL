package mymap

import (
	"JacuteSQL/internal/data_structures/mysl"
	"errors"
	"fmt"
	"regexp"
)

var (
	DefaultCapacity   = 16
	DefaultLoadFactor = 50
	DefaultHashFunc   = djb2
)

var (
	jsonRegexp = regexp.MustCompile(`"(.+)": \[(.+?)\]`)
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// KeyVal stores key-value pair of map entity
type KeyVal struct {
	Key   string
	Value interface{}
}

// CustomMap is a custom realisation of hash table
type CustomMap struct {
	// Total capacity of HashTable
	BucketSize int
	// Current number of filled buckets
	FilledSize int
	// Data, which stored in array for compensate collisons
	Buckets [][]KeyVal

	// Percentage at which you need extend the capacity and rehash.
	//
	// (FilledSize * 100) / BucketSize >= LoadFactor => double capacity and rehash
	LoadFactor int

	// Function to hash key
	HashFunc func(string) uint32
}

// New creates a new CustomMap
//
// New(capacity int, loadFactor int)
//
// default capacity = 16, default load factor = 50.
func New(params ...int) *CustomMap {
	// If provided parameters, use them to set capacity and load factor.
	if len(params) > 0 {
		DefaultCapacity = params[0]
	}
	if len(params) > 1 {
		DefaultLoadFactor = params[1]
	}
	return &CustomMap{
		BucketSize: DefaultCapacity,
		FilledSize: 0,
		Buckets:    make([][]KeyVal, DefaultCapacity),
		LoadFactor: DefaultLoadFactor,
		HashFunc:   DefaultHashFunc,
	}
}

func (cm *CustomMap) Len() int {
	return cm.FilledSize
}

func (cm *CustomMap) Cap() int {
	return cm.BucketSize
}

func (cm *CustomMap) Add(key string, value interface{}) {
	if cm.FilledSize*100/cm.BucketSize >= cm.LoadFactor {
		cm.rehash()
	}
	h1 := int(cm.HashFunc(key) % uint32(cm.BucketSize))
	if len(cm.Buckets[h1]) == 0 {
		cm.Buckets[h1] = []KeyVal{
			{Key: key, Value: value},
		}
	} else {
		for i := range cm.Buckets[h1] {
			if cm.Buckets[h1][i].Key == key {
				cm.Buckets[h1][i].Value = value
				return
			}
		}
		cm.Buckets[h1] = append(cm.Buckets[h1], KeyVal{Key: key, Value: value})
	}
	cm.FilledSize++
}

func (cm *CustomMap) rehash() {
	oldBuckets := cm.Buckets
	cm.BucketSize *= 2
	cm.Buckets = make([][]KeyVal, cm.BucketSize)
	cm.FilledSize = 0
	for _, bucketArr := range oldBuckets {
		for _, bucket := range bucketArr {
			cm.Add(bucket.Key, bucket.Value)
		}
	}
}

func (cm *CustomMap) Get(key string) interface{} {
	h1 := int(cm.HashFunc(key) % uint32(cm.BucketSize))
	for _, bucket := range cm.Buckets[h1] {
		if bucket.Key == key {
			return bucket.Value
		}
	}
	return nil
}

func (cm *CustomMap) Delete(key string) error {
	h1 := int(cm.HashFunc(key) % uint32(cm.BucketSize))
	for _, bucket := range cm.Buckets[h1] {
		if bucket.Key == key {
			cm.Buckets[h1] = nil
			return nil
		}
	}
	return ErrKeyNotFound
}

func (cm *CustomMap) Keys() *mysl.MySl[string] {
	keys := mysl.New[string]()
	for _, bucketArr := range cm.Buckets {
		for _, bucket := range bucketArr {
			keys.Append(bucket.Key)
		}
	}
	return keys
}

// UnmarshalJSON unmarshal json string: []string -> CustomMap
//
// This impementation only works for "key": ["value1", "value2", "value3"]
func (cm *CustomMap) UnmarshalJSON(data []byte) error {
	if cm.BucketSize == 0 {
		cm.BucketSize = DefaultCapacity
		cm.LoadFactor = DefaultLoadFactor
		cm.Buckets = make([][]KeyVal, cm.BucketSize)
		cm.HashFunc = djb2
	}

	matches := jsonRegexp.FindAllSubmatch(data, -1)
	for _, match := range matches {
		if len(match) == 3 {
			key := string(match[1])
			valuesStr := string(match[2])
			values := regexp.MustCompile(`\s*"(.*?)"\s*`).FindAllStringSubmatch(valuesStr, -1)
			var valueList []string
			for _, v := range values {
				if len(v) > 1 {
					valueList = append(valueList, v[1])
				}
			}
			// Добавляем в CustomMap
			cm.Add(key, valueList)
		}
	}
	return nil
}

func (cm CustomMap) String() string {
	output := ""
	for _, bucketArr := range cm.Buckets {
		for _, bucket := range bucketArr {
			output += fmt.Sprintf("%s - %v\n", bucket.Key, bucket.Value)
		}
	}
	return output
}
