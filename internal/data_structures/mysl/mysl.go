package mysl

import (
	"errors"
	"fmt"
)

var (
	ErrIndexOutOfRange = errors.New("index out of range")
	ErrEmptySlice      = errors.New("slice is empty")
)

var (
	DefaultCapacity   = 16
	DefaultLoadFactor = 50
)

type MySl[T any] struct {
	capacity int
	length   int
	data     []T
	// Precentage at which you need Extend the capacity.
	//
	// (Len * 100) / Capacity >= LoadFactor => double capacity
	loadFactor int
}

// NewMySl creates a new MySl with given capacity and load factor.
//
// NewMySl(capacity int, loadFactor int)
//
// default capacity = 16, default load factor = 50
func New[T any](params ...int) *MySl[T] {
	// If provided parameters, use them to set capacity and load factor.
	if len(params) > 0 {
		DefaultCapacity = params[0]
	}
	if len(params) > 1 {
		DefaultLoadFactor = params[1]
	}

	return &MySl[T]{
		data:       make([]T, DefaultCapacity),
		capacity:   DefaultCapacity,
		length:     0,
		loadFactor: DefaultLoadFactor,
	}
}

// Len returns the length of the MySl
func (cs *MySl[T]) Len() int {
	return cs.length
}

// Cap returns the capacity of the MySl
func (cs *MySl[T]) Cap() int {
	return cs.capacity
}

func (cs *MySl[T]) Resize(newSize int) {
	if newSize > cs.capacity {
		cs.Extend(newSize)
	}
	cs.length = newSize
}

func (cs *MySl[T]) GetData() []T {
	data := make([]T, cs.length)
	for i := 0; i < cs.length; i++ {
		data[i] = cs.data[i]
	}
	return data
}

// Append add a value to the MySl
func (cs *MySl[T]) Append(value T) {
	if cs.length*100/cs.capacity >= cs.loadFactor {
		cs.Extend(cs.capacity * 2)
	}
	cs.data[cs.length] = value
	cs.length++
}

func (cs *MySl[T]) Insert(value T, index int) {
	if index < 0 || index >= cs.length {
		panic(ErrIndexOutOfRange)
	}
	if cs.length*100/cs.capacity >= cs.loadFactor {
		cs.Extend(cs.capacity * 2)
	}
	for i := cs.length - 1; i >= index; i-- {
		cs.data[i+1] = cs.data[i]
	}
	cs.data[index] = value
	cs.length++
}

// Double Extend the MySl
func (cs *MySl[T]) Extend(newCapacity int) {
	newData := make([]T, newCapacity)

	for i := 0; i < cs.length; i++ {
		newData[i] = cs.data[i]
	}

	cs.data = newData
	cs.capacity = newCapacity
}

// Get returns the element by index in the MySl
func (cs *MySl[T]) Get(index int) T {
	if index < 0 || index >= cs.length {
		panic(ErrIndexOutOfRange)
	}
	return cs.data[index]
}

// Set change the element by index in the MySl
func (cs *MySl[T]) Set(index int, value T) {
	if index < 0 || index >= cs.length {
		panic(ErrIndexOutOfRange)
	}
	cs.data[index] = value
}

// Delete removes the element by index from the MySl
func (cs *MySl[T]) Delete(index int) {
	if cs.length == 0 {
		panic(ErrEmptySlice)
	}
	if index < 0 || index >= cs.length {
		panic(ErrIndexOutOfRange)
	}

	newData := make([]T, cs.capacity)
	for i := 0; i < index; i++ {
		newData[i] = cs.data[i]
	}
	for i := index; i < cs.length-1; i++ {
		newData[i] = cs.data[i+1]
	}
	cs.data = newData
	cs.length--
}

func (cs *MySl[T]) String() string {
	output := "["
	for i := 0; i < cs.length; i++ {
		output += fmt.Sprintf("%v", cs.data[i])
		if i != cs.length-1 {
			output += " "
		}
	}
	output += "]"
	return output
}
