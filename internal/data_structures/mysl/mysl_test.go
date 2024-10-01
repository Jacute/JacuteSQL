package mysl

import (
	"fmt"
	"testing"

	"math/rand"

	"github.com/stretchr/testify/assert"
)

func GenerateRandomSlice(n int, max int) []int {
	result := make([]int, n)
	for i := 0; i < n; i++ {
		result[i] = rand.Intn(max)
	}
	return result
}

func TestMySlHappyPathAppendGet(t *testing.T) {
	t.Parallel()

	cases := []struct {
		TestName string
		Count    int
		Want     string
	}{
		{TestName: "Test 0-4", Count: 5, Want: "[0 1 2 3 4]"},
		{TestName: "Test 0-9", Count: 10, Want: "[0 1 2 3 4 5 6 7 8 9]"},
	}

	for _, c := range cases {
		t.Run(c.TestName, func(tt *testing.T) {
			mySlice := New[int]()
			for i := 0; i < c.Count; i++ {
				mySlice.Append(i)
			}
			got := mySlice.String()
			assert.Equal(tt, c.Want, got)

			first := mySlice.Get(0)
			assert.Equal(tt, 0, first)

			last := mySlice.Get(c.Count - 1)
			assert.Equal(tt, c.Count-1, last)
		})
	}
}

func TestMySlHappyPathSetDelete(t *testing.T) {
	cases := []struct {
		TestName    string
		Data        []int
		SetIndex    int
		SetValue    int
		DeleteIndex int
		Want        string
	}{
		{
			TestName:    "Test Set 2 Delete 2",
			Data:        []int{0, 1, 2, 3, 4},
			SetIndex:    2,
			SetValue:    54,
			DeleteIndex: 2,
			Want:        "[0 1 3 4]",
		},
		{
			TestName: "Test Set 0 Delete 4",
			Data:     []int{0, 1, 2, 3, 4},
			SetIndex: 0, SetValue: 1111,
			DeleteIndex: 4,
			Want:        "[1111 1 2 3]",
		},
	}

	for _, c := range cases {
		t.Run(c.TestName, func(tt *testing.T) {
			mySlice := New[int]()
			for _, v := range c.Data {
				mySlice.Append(v)
			}

			// Set test
			oldValue := mySlice.Get(c.SetIndex)
			mySlice.Set(c.SetIndex, c.SetValue)
			newValue := mySlice.Get(c.SetIndex)

			assert.NotEqual(tt, newValue, oldValue)

			// Delete test
			oldValue = mySlice.Get(c.DeleteIndex)

			mySlice.Delete(c.DeleteIndex)

			newValue = mySlice.Get(c.SetIndex)
			assert.NotEqual(tt, oldValue, newValue)

			output := mySlice.String()
			fmt.Println(mySlice.data)
			assert.Equal(tt, c.Want, output)
		})
	}
}

func TestMySlInsert(t *testing.T) {
	mySlice := New[int]()
	mySlice.Append(5)
	mySlice.Append(5)
	mySlice.Append(5)
	assert.Equal(t, mySlice.String(), "[5 5 5]")
	mySlice.Insert(6, 1)
	assert.Equal(t, mySlice.String(), "[5 6 5 5]")
}

func TestMySlDoubleSl(t *testing.T) {
	mySlice := New[MySl[string]]()
	mySliceInternal1 := New[string]()
	mySliceInternal2 := New[string]()
	mySliceInternal1.Append("aboba1")
	mySliceInternal1.Append("aboba2")
	mySliceInternal2.Append("aboba3")
	mySliceInternal2.Append("aboba4")
	mySlice.Append(*mySliceInternal1)
	mySlice.Append(*mySliceInternal2)
	assert.Equal(t, 2, mySlice.Get(0).length)
	assert.Equal(t, 2, mySlice.Get(1).length)
}

// func TestMySlDeleteEmpty(t *testing.T) {
// 	mySlice := New[int](16)
// 	err := mySlice.Delete(0)
// 	assert.ErrorIs(t, err, ErrEmptySlice)
// }
