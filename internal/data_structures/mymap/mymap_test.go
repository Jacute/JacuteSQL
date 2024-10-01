package mymap

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCustomMapHappyPath(t *testing.T) {
	myMap := New(5)

	myMap.Add("random_key1", "123456")
	myMap.Add("random_key2", "13232131")

	assert.Equal(t, 2, myMap.Len())
	assert.Equal(t, 5, myMap.Cap())

	myMap.Add("random_key4", "123456")
	myMap.Add("random_key4", "123456")

	assert.Equal(t, 3, myMap.Len())
	assert.Equal(t, 10, myMap.Cap())

	myMap.Add("random_key5", "123456")
	myMap.Add("random_key6", "123456")
	myMap.Add("random_key7", "123456")

	assert.Equal(t, 6, myMap.Len())
	assert.Equal(t, 20, myMap.Cap())

	assert.Equal(t, "123456", myMap.Get("random_key1"))
	assert.Equal(t, "13232131", myMap.Get("random_key2"))

	fmt.Println(myMap)
}
