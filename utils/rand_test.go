package utils

import (
	"fmt"
	"testing"
)

func TestRandomString(t *testing.T) {
	result := RandomString(5)
	fmt.Println(result)
}
