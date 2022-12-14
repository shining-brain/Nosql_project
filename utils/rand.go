package utils

import (
	"math/rand"
	"time"
)

// RandInt 生成最大为max的随机数
func RandInt(max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max)
}

// RandomString 返回n长度的随机字符串
func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[RandInt(len(letters))]
	}

	return string(b)
}
