package main

import (
	"bytes"
	"fmt"
)

func main() {
	start := "AgAAAAAAAAABkxJqaW00Njc2MjM1"

	end := "AQAAAAAAAAAB+QJi3Cs="

	key := "AQAAAAAAAAAB+QJmL/g="

	fmt.Println(bytes.Compare([]byte(key), []byte(start)))
	fmt.Println(bytes.Compare([]byte(key), []byte(end)))
}
