package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Printf("Go is working in WSL at %s\n", time.Now().Format(time.RFC3339))
}
