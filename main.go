package main

import (
	"fmt"
	"github.com/degupta/godao/models"
	"github.com/degupta/godao/test"
)

func main() {
	fmt.Println(models.GenerateFile(test.User{}, "accounts", "./test"))
}
