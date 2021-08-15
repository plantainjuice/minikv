package test

import (
	"fmt"
	"testing"
)

func TestDefer(t *testing.T) {
	fmt.Println(1)
	{
		defer func(){
			fmt.Println(2)
		}()
	}
	fmt.Println(3)


}
