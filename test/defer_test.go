package test

import (
	"fmt"
	"testing"
)

func TestDefer(t *testing.T) {

	type T struct{
		TStr string
	}

	A := &T{"dfd"}

	B := *A

	C := A

	B.TStr = "f"

	fmt.Println(A.TStr)
	C.TStr = "change"
	fmt.Println(A)



	fmt.Println(1)
	{
		defer func(){
			fmt.Println(2)
		}()
	}
	fmt.Println(3)


}
