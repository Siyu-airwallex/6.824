package main

import ("fmt")


var sharedVar = "Shared Variable"

var (
	a = 5
	b = 6
	c = 10
)

func main() {


	for i:=0; i<100; i++ {
		fmt.Println(i)
	}

	slice := []string{"fengsiyu","taoshuang"}

	slice = remove(slice, "fengsiyu")
	fmt.Println(slice)


	test := 5
	zero(&test)
	fmt.Println(test)

	fmt.Println(*(&test))

	xPtr := new(int)
	zero(xPtr)
	fmt.Println(*xPtr)



}


func zero(xPtr *int){
	*xPtr = 1000000;
}



func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}