package main

import (
	"fmt"
	"sync"
	"time"
)

var m *sync.Mutex

func main() {
	//声明
	var mutex sync.Mutex
	fmt.Println("Lock the lock. (G0)")
	//加锁mutex
	mutex.Lock()

	fmt.Println("The lock is locked.(G0)")
	for i := 1; i < 4; i++ {
		go func(i int) {
			fmt.Printf("Lock the lock. (G%d)\n", i)
			mutex.Lock()
			fmt.Printf("The lock is locked. (G%d)\n", i)
		}(i)
	}
	//休息一会,等待打印结果
	time.Sleep(time.Second)
	fmt.Println("Unlock the lock. (G0)")
	//解锁mutex
	mutex.Unlock()

	fmt.Println("The lock is unlocked. (G0)")
	//休息一会,等待打印结果

	time.Sleep(time.Second)


	fmt.Println("-------------------------------------------------------------------------------------------------------")


	m = new(sync.Mutex)

	go lockPrint(1)

	lockPrint(2)


	time.Sleep(time.Second)



	fmt.Printf("%s\n", "exit!")



}

func lockPrint(i int) {

	println(i, "lock start")



	m.Lock()                       // 互斥锁保证了 Lock 和 Unlock之间的代码不能被多个goroutine同时调用



	println(i, "in lock")

	time.Sleep(3 * time.Second)



	m.Unlock()                     //

	println(i, "unlock")

}