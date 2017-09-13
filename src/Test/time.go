package main

import (
	"fmt"
	"time"
)


func main() {

	d := 10 * time.Second
	ch := make(chan struct{})
	go watch(d, ch)
	time.Sleep(5*time.Second)
	fmt.Println("sending")
	ch <- struct{}{}
	time.Sleep(20*time.Second)
}

func watch(d time.Duration, ch chan struct{}) {
	tmr := time.NewTimer(d)
	for {
		select {
		case <-tmr.C:
			fmt.Println("timeout")

		case <-ch:
			fmt.Println("time add more")
			tmr.Reset(d)
		}
		fmt.Println("outside select executed!")
	}

}
