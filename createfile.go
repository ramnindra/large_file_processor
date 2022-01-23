package main

import (
	"os"
	"math/rand"
	"fmt"
	"time"
	"bufio"
	"strconv"
	"strings"
)

var words [10]string = [10]string{"a", "cat", "mouse", "house", "pig", "three",
	"ten", "fire", "wolf", "cheese"}

func doLines(send chan []string) {
	s := rand.New(rand.NewSource(99))
	r1 := rand.New(s)
	for {
		n := r1.Int31n(10)
		line := make([]string, n)
		for n >= 0 {
			line = append(line, words[r1.Int31n(10)])
			n--
		}
		send <- line
	}
}

func main() {
	var gsize int = 10000000
	if len(os.Args) > 1 {
		gsize,_ = strconv.Atoi(os.Args[1])
	}

	fmt.Printf("Creating input file...")
	file, err := os.Create("inputfile")
	if err != nil {
		panic("failed to create input file")
	}
	defer file.Close()

	chanSend := make(chan []string, 1024*1024)
	workers := 8
	for workers > 0 {
		go doLines(chanSend)
		workers--
	}

	limit := 1024 * 1024

	ticker := time.NewTicker(20 * time.Second)
	w := bufio.NewWriterSize(file, limit)

	run := true
	total := 0
	for run {
		select {
		case row, ok := <-chanSend:
			if !ok {
				break
			}
			srow := strings.Join(row, " ")
			srow = srow + "\n"
			if w.Available() < len(srow) {
				err := w.Flush()
				if err != nil {
					panic("flush error")
				}
			}
			size, err := w.WriteString(srow)
			if err != nil {
				panic("short write")
			}
			total += size
			if total > gsize {
				run = false
				break
			}
		case <-ticker.C:
		}
	}
	w.Flush()
}
