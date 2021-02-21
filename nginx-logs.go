package main

import (
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/joho/godotenv"
	"log"
	"time"
	"os"
	"strconv"
	"os/signal"
	"syscall"
)

var logString = make(chan string, 10)

func storeLogs(logs []string) {
	log.Printf("Storing %d entries\n", len(logs))
	for _, line := range logs {
		fmt.Println(line)
	}
}

func processLogs() {
	lastSync := time.Now()
	now := time.Now()
	buffer := make([]string, 0, 100)
	var s string
	flushTime, _ := strconv.ParseInt(os.Getenv("FLUSH_MS"), 10, 64)
	for {
		s = <- logString
		now = time.Now()
		buffer = append(buffer, s)
		if (lastSync.Equal(now) || now.Sub(lastSync).Milliseconds() >= flushTime) {
			storeLogs(buffer)
			buffer = buffer[:0]
			lastSync = now
		}
	}
}

func onExit() {
	fmt.Print("goodbye")
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	s := make(chan os.Signal)
	signal.Notify(s, os.Interrupt, syscall.SIGTERM)
	go func() {
		<- s
		log.Println("Exiting")
		os.Exit(0)
	}()

	go processLogs()
	t, err := tail.TailFile(os.Getenv("LOG_FILE"), tail.Config{
		Follow: true,
		ReOpen: true,
	})
	if err != nil {
		panic(err)
	}
	for line := range t.Lines {
		logString <- line.Text
	}
}
