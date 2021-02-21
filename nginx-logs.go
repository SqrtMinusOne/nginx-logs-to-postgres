package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"text/template"
	"time"

	"github.com/hpcloud/tail"
	"github.com/jackc/pgx/v4"
	"github.com/joho/godotenv"
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
		s = <-logString
		now = time.Now()
		buffer = append(buffer, s)
		if lastSync.Equal(now) || now.Sub(lastSync).Milliseconds() >= flushTime {
			storeLogs(buffer)
			buffer = buffer[:0]
			lastSync = now
		}
	}
}

func initDb(conn *pgx.Conn) {
	tmpl, err := template.ParseFiles("nginx-table.sql")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec(context.Background(), fmt.Sprintf("create schema if not exists \"%s\" ", os.Getenv("TABLE_SCHEMA")))
	
	if err != nil {
		panic(err)
	}
	buf := &bytes.Buffer{}
	type InitParams struct {
		Schema string
		Table  string
	}
	err = tmpl.ExecuteTemplate(buf, "nginx-table.sql", InitParams{Schema: os.Getenv("TABLE_SCHEMA"), Table: os.Getenv("TABLE_NAME")})
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec(context.Background(), buf.String())
	if (err != nil) {
		panic(err)
	}
	log.Println("Database initialized")
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	initDb(conn)

	s := make(chan os.Signal)
	signal.Notify(s, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-s
		err := conn.Close(context.Background())
		if err != nil {
			log.Fatalf("Unable to connect disconnect from database: %v\n", err)
		}
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
