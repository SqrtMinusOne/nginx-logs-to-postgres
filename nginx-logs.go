package main

import (
	"bytes"
	"context"
	"encoding/json"
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

func jsonGetInt(val interface{}) interface{} {
	if val == nil || val == "-" {
		return nil
	}
	res, err := strconv.ParseInt(val.(string), 10, 64)
	if err != nil {
		panic(err)
	}
	return res
}

func jsonGetFloat(val interface{}) interface{} {
	if val == nil || val == "-" {
		return nil
	}
	res, err := strconv.ParseFloat(val.(string), 10)
	if err != nil {
		panic(err)
	}
	return res
}

func jsonGetUid(val interface{}) interface{} {
	if val == nil || val == "-" {
		return nil
	}
	s := val.(string)
	return s[4:]
}

func parseLine(line string) ([]interface{}, time.Time) {
	var datum map[string]interface{}
	err := json.Unmarshal([]byte(line), &datum)
	if err != nil {
		panic(err)
	}
	reqTime, err := time.Parse(time.RFC3339, datum["time"].(string))
	if err != nil {
		panic(err)
	}
	parsed := []interface{}{
		datum["time_local"],
		datum["path"],
		datum["ip"],
		datum["server_name"],
		datum["remote_user"],
		jsonGetInt(datum["remote_port"]),
		reqTime,
		datum["user_agent"],
		jsonGetUid(datum["user_id_got"]),
		jsonGetUid(datum["user_id_set"]),
		datum["request"],
		jsonGetInt(datum["status"]),
		jsonGetInt(datum["body_bytes_sent"]),
		jsonGetFloat(datum["request_time"]),
		datum["request_method"],
		datum["geoip_country_code"],
		datum["http_referrer"],
	}
	for i, v := range parsed {
		if v == "-" {
			parsed[i] = nil
		}
	}
	return parsed, time.Now()
}

func storeLogs(logs []string, conn *pgx.Conn) {
	rows := make([][]interface{}, 0, len(logs))
	for _, line := range logs {
		fmt.Println(line)
		parsed, _ := parseLine(line)
		rows = append(rows, parsed)
	}
	if len(rows) > 0 {
		_, err := conn.CopyFrom(
			context.Background(),
			pgx.Identifier{os.Getenv("TABLE_SCHEMA"), os.Getenv("TABLE_NAME")},
			[]string{"time_local", "path", "ip", "server_name", "remote_user", "remote_port", "time", "user_agent", "user_id_got", "user_id_set", "request", "status",  "body_bytes_sent", "request_time", "request_method", "geoip_country_code", "http_referrer"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			fmt.Printf("%v\n", rows)
			panic(err)
		}
		log.Printf("Stored %d entries\n", len(rows))
	}
}

func processLogs(conn *pgx.Conn) {
	lastSync := time.Now()
	now := time.Now()
	buffer := make([]string, 0, 100)
	var s string
	flushTime, _ := strconv.ParseInt(os.Getenv("FLUSH_MS"), 10, 64)
	for {
		s = <-logString
		now = time.Now()
		buffer = append(buffer, s)
		if now.Sub(lastSync).Milliseconds() >= flushTime || now.Sub(lastSync).Milliseconds() < 100 {
			storeLogs(buffer, conn)
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
	if err != nil {
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

	go processLogs(conn)
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
