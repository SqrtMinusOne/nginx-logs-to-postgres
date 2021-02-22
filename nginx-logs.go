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
	"regexp"
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

var hexRegex = regexp.MustCompile(`\\x[0-9A-F]{2}`)

func parseLine(line string) ([]interface{}, time.Time) {
	var datum map[string]interface{}
	line = hexRegex.ReplaceAllString(line, "?")
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
	return parsed, reqTime
}

func storeLogs(logs []string, conn *pgx.Conn, lastEntryTime time.Time) {
	rows := make([][]interface{}, 0, len(logs))
	for _, line := range logs {
		parsed, entryTime := parseLine(line)
		if entryTime.After(lastEntryTime) {
			rows = append(rows, parsed)
		}
	}
	if len(rows) > 0 {
		_, err := conn.CopyFrom(
			context.Background(),
			pgx.Identifier{os.Getenv("TABLE_SCHEMA"), os.Getenv("TABLE_NAME")},
			[]string{"time_local", "path", "ip", "server_name", "remote_user", "remote_port", "time", "user_agent", "user_id_got", "user_id_set", "request", "status", "body_bytes_sent", "request_time", "request_method", "geoip_country_code", "http_referrer"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			fmt.Printf("%v\n", rows)
			panic(err)
		}
		log.Printf("Stored %d entries\n", len(rows))
	}
	if len(rows) != len(logs) {
		log.Printf("Skipped %d entries\n", len(logs) - len(rows))
	}
}

func processLogs(conn *pgx.Conn) {
	lastSync := time.Now()
	now := time.Now()
	buffer := make([]string, 0, 100)
	var s string
	flushTime, _ := strconv.ParseInt(os.Getenv("FLUSH_MS"), 10, 64)

	var lastEntryString string
	var lastEntryTime time.Time
	err := conn.QueryRow(
		context.Background(),
		fmt.Sprintf("SELECT MAX(time)::text max FROM \"%s\".\"%s\"", os.Getenv("TABLE_SCHEMA"), os.Getenv("TABLE_NAME")),
	).Scan(&lastEntryString)
	if err != nil {
		log.Println("First time sync")
	} else {
		log.Printf("Last entry time: %s", lastEntryString)
		lastEntryTime, err = time.Parse("2006-01-02 15:04:05", lastEntryString)
		if err != nil {
			panic(err)
		}
	}

	for {
		s = <-logString
		now = time.Now()
		buffer = append(buffer, s)
		if now.Sub(lastSync).Milliseconds() >= flushTime {
			storeLogs(buffer, conn, lastEntryTime)
			buffer = buffer[:0]
			lastSync = now
		}
	}
}

var createTemplate = `
create table if not exists "{{.Schema}}"."{{.Table}}"
(
	id serial
		constraint nginx_pk
			primary key,
	time_local varchar(256),
	path text,
	ip varchar(256),
	remote_user varchar(256),
	remote_port int,
    time timestamp,
	user_agent text,
	user_id_got text,
	user_id_set text,
	request text,
	status int,
	body_bytes_sent int,
	request_time float,
	request_method varchar(128),
	server_name varchar(256),
	geoip_country_code varchar(256),
	http_referrer text
);
`

func initDb(conn *pgx.Conn) {
	tmpl, err := template.New("nginx-table.sql").Parse(createTemplate)
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
