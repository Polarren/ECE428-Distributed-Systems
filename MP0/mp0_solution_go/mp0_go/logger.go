package main

import (
  "fmt"
  "os"
  "net"
  "bufio"
  "time"
)

const ARG_NUM_SERVER int = 1

func handle_err(err error) {
  if err != nil {
    fmt.Fprintf(os.Stderr, "%s\n", err)
    os.Exit(1)
  }
}

func handle_receive(conn net.Conn) {
  reader := bufio.NewReader(conn)
  node_name, _ := reader.ReadString('\n')
  node_name = node_name[:len(node_name)-1]
	timeinmicros := time.Now().UnixNano() / 1000
	timeinsecs := float64(timeinmicros) / 1000000
  fmt.Fprintf(os.Stdout, "%.6f - %s connected\n", timeinsecs, node_name)

  for {
    line, err := reader.ReadString('\n')
		timeinmicros = time.Now().UnixNano() / 1000
		timeinsecs = float64(timeinmicros) / 1000000
    if err != nil {
	     fmt.Fprintf(os.Stdout, "%.6f - %s disconnected\n", timeinsecs, node_name)
	     return
    }
    transaction_time := ""
    transaction_hash := ""
    fmt.Sscanf(line, "%s %s", &transaction_time, &transaction_hash)
    fmt.Fprintf(os.Stdout, "%s %s %s\n", transaction_time, node_name, transaction_hash)
  }
}

func main()  {
  argv := os.Args[1:]
  if (len(argv) != ARG_NUM_SERVER) {
    fmt.Fprintf(os.Stderr, "usage: ./logger <SERV_PORT>\n")
    os.Exit(1)
  }

  serv_port := ":" + argv[0]

  ln, err := net.Listen("tcp", serv_port)

  handle_err(err)

  defer ln.Close()

  for {
    conn, err := ln.Accept()
    handle_err(err)
    go handle_receive(conn)
  }
}
