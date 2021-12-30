package main

import (
  "fmt"
  "os"
  "net"
  "strings"
  "bufio"
  "sync"
)

const ARG_NUM_CLIENT int = 3

var wg sync.WaitGroup
var conn net.Conn
var err error

func handle_transaction() {
  reader := bufio.NewReader(os.Stdin)
  for {
    text, _ := reader.ReadString('\n')
    fmt.Fprintf(conn, "%s", text)
  }
}

func main()  {
  argv := os.Args[1:]
  if (len(argv) != ARG_NUM_CLIENT) {
    fmt.Fprintf(os.Stderr, "usage: ./node <NODE_NAME> <SERV_ADDR> <SERV_PORT>\n")
    os.Exit(1)
  }

  node_name := argv[0]
  serv_addr := argv[1]
  serv_port := argv[2]

  conn, err = net.Dial("tcp", strings.Join([]string{serv_addr, serv_port}, ":"))

  if err != nil {
   fmt.Fprintf(os.Stderr, "%s\n", err)
   os.Exit(1)
  }

  fmt.Fprintf(conn, "%s\n", node_name)

  wg.Add(1)
  go handle_transaction()
  wg.Wait()
}
