package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
)

// Function for setting up client communication channel.
func setup_channel(ID string, split_line []string) net.Conn {
	// Constructing address from arguments and establishing connection conn
	servAddr := split_line[1] + ":" + split_line[2]
	tcpAddr, _ := net.ResolveTCPAddr("tcp", servAddr)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	// Loop to connection is tried until it works.
	for err != nil {
		conn, err = net.DialTCP("tcp", nil, tcpAddr)
	}
	conn.Write([]byte(ID + "\n"))

	return conn

}

func main() {
	// Parse command line args to get node name and establish connection
	args := os.Args[1:]
	ID := args[0]
	config := args[1]

	// Read config file.
	file, _ := os.Open(config)
	// Line by line read.
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string
	for scanner.Scan() {
		a := scanner.Text()
		text = append(text, a)
	}
	file.Close()

	// Array of connections to randomly choose from
	// Bufio's for wiating for responses
	connections := make([]net.Conn, len(text))
	bufReaders := make([]*bufio.Reader, len(text))

	// Make connections and store in array.
	for i := range text {
		line := string(text[i])
		split_line := strings.Split(line, " ")
		conn := setup_channel(ID, split_line)
		connections[i] = conn
		bufReaders[i] = bufio.NewReader(conn)
	}

	// In transaction flag for ignoring commands when not in transaction
	in_transaction := false
	// Set server_num to -1 by default
	server_num := -1
	// Read from stdin
	reader := bufio.NewReader(os.Stdin)
	for {
		user_in, _ := reader.ReadString('\n')
		trimmed := strings.TrimSuffix(user_in, "\n")
		if len(trimmed) == 0 {
			continue
		}

		// Split array based on whitespace and get transaction type
		transaction_type := strings.Fields(trimmed)[0]
		// Based on trimmed user input, decide appropriate action
		switch transaction_type {

		case "BEGIN":
			if in_transaction {
				continue
			}
			in_transaction = true
			server_num = rand.Intn(len(connections))

			conn := connections[server_num]
			msg := string(ID) + " " + user_in
			conn.Write([]byte(msg))

			// Wait for response
			bufReader := bufReaders[server_num]
			resp, err := bufReader.ReadString('\n')
			if err == nil {
				fmt.Print(resp)
				if strings.Compare(resp[:len(resp)-1], "OK") != 0 {
					in_transaction = false
				}
			}

		// If DEPOSIT, check if in transaction, if so, send message and wait for response.
		case "DEPOSIT":
			if !in_transaction {
				continue
			}
			conn := connections[server_num]
			msg := string(ID) + " " + user_in
			conn.Write([]byte(msg))

			// Wait for response
			bufReader := bufReaders[server_num]
			resp, err := bufReader.ReadString('\n')
			if err == nil {
				fmt.Print(resp)
				if strings.Compare(resp[:len(resp)-1], "OK") != 0 {
					in_transaction = false
				}
			}

		case "WITHDRAW":
			if !in_transaction {
				continue
			}
			conn := connections[server_num]
			msg := string(ID) + " " + user_in
			conn.Write([]byte(msg))

			// Wait for response
			bufReader := bufReaders[server_num]
			resp, err := bufReader.ReadString('\n')
			if err == nil {
				fmt.Print(resp)
				if strings.Compare(resp[:len(resp)-1], "OK") != 0 {
					in_transaction = false
				}
			}

		case "BALANCE":
			if !in_transaction {
				continue
			}
			conn := connections[server_num]
			msg := string(ID) + " " + user_in
			conn.Write([]byte(msg))
			// Wait for response
			bufReader := bufReaders[server_num]
			resp, err := bufReader.ReadString('\n')
			if err == nil {

				fmt.Print(resp)
				if strings.Compare(resp[:len(resp)-1], "NOT FOUND, ABORTED") == 0 {
					in_transaction = false
				}
			}

		case "COMMIT":
			if !in_transaction {

				continue
			}
			conn := connections[server_num]
			msg := string(ID) + " " + user_in
			conn.Write([]byte(msg))
			bufReader := bufReaders[server_num]
			resp, err := bufReader.ReadString('\n')
			if err == nil {
				fmt.Print(resp)
				in_transaction = false
			}

		default:
		}

	}
}
