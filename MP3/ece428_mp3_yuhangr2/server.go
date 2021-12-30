package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Struct for keeping track of shared variables between goroutines
//
type server struct {
	branch            string // Branch string from cmd line args
	port              string
	num_nodes         int       // Number of server nodes in the network
	timestamp         int       // Latest transaction number for assigning incoming transactions
	latest_commit     int       // Last committed entry
	latest_finished   int       // The latest one committed or aborted
	num_votes         int       // Number of votes received during a 2PC
	VoteChannel       chan bool // Vote Channel for signaling that all votes have been received
	in_prepare        bool      // In prepare state for ignoring any votes that come
	prepare_timestamp int
	config_text       []string

	server_to_addr     map[string]string   // Map for server addresses
	server_to_port     map[string]string   // Map for server ports
	accounts           map[string]int      // Accounts for the branch
	branch_connections map[string]net.Conn // Map for looking up branches to connections
	client_connections map[int]net.Conn    // Map for client connections
	timestamp_map      map[int]int         // Map for looking up what a client's transaction timestamp is
	commit_map         map[string]int
	write_map          map[int][]string
	RTS                map[string][]int
	TW                 map[string]map[int]int

	commit_cond    *sync.Cond // Cond for waking up goroutines whenever there is a commit
	network_mutex  sync.Mutex // Mutex for accessing communication channels
	accounts_mutex sync.Mutex // Mutex for accessing accounts
	msg_mutex      sync.Mutex // Mutex for timestamp/other message related syncs
	commit_mutex   sync.Mutex // Mutex for reading + broadcast
}

// Global acccess for different goroutines
var self server

// Function to initialize the server
func server_setup() {
	// creating maps/setting defaults
	self.num_votes = 0
	self.timestamp = 0
	self.latest_commit = 0
	self.latest_finished = 0
	self.in_prepare = false
	self.VoteChannel = make(chan bool)
	self.server_to_addr = make(map[string]string)
	self.server_to_port = make(map[string]string)
	self.branch_connections = make(map[string]net.Conn)
	self.client_connections = make(map[int]net.Conn)
	self.timestamp_map = make(map[int]int)
	self.accounts = make(map[string]int)
	self.RTS = make(map[string][]int)
	self.TW = make(map[string]map[int]int)
	self.commit_cond = sync.NewCond(&self.commit_mutex)
	self.commit_map = make(map[string]int)
	self.write_map = make(map[int][]string)

}

// Function for setting up hadling connections.
func connection_handler(c net.Conn) {
	defer c.Close()
	// create Buffer Reader, node name set
	bufReader := bufio.NewReader(c)
	// Get name and print time when connection made.
	bytes, err2 := bufReader.ReadBytes('\n')
	if err2 != nil {
		c.Close()
		return
	}
	trimmed_bytes := string(bytes)[:len(bytes)-1]
	conn_name := trimmed_bytes

	ID, err := strconv.Atoi(trimmed_bytes)
	isClient := (err == nil)

	if isClient {
		// If client, store connections so that messages can be sent directly to client outside of this goroutine if necessary
		// fmt.Printf("Client %s connected - %f\n", trimmed_bytes, now)
		self.client_connections[ID] = c
	} else {
		// fmt.Printf("Node %s connected - %f\n", trimmed_bytes, now)
	}

	// Read messages forever
	for {
		msg, err1 := bufReader.ReadString('\n')
		if err1 != nil {
			// fmt.Println("Error in reading message.")
			self.network_mutex.Lock()
			c.Write([]byte("Couldn't Read message.\n"))
			self.network_mutex.Unlock()
			return
		}
		go func(msg string) {
			// err is nil => we are taking messages from clients so process message
			if isClient {
				process_client_req(msg)
			} else {
				// If reply from branch to coordinator, fwd message back to client
				split := strings.Fields(msg)
				if split[0] == "REPLY" {
					client, _ := strconv.Atoi(split[1])
					reply_msg := ""
					// Since message is split, we loop through after skipping first two fields and construct the reply
					for i := 0; i < len(split)-2; i++ {
						if i != 0 {
							reply_msg += " "
						}
						reply_msg += split[i+2]
					}
					reply_msg += "\n"
					conn := self.client_connections[client]
					self.network_mutex.Lock()
					conn.Write([]byte(reply_msg))
					self.network_mutex.Unlock()

				} else if split[0] == "PREPARE" {
					// TODO RTS and TW list setting vote boolean
					vote := true
					conn := self.branch_connections[conn_name]
					var msg string
					// var num int
					client_ID, _ := strconv.Atoi(split[1])
					if vote {
						msg = fmt.Sprintf("YES %d\n", client_ID)
						self.network_mutex.Lock()
						conn.Write([]byte(msg))
						self.network_mutex.Unlock()
					} else {
						msg = fmt.Sprintf("NO %d\n", client_ID)
						self.network_mutex.Lock()
						conn.Write([]byte(msg))
						self.network_mutex.Unlock()
					}
				} else if split[0] == "YES" {
					// check to make sure in correct election
					client_ID, _ := strconv.Atoi(split[1])
					// if YES reply to commit, increment num votes
					self.msg_mutex.Lock()
					// check if in prepare and if it is a vote from the correct prepare request
					if !self.in_prepare || self.timestamp_map[client_ID]-1 != self.latest_finished {
						self.msg_mutex.Unlock()
						return
					}
					self.num_votes += 1
					if self.num_votes == self.num_nodes {
						self.VoteChannel <- true
					}
					self.msg_mutex.Unlock()
				} else if split[0] == "NO" {
					// check to make sure in correct election
					client_ID, _ := strconv.Atoi(split[1])
					// if NO to commit just send message of VoteChannel to abort commit
					self.msg_mutex.Lock()
					// check if in prepare and correct prepare timestamp
					if !self.in_prepare || self.timestamp_map[client_ID]-1 != self.latest_finished {
						self.msg_mutex.Unlock()
						return
					}
					self.VoteChannel <- true
					self.msg_mutex.Unlock()
				} else {
					// if not a reply, do transaction and send appropriate reply to coordinator
					reply := do_transaction(split)
					// On BEGIN we get empty string back so no need to reply
					if len(reply) > 0 {
						conn := self.branch_connections[conn_name]
						reply_msg := "REPLY " + split[0] + " " + reply
						self.network_mutex.Lock()
						conn.Write([]byte(reply_msg))
						self.network_mutex.Unlock()
					}
				}
			}
		}(msg)
	}
}

// Function for processing client request.
func process_client_req(msg string) {
	split_msg := strings.Fields(msg)
	op := split_msg[1]
	ID_str := split_msg[0]
	client_ID, _ := strconv.Atoi(ID_str)
	client_conn := self.client_connections[client_ID]
	if op == "BEGIN" {
		// Send BEGIN notice
		for i := range self.branch_connections {
			self.network_mutex.Lock()
			self.branch_connections[i].Write([]byte(msg))
			self.network_mutex.Unlock()
		}
		// Change event timestamp and note in own map.
		do_transaction(split_msg)
		self.network_mutex.Lock()
		client_conn.Write([]byte("OK\n"))
		self.network_mutex.Unlock()

	} else if op == "COMMIT" {
		// Do 2PC and send response
		reply := do_transaction(split_msg)

		// Update coordinator's timestamps etc
		if reply == "COMMIT OK\n" {
			// If commit, update the latest commit and latest finished vars
			write_changes(client_ID)
			self.msg_mutex.Lock()
			timestamp := self.timestamp_map[client_ID]
			self.latest_commit = timestamp
			self.latest_finished = timestamp
			self.msg_mutex.Unlock()

			// Signal waiting goroutines for read and commit
			self.commit_cond.L.Lock()
			self.commit_cond.Broadcast()
			self.commit_cond.L.Unlock()
		} else {
			// it is an abort
			self_abort(client_ID)
		}

		self.network_mutex.Lock()
		// fmt.Println(reply)
		client_conn.Write([]byte(reply))
		self.network_mutex.Unlock()
	} else {
		// Else message will contain a branch, split message and forward to correct branch
		branch := strings.Split(split_msg[2], ".")[0]

		// If branches match, do transaction, else forward to correct node
		if branch == self.branch {
			reply := do_transaction(split_msg)
			self.network_mutex.Lock()
			client_conn.Write([]byte(reply))
			self.network_mutex.Unlock()

		} else {
			conn := self.branch_connections[branch]
			self.network_mutex.Lock()
			conn.Write([]byte(msg))
			self.network_mutex.Unlock()
		}
	}
}

// Function for actually doing transaction.
func do_transaction(split_msg []string) string {
	ID, _ := strconv.Atoi(split_msg[0])
	switch strings.TrimSuffix(split_msg[1], "\n") {
	case "BEGIN":
		self.msg_mutex.Lock()
		self.timestamp += 1
		self.timestamp_map[ID] = self.timestamp
		self.msg_mutex.Unlock()
		return ""

	case "DEPOSIT":

		split_ := strings.Split(split_msg[2], ".")
		account := split_[1]

		self.msg_mutex.Lock()
		transaction_timestamp := self.timestamp_map[ID]
		self.msg_mutex.Unlock()

		// Should the write be attempted given the timestamped ordering condition?
		attempt_write := timestamp_check(ID, account)
		if !attempt_write {
			send_abort(split_msg[0])
			self_abort(ID)
			return "ABORTED\n"
		}

		amount, _ := strconv.Atoi(split_msg[3])

		// Get existing amount 0 if account doesn't exist.
		var exists bool
		var timestamps map[int]int
		// Add to write map and TW's
		self.msg_mutex.Lock()

		_, exists = self.write_map[transaction_timestamp]
		if !exists {
			self.write_map[transaction_timestamp] = make([]string, 0)
		}

		// Adding to writemap for commits
		in_wlist := check_arr(account, self.write_map[transaction_timestamp])
		if !in_wlist {
			self.write_map[transaction_timestamp] = append(self.write_map[transaction_timestamp], account)
		}
		// // fmt.Println(transaction_timestamp, self.write_map[transaction_timestamp])

		// If tentative write list doesn't exist, make map
		timestamps, exists = self.TW[account]
		if !exists {
			self.TW[account] = make(map[int]int)
			timestamps = self.TW[account]
		}
		// If this is the first write, set initial value to 0
		_, exists = timestamps[transaction_timestamp]
		if !exists {
			timestamps[transaction_timestamp] = 0
		}
		timestamps[transaction_timestamp] += amount

		self.msg_mutex.Unlock()
		return "OK\n"

	case "WITHDRAW":

		split_ := strings.Split(split_msg[2], ".")
		account := split_[1]

		self.msg_mutex.Lock()
		transaction_timestamp := self.timestamp_map[ID]
		self.msg_mutex.Unlock()
		amount, _ := strconv.Atoi(split_msg[3])
		// Does the write satisfy concurrency check?
		// If not, abort
		attempt_write := timestamp_check(ID, account)
		if !attempt_write {
			send_abort(split_msg[0])
			self_abort(ID)
			return "ABORTED\n"
		}

		self.accounts_mutex.Lock()
		_, ok := self.accounts[account]
		//account_balance := 0
		self.accounts_mutex.Unlock()
		if !ok {
			self.msg_mutex.Lock()
			_, ok = self.TW[account][transaction_timestamp]
			self.msg_mutex.Unlock()
		}
		if ok {
			//account_balance = self.accounts[account]
			var exists bool
			var timestamps map[int]int
			// Add to write map and TW's
			self.msg_mutex.Lock()

			_, exists = self.write_map[transaction_timestamp]
			if !exists {
				self.write_map[transaction_timestamp] = make([]string, 0)
			}

			// If tentative write list doesn't exist, make map
			timestamps, exists = self.TW[account]
			if !exists {
				self.TW[account] = make(map[int]int)
				timestamps = self.TW[account]
			}
			// else {
			// 	if account_balance+self.TW[account][transaction_timestamp] < amount {
			// 		send_abort(split_msg[0])
			// 		self_abort(ID)
			// 		self.msg_mutex.Unlock()
			// 		return "ABORTED\n"
			// 	}
			// }

			// Adding to writemap for commits
			in_wlist := check_arr(account, self.write_map[transaction_timestamp])
			if !in_wlist {
				self.write_map[transaction_timestamp] = append(self.write_map[transaction_timestamp], account)
			}

			// If this is the first write, set initial value to 0
			_, exists = timestamps[transaction_timestamp]
			if !exists {
				timestamps[transaction_timestamp] = 0
			}
			timestamps[transaction_timestamp] -= amount

			self.msg_mutex.Unlock()
			return "OK\n"
		} else {
			// Account doesn't exist abort
			send_abort(split_msg[0])

			self_abort(ID)

			return "NOT FOUND, ABORTED\n"
		}

	case "BALANCE":

		// Get account and try BALANCE, if not found abort and notify others in the
		// cluster
		split_ := strings.Split(split_msg[2], ".")
		account := split_[1]

		ID, _ := strconv.Atoi(split_msg[0])

		// Get transaction timestamp and apply read rule
		self.msg_mutex.Lock()
		transaction_timestamp := self.timestamp_map[ID]
		// latest write commit on account
		latest_wcommit, exists := self.commit_map[account]
		// If not exists, there are no writes, so do read and return
		// Should return not found

		// Get Tw timestamps and get the latest tentative write on D
		// timestamps := self.TW[account]
		// Get latest TW on Data D
		// max_TW := get_max_TW_lt(timestamps, transaction_timestamp)
		// fmt.Printf("max_TW is %d\n", max_TW)

		// If there are no pending writes and account doesn't exist
		// do read that returns not found
		if !exists && len(self.TW[account]) == 0 {
			self.msg_mutex.Unlock()
			return do_read(split_msg, account)
		} else if transaction_timestamp >= latest_wcommit {
			// D is not committed wait until no entries on TW list < transaction timestamp
			// Goroutine sleeps and checks every time it is woken up via
			// broadcast.
			self.commit_cond.L.Lock()
			for exists_less(account, transaction_timestamp) {
				self.msg_mutex.Unlock()
				self.commit_cond.Wait()
				self.msg_mutex.Lock()
			}
			// The latest value is now committed/aborted so read
			// last commited value if it exists.
			self.commit_cond.L.Unlock()
			var read_val int
			read_val, exists = self.TW[account][transaction_timestamp]

			// If written by same transaction, return the written value
			if exists {
				// Add to RTS list
				_, exists = self.RTS[account]
				if exists {
					self.RTS[account] = append(self.RTS[account], transaction_timestamp)
				} else {
					self.RTS[account] = make([]int, 0)
					self.RTS[account] = append(self.RTS[account], transaction_timestamp)
				}

				self.accounts_mutex.Lock()
				existing, ok := self.accounts[account]
				if ok {
					read_val += existing
				}
				self.accounts_mutex.Unlock()
				self.msg_mutex.Unlock()
				return fmt.Sprintf("%s = %d\n", split_msg[2], read_val)
			} else {
				// Else just read value with latest commit
				// Add to RTS list
				_, exists = self.RTS[account]
				if exists {
					self.RTS[account] = append(self.RTS[account], transaction_timestamp)
				} else {
					self.RTS[account] = make([]int, 0)
				}
				self.msg_mutex.Unlock()

				return do_read(split_msg, account)
			}

		} else {
			// abort operation since read request timestamp < commit timestamp
			self.msg_mutex.Unlock()
			send_abort(split_msg[0])
			self_abort(ID)
			return "ABORTED\n"
		}

	// return the result of the 2 phase voting No need to return string
	case "COMMIT":
		// Check TW list to see if must wait. TODO change to get check TW
		// from all accounts
		must_wait_til, wait_account := check_TWs(ID)
		// fmt.Println(must_wait_til, wait_account)
		// Since there is a write to something on the TW list that must
		// be committed first
		//
		if must_wait_til > 0 {
			exists := true
			// transaction_timestamp := self.timestamp_map[ID]
			for exists {
				self.commit_cond.L.Lock()
				self.commit_cond.Wait()
				self.msg_mutex.Lock()
				_, exists = self.TW[wait_account][must_wait_til]
				self.msg_mutex.Unlock()
			}
			self.commit_cond.L.Unlock()
		}

		return start_2PC(split_msg[0])

	case "DO-COMMIT":
		write_changes(ID)
		// If commit, update the latest commit and latest finished vars
		self.msg_mutex.Lock()
		timestamp := self.timestamp_map[ID]
		self.latest_commit = timestamp
		self.latest_finished = timestamp
		self.msg_mutex.Unlock()

		self.commit_cond.L.Lock()
		self.commit_cond.Broadcast()
		self.commit_cond.L.Unlock()
	case "ABORT":
		// If aborted, only update latest finished

		timestamp := self.timestamp_map[ID]
		self.latest_finished = timestamp
	default:
		return ""
	}
	return ""
}

// Function for sending messages for aborting transactions.
func send_abort(ID string) {
	for _, conn := range self.branch_connections {
		self.network_mutex.Lock()
		conn.Write([]byte(fmt.Sprintf("%s ABORT\n", ID)))
		self.network_mutex.Unlock()
	}
}

// Return true if there exists a timestamp < transaction_timestamp on the TW
func exists_less(account string, transaction_timestamp int) bool {
	timestamps := self.TW[account]
	for timestamp := range timestamps {
		if timestamp < transaction_timestamp {
			return true
		}
	}
	return false
}

// On a DO-COMMIT message, write the actual changes to the accounts, also update
// commit_map and delete from TW list.
// Inputs- ID of the client initiating transaction
// Outputs- None
//
func write_changes(ID int) {
	self.msg_mutex.Lock()
	transaction_timestamp := self.timestamp_map[ID]
	writes, exists := self.write_map[transaction_timestamp]
	if !exists {
		self.msg_mutex.Unlock()
		return
	}
	self.accounts_mutex.Lock()
	for _, account := range writes {
		_, exists = self.accounts[account]
		if exists {
			self.accounts[account] += self.TW[account][transaction_timestamp]
		} else {
			self.accounts[account] = self.TW[account][transaction_timestamp]
		}
		self.commit_map[account] = transaction_timestamp
		delete(self.TW[account], transaction_timestamp)
	}
	self.accounts_mutex.Unlock()
	self.msg_mutex.Unlock()
}

// On an abort, remove from TW, RTS lists.
//
func abort_changes(ID int) {
	self.msg_mutex.Lock()
	transaction_timestamp := self.timestamp_map[ID]
	writes, exists := self.write_map[transaction_timestamp]
	if exists {
		for _, account := range writes {
			delete(self.TW[account], transaction_timestamp)
		}
	}
	self.msg_mutex.Unlock()
}

// If transaction has made it to the vote stage, then we just have to check balances
func check_balances(ID int) bool {
	self.msg_mutex.Lock()
	defer self.msg_mutex.Unlock()
	transaction_timestamp := self.timestamp_map[ID]
	writes, exists := self.write_map[transaction_timestamp]
	if !exists {
		return true
	}
	for _, account := range writes {
		timestamps := self.TW[account]
		if timestamps[transaction_timestamp] < 0 {
			return false
		}
	}
	return true
}

// Function that loops through write list and returns a the timestamp
func check_TWs(ID int) (int, string) {
	self.msg_mutex.Lock()
	defer self.msg_mutex.Unlock()
	transaction_timestamp := self.timestamp_map[ID]
	w_accounts, exists := self.write_map[transaction_timestamp]

	// Map doesn't exist implies no writes
	if !exists {
		return 0, ""
	}
	latest_time := 0
	latest_account := ""
	for _, account := range w_accounts {
		timestamps := self.TW[account]
		temp := wait_timestamp(transaction_timestamp, timestamps)
		if temp > latest_time {
			latest_time = temp
			latest_account = account
		}
	}
	// self.msg_mutex.Unlock()
	return latest_time, latest_account
}

// Function for returning the greatest timestamp < transaction's timestamp
func wait_timestamp(transaction_timestamp int, timestamps map[int]int) int {
	ans := 0
	for timestamp := range timestamps {
		if timestamp > ans && timestamp < transaction_timestamp {
			ans = timestamp
		}
	}
	return ans
}

// Function that does the timestamped ordering check
func timestamp_check(ID int, account string) bool {
	self.msg_mutex.Lock()
	// booleans used for making sure write is valid
	read_ok := true  // set for RTS check
	write_ok := true // set for committed write check
	transaction_timestamp := self.timestamp_map[ID]

	// Check the RTS and commit_map to make sure valid write else abort
	RTS_acc, exists_ := self.RTS[account]
	last_commit, exists1_ := self.commit_map[account]
	if exists_ {
		last_read := max_arr(RTS_acc)
		read_ok = (transaction_timestamp >= last_read)
	}
	if exists1_ {
		write_ok = (transaction_timestamp > last_commit)
	}
	self.msg_mutex.Unlock()
	return read_ok && write_ok
}

// Returns max of array of ints.
func max_arr(arr []int) int {
	var max int
	for _, elem := range arr {
		max = elem
		break
	}

	for _, elem := range arr {
		if elem > max {
			max = elem
		}
	}
	return max
}

// Checks to see if already in list of accounts to write to.
func check_arr(account string, write_list []string) bool {
	for _, l_account := range write_list {
		if strings.Compare(account, l_account) == 0 {
			return true
		}
	}
	return false
}

// On an abort set latest_finish only
func self_abort(ID int) {
	abort_changes(ID)
	// Signal any waiting goroutines for read and commit
	self.commit_cond.L.Lock()
	self.commit_cond.Broadcast()
	self.commit_cond.L.Unlock()
}

// Function for doing read on a BALANCE call
func do_read(split_msg []string, account string) string {
	self.accounts_mutex.Lock()
	amount, ok := self.accounts[account]
	self.accounts_mutex.Unlock()

	ID, _ := strconv.Atoi(split_msg[0])

	if ok {
		return (split_msg[2] + fmt.Sprintf(" = %d\n", amount))
	} else {
		send_abort(split_msg[0])
		self_abort(ID)
		return "NOT FOUND, ABORTED\n"
	}
}

// Function for getting maximum Tentative Write (TW) timestamp key.
// Inputs- None
// Outputs- Maximum timestamp in TW for that account
// Loops through self.TW and finds maximum key
//
func get_max_TW(timestamps map[int]int) int {
	var max_TW int
	for max_TW = range timestamps {
		break
	}
	for n := range timestamps {
		if n > max_TW {
			max_TW = n
		}
	}
	return max_TW
}

// Function for getting maximum less than (lt) transaction Tentative Write (TW) timestamp key
// Inputs- None
// Outputs- Maximum timestamp in TW for that account <= current timestamp
// Loops through self.TW and finds maximum key <= current transaction timestamp
//
func get_max_TW_lt(timestamps map[int]int, transaction_timestamp int) int {
	var max_TW int
	max_TW = 0
	for n := range timestamps {
		if n > max_TW && n <= transaction_timestamp {
			max_TW = n
		}
	}
	return max_TW
}

// Function for running 2 phase commit.
func start_2PC(ID string) string {
	// Send preapre requests to all nodes and vote, also
	// set in prepare bool and reset vote count.
	client_ID, _ := strconv.Atoi(ID)
	self.msg_mutex.Lock()
	self.in_prepare = true
	self.num_votes = 0
	self.msg_mutex.Unlock()

	self.network_mutex.Lock()
	for _, conn := range self.branch_connections {
		conn.Write([]byte(fmt.Sprintf("PREPARE %s\n", ID)))
	}
	self.network_mutex.Unlock()

	go self_vote(client_ID)
	// set prepare timeout in case
	prepare_timeout := 1000
	var do_commit bool
	select {
	case <-time.After(time.Duration(prepare_timeout) * time.Millisecond):
		// Check votes. If unanimous, send do COMMIT, else send ABORT
		// message.
		self.msg_mutex.Lock()
		votes := self.num_votes
		self.msg_mutex.Unlock()
		do_commit = (votes == self.num_nodes)

		self.network_mutex.Lock()
		for _, conn := range self.branch_connections {
			if do_commit {
				conn.Write([]byte(fmt.Sprintf("%s DO-COMMIT\n", ID)))
			} else {
				conn.Write([]byte(fmt.Sprintf("%s ABORT\n", ID)))
			}
		}
		self.network_mutex.Unlock()

	case <-self.VoteChannel:
		// Check votes. If unanimous, send do COMMIT, else send ABORT
		// message.
		self.msg_mutex.Lock()
		votes := self.num_votes
		self.msg_mutex.Unlock()
		do_commit = (votes == self.num_nodes)

		self.network_mutex.Lock()
		for _, conn := range self.branch_connections {
			if do_commit {
				conn.Write([]byte(fmt.Sprintf("%s DO-COMMIT\n", ID)))
			} else {
				conn.Write([]byte(fmt.Sprintf("%s ABORT\n", ID)))
			}
		}
		self.network_mutex.Unlock()
	}
	self.msg_mutex.Lock()
	self.in_prepare = false
	self.msg_mutex.Unlock()
	if do_commit {
		return "COMMIT OK\n"
	}
	return "ABORTED\n"
}

// Function for self voting.
func self_vote(ID int) {
	self.msg_mutex.Lock()
	defer self.msg_mutex.Unlock()
	vote := true
	// If vote is true, add to numvotes and send msg on channel if
	// required. If false, we can abort immediately so send message
	// on channel to start abort.
	if vote {
		self.num_votes += 1
		if self.num_votes == self.num_nodes {
			self.VoteChannel <- true
		}
	} else {
		self.VoteChannel <- true
	}
}

func config() {
	// Get command line args: name and config file name
	args := os.Args[1:]
	self.branch = args[0]
	config := args[1]
	// Read config file.
	file, _ := os.Open(config)
	// Line by line read.
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string
	for scanner.Scan() {
		config_line := scanner.Text()
		// fmt.Println(config_line)
		text = append(text, config_line)
		config_line_split := strings.Fields(config_line)
		if config_line_split[0] == self.branch {
			self.port = config_line_split[2]
		}
	}
	self.config_text = text
	file.Close()
	self.num_nodes = len(text)
}

// Function  to set up server connections.
func setup_channel(name string, split_line []string) {
	servAddr := split_line[1] + ":" + split_line[2]
	tcpAddr, _ := net.ResolveTCPAddr("tcp", servAddr)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)

	// Loop to connection is tried until it works.
	//
	for err != nil {
		conn, err = net.DialTCP("tcp", nil, tcpAddr)
	}

	// Send over name.
	//
	self.network_mutex.Lock()
	conn.Write([]byte(name + "\n"))
	self.network_mutex.Unlock()
	// Set connection in map
	self.network_mutex.Lock()
	self.branch_connections[split_line[0]] = conn
	self.network_mutex.Unlock()

}

// Function for making outgoing channels read from config file
func set_up_channels() {
	// Loop through node names and establish connections. Store
	// node names and connections in array.
	text := self.config_text
	for i := 0; i < len(text); i++ {
		line := string(text[i])
		split_line := strings.Split(line, " ")
		// Don't setup comm line if to self
		if strings.Compare(split_line[0], self.branch) == 0 {
			continue
		}
		go setup_channel(self.branch, split_line)
	}
}

func main() {

	server_setup()
	config()
	go set_up_channels()

	ln, err := net.Listen("tcp", ":"+self.port)
	if err != nil {
		fmt.Println("Error setting up listener")
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection")
			continue
		} else {
			go connection_handler(conn)
		}
	}
	ln.Close()
}
