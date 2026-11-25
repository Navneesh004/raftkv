// Command raftkv-cli talks to a raftkv cluster.
//
//	raftkv-cli -servers a:7000,b:7000,c:7000 put  key value
//	raftkv-cli -servers ...                  append key value
//	raftkv-cli -servers ...                  get  key
//	raftkv-cli -servers ...                  status
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Navneesh004/raftkv/kv"
	"github.com/Navneesh004/raftkv/node"
	"github.com/Navneesh004/raftkv/transport"
)

func main() {
	servers := flag.String("servers", "localhost:7000,localhost:7001,localhost:7002", "comma-separated server addresses")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: raftkv-cli [-servers list] get KEY | put KEY VALUE | append KEY VALUE | status")
		flag.PrintDefaults()
	}
	flag.Parse()
	addrs := strings.Split(*servers, ",")
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	if args[0] == "status" {
		status(addrs)
		return
	}
	ck := kv.MakeClerk(node.Endpoints(addrs, time.Second))
	switch {
	case args[0] == "get" && len(args) == 2:
		fmt.Println(ck.Get(args[1]))
	case args[0] == "put" && len(args) == 3:
		ck.Put(args[1], args[2])
		fmt.Println("OK")
	case args[0] == "append" && len(args) == 3:
		ck.Append(args[1], args[2])
		fmt.Println("OK")
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func status(addrs []string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ADDR\tID\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLOG\tKEYS")
	for _, a := range addrs {
		var reply kv.StatusReply
		if !transport.NewClient(a, 500*time.Millisecond).Call("KV.Status", &kv.StatusArgs{}, &reply) {
			fmt.Fprintf(w, "%s\t-\tDOWN\t\t\t\t\t\t\n", a)
			continue
		}
		r := reply.Raft
		fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%d\t%d\t[%d..%d]\t%d\n",
			a, r.ID, r.Role, r.Term, r.LeaderID, r.CommitIndex, r.LastApplied, r.FirstIndex, r.LastIndex, reply.Keys)
	}
	w.Flush()
}
