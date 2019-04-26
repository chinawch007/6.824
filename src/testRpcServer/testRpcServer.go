//一个文件夹里只能有一个包
package main

import (
	"fmt"
	"strconv"

	"../labrpc"
)

// net := MakeNetwork() -- holds network, clients, servers.
// end := net.MakeEnd(endname) -- create a client end-point, to talk to one server.
// net.AddServer(servername, server) -- adds a named server to network.
// net.DeleteServer(servername) -- eliminate the named server.
// net.Connect(endname, servername) -- connect a client to a server.
// net.Enable(endname, enabled) -- enable/disable a client.
// net.Reliable(bool) -- false means drop/delay messages

type JunkServer struct {
	data int
}

func (js *JunkServer) Handler1(args string, reply *int) {
	fmt.Println(args)
	*reply, _ = strconv.Atoi(args)
}

func main() {
	rn := labrpc.MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := labrpc.MakeService(js)

	rs := labrpc.MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	{
		reply := 0
		e.Call("JunkServer.Handler1", "9099", &reply)
	}
	for i := 0; ; i++ {

	}

}
