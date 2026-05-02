# Load Balancer

A Basic Load Balancer written in Go. The main aim of this project is to learn how load balancer(at least, the basic ones) function.
The secondary aim was to deepen my understanding of Go.

## How to use
1) Create a basic server (or multiple) like this (Keep them in separate directories):
```go
package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Response from Server 1")
	})
	http.ListenAndServe(":8081", nil)
}

```

2) Run the server(s)
3) Run the Load Balancer 
```bash
go run main.go -port=3030 -backends=http://localhost:8081
```
4) Check the functioning in the terminal where the load balancer is running & using `curl http://localhost:3030` in a separate terminal.

