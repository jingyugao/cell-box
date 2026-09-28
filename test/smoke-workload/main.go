// A static fixture for the opt-in local Docker integration test.
package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	groups, err := os.Getgroups()
	if err != nil || len(groups) != 0 {
		fmt.Fprint(os.Stderr, "inherited supplementary groups")
		os.Exit(1)
	}
	if len(os.Args) < 2 {
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "loopback-ok") })
		if err := http.ListenAndServe("127.0.0.1:8080", nil); err != nil {
			panic(err)
		}
	case "identity":
		fmt.Printf("uid=%d gid=%d\n", os.Getuid(), os.Getgid())
	case "secret-check":
		if _, err := os.ReadFile("/var/lib/cellbox/debug/demo"); err == nil {
			fmt.Print("unexpected-secret-access")
			os.Exit(1)
		}
		fmt.Print("secret-denied")
	case "tool":
		b, err := os.ReadFile(os.Getenv("DEMO_FILE"))
		if err != nil || string(b) != "smoke-secret" {
			os.Exit(1)
		}
		fmt.Printf("uid=%d credential-ok", os.Getuid())
	default:
		os.Exit(2)
	}
}
