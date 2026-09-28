// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "idle":
			for {
				time.Sleep(time.Hour)
			}
		case "get":
			client := &http.Client{Timeout: 20 * time.Second}
			resp, err := client.Get(os.Args[2])
			if err != nil {
				log.Fatal(err)
			}
			defer resp.Body.Close()
			io.Copy(os.Stdout, resp.Body)
			if resp.StatusCode != 200 {
				os.Exit(1)
			}
			return
		}
	}
	started := time.Now().UTC().Format(time.RFC3339Nano)
	memory := make([]byte, 32<<20)
	if _, err := rand.Read(memory); err != nil {
		log.Fatal(err)
	}
	initialHash := fmt.Sprintf("%x", sha256.Sum256(memory))
	instance := hex.EncodeToString(memory[:16])
	if err := os.WriteFile("/marker", []byte(instance), 0600); err != nil {
		log.Fatal(err)
	}
	var count atomic.Int64
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	http.HandleFunc("/increment", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, count.Add(1))
	})
	http.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		hash := fmt.Sprintf("%x", sha256.Sum256(memory))
		marker, err := os.ReadFile("/marker")
		if err != nil || string(marker) != instance || hash != initialHash {
			http.Error(w, fmt.Sprintf("state mismatch: marker=%q err=%v hash=%s expected=%s", marker, err, hash, initialHash), 500)
			return
		}
		ifaces, _ := net.Interfaces()
		var addrs []string
		for _, iface := range ifaces {
			ips, _ := iface.Addrs()
			for _, ip := range ips {
				addrs = append(addrs, iface.Name+":"+ip.String())
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"count": count.Load(), "started": started,
			"instance": instance, "memory_bytes": len(memory), "memory_sha256": hash,
			"marker": string(marker), "pid": os.Getpid(), "addresses": addrs})
	})
	http.HandleFunc("/network", func(w http.ResponseWriter, r *http.Request) {
		results := map[string]any{}
		for _, host := range []string{"kubernetes.default.svc.cluster.local.", "counter.recoverable-system.svc.cluster.local.", "example.com."} {
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			ips, err := net.DefaultResolver.LookupHost(ctx, host)
			cancel()
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			results[host] = ips
		}
		for _, addr := range []string{"example.com:443", "1.1.1.1:443"} {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			conn.Close()
			results[addr] = "connected"
		}
		json.NewEncoder(w).Encode(results)
	})
	log.Fatal(http.ListenAndServe(":8000", nil))
}
