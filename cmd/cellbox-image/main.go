package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"cellbox.local/cellbox/internal/image"
)

func main() {
	var opts image.Options
	flag.StringVar(&opts.Binary, "docker", "docker", "Docker CLI executable")
	flag.StringVar(&opts.Base, "base", "", "user base image")
	flag.StringVar(&opts.GuestBinary, "guest", "", "static Linux guest binary")
	flag.StringVar(&opts.ProductDir, "product-dir", "", "optional product payload directory")
	flag.StringVar(&opts.Manifest, "manifest", "", "optional product manifest file")
	flag.StringVar(&opts.Platform, "platform", "", "optional Linux platform, e.g. linux/amd64")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	result, err := image.Prepare(context.Background(), opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
