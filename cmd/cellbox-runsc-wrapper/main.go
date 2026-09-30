// SPDX-License-Identifier: Apache-2.0

package main

import (
	"cellbox.local/cellbox/internal/adapter"
	"cellbox.local/cellbox/internal/version"
	"fmt"
	"os"
)

func main() {
	// Preserve runsc's own --version behavior used by runtime consumers.
	if len(os.Args) == 2 && os.Args[1] == "--adapter-version" {
		fmt.Println(version.String("cellbox-runsc-wrapper"))
		return
	}
	adapter.Main()
}
