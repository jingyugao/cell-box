// SPDX-License-Identifier: Apache-2.0

// Package version describes the executable, without contacting Kubernetes.
package version

import (
	"fmt"
	"runtime"
)

// Version and Revision are supplied by hack/build.sh. Direct go builds are dev builds.
var Version = "dev"
var Revision = "unknown"

func String(component string) string {
	return fmt.Sprintf("%s %s (%s; %s; %s/%s)", component, Version, Revision, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
