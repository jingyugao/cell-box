package boxprovider

import (
	"fmt"
	"sort"

	"cellbox.local/cellbox/internal/guestapi"
)

// NormalizeWaitPorts returns the requested TCP ports in a stable order.
// An omitted list waits for the Guest management listener.
func NormalizeWaitPorts(ports []int) ([]int, error) {
	if len(ports) == 0 {
		return []int{guestapi.Port}, nil
	}
	if len(ports) > 64 {
		return nil, fmt.Errorf("waitPorts supports at most 64 ports")
	}
	seen := make(map[int]bool, len(ports))
	out := make([]int, 0, len(ports))
	for _, port := range ports {
		if port < 1 || port > 65535 {
			return nil, fmt.Errorf("waitPorts values must be between 1 and 65535")
		}
		if !seen[port] {
			seen[port] = true
			out = append(out, port)
		}
	}
	sort.Ints(out)
	return out, nil
}
