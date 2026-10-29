package guest

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const maxWaitPorts = 64

type listeningPorts struct {
	Listening []int `json:"listening"`
	Missing   []int `json:"missing"`
}

func (s *Server) portsListeningHandler(w http.ResponseWriter, r *http.Request) {
	values, ok := r.URL.Query()["ports"]
	if !ok || len(values) != 1 || values[0] == "" {
		http.Error(w, "ports query parameter is required", http.StatusBadRequest)
		return
	}
	ports, err := parsePorts(values[0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	listening, err := readListeningPorts()
	if err != nil {
		http.Error(w, "cannot inspect listening sockets", http.StatusInternalServerError)
		return
	}
	result := listeningPorts{Listening: make([]int, 0, len(ports)), Missing: make([]int, 0, len(ports))}
	for _, port := range ports {
		if listening[port] {
			result.Listening = append(result.Listening, port)
		} else {
			result.Missing = append(result.Missing, port)
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func parsePorts(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	if len(parts) > maxWaitPorts {
		return nil, fmt.Errorf("ports query parameter supports at most %d ports", maxWaitPorts)
	}
	ports := make([]int, 0, len(parts))
	seen := make(map[int]bool, len(parts))
	for _, part := range parts {
		port, err := strconv.Atoi(part)
		if err != nil || port < 1 || port > 65535 || seen[port] {
			return nil, fmt.Errorf("invalid ports query parameter")
		}
		seen[port] = true
		ports = append(ports, port)
	}
	return ports, nil
}

func readListeningPorts() (map[int]bool, error) {
	listening := make(map[int]bool)
	for _, name := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		file, err := os.Open(name)
		if err != nil {
			if name == "/proc/net/tcp6" && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) < 4 || fields[0] == "sl" || fields[3] != "0A" {
				continue
			}
			address := fields[1]
			separator := strings.LastIndexByte(address, ':')
			if separator < 0 || len(address)-separator-1 != 4 {
				continue
			}
			port, err := strconv.ParseUint(address[separator+1:], 16, 16)
			if err == nil {
				listening[int(port)] = true
			}
		}
		err = scanner.Err()
		closeErr := file.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return listening, nil
}
