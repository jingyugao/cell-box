//go:build linux

package portwait

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/asm"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
	"golang.org/x/sys/unix"
)

const (
	listenCallback = 11 // BPF_SOCK_OPS_TCP_LISTEN_CB.
	eventSize      = 32
	ringSize       = 256 * 1024
	maxWaiters     = 256
	waiterBuffer   = 64
)

var (
	ErrClosed      = errors.New("port wait monitor is closed")
	ErrTooManyWait = errors.New("too many concurrent port waiters")
)

type listenEvent struct {
	netnsCookie uint64
	port        uint16
}

type waiter struct {
	ports    map[uint16]struct{}
	events   chan listenEvent
	overflow chan struct{}
}

// Monitor observes successful TCP listen transitions from processes in a
// cgroup and wakes waiters for matching network namespaces and ports.
type Monitor struct {
	mu          sync.Mutex
	waiters     map[*waiter]struct{}
	closed      bool
	terminalErr error
	done        chan struct{}
	readerDone  chan struct{}
	closeOnce   sync.Once
	closeErr    error

	events  *ebpf.Map
	program *ebpf.Program
	link    link.Link
	reader  *ringbuf.Reader
}

// New attaches a TCP listen observer to cgroupPath. The process needs the BPF
// privileges required to load a SOCK_OPS program and attach it to the cgroup.
func New(cgroupPath string) (*Monitor, error) {
	if cgroupPath == "" {
		return nil, errors.New("cgroup path is empty")
	}
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("remove BPF memlock limit: %w", err)
	}

	events, err := ebpf.NewMap(&ebpf.MapSpec{
		Name:       "portwait_events",
		Type:       ebpf.RingBuf,
		MaxEntries: ringSize,
	})
	if err != nil {
		return nil, fmt.Errorf("create listener event ring: %w", err)
	}
	cleanupMap := true
	defer func() {
		if cleanupMap {
			_ = events.Close()
		}
	}()

	program, err := ebpf.NewProgram(&ebpf.ProgramSpec{
		Name:         "portwait_listen",
		Type:         ebpf.SockOps,
		AttachType:   ebpf.AttachCGroupSockOps,
		License:      "GPL",
		Instructions: listenProgram(events),
	})
	if err != nil {
		return nil, fmt.Errorf("load SOCK_OPS TCP_LISTEN_CB program: %w", err)
	}
	cleanupProgram := true
	defer func() {
		if cleanupProgram {
			_ = program.Close()
		}
	}()

	attached, err := link.AttachCgroup(link.CgroupOptions{
		Path:    cgroupPath,
		Attach:  ebpf.AttachCGroupSockOps,
		Program: program,
	})
	if err != nil {
		return nil, fmt.Errorf("attach TCP listen observer to cgroup %q: %w", cgroupPath, err)
	}
	cleanupLink := true
	defer func() {
		if cleanupLink {
			_ = attached.Close()
		}
	}()

	reader, err := ringbuf.NewReader(events)
	if err != nil {
		return nil, fmt.Errorf("open listener event ring: %w", err)
	}
	cleanupReader := true
	defer func() {
		if cleanupReader {
			_ = reader.Close()
		}
	}()

	m := &Monitor{
		waiters:    make(map[*waiter]struct{}),
		done:       make(chan struct{}),
		readerDone: make(chan struct{}),
		events:     events,
		program:    program,
		link:       attached,
		reader:     reader,
	}
	cleanupMap = false
	cleanupProgram = false
	cleanupLink = false
	cleanupReader = false
	go m.readEvents()
	return m, nil
}

// Close stops event collection and wakes all pending waiters.
func (m *Monitor) Close() error {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		if !m.closed {
			m.closed = true
			close(m.done)
		}
		m.mu.Unlock()

		readerErr := m.reader.Close()
		<-m.readerDone
		linkErr := m.link.Close()
		programErr := m.program.Close()
		mapErr := m.events.Close()
		m.closeErr = errors.Join(readerErr, linkErr, programErr, mapErr)
	})
	return m.closeErr
}

// WaitPortsListening waits until each requested port has a listening TCP
// socket in netnsPath. It subscribes before reading the socket table so a
// listen event cannot be lost between the initial check and the wait.
func (m *Monitor) WaitPortsListening(ctx context.Context, netnsPath string, ports []int) error {
	wanted, err := normalizePorts(ports)
	if err != nil {
		return err
	}
	if len(wanted) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w, err := m.subscribe(wanted)
	if err != nil {
		return err
	}
	defer m.unsubscribe(w)

	cookie, listening, err := snapshot(ctx, netnsPath)
	if err != nil {
		return err
	}
	if allListening(listening, wanted) {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.done:
			return m.closedError()
		case <-w.overflow:
			currentCookie, current, err := snapshot(ctx, netnsPath)
			if err != nil {
				return err
			}
			if currentCookie != cookie {
				return errors.New("network namespace changed while waiting for ports")
			}
			if allListening(current, wanted) {
				return nil
			}
		case event := <-w.events:
			if event.netnsCookie != cookie {
				continue
			}
			currentCookie, current, err := snapshot(ctx, netnsPath)
			if err != nil {
				return err
			}
			if currentCookie != cookie {
				return errors.New("network namespace changed while waiting for ports")
			}
			if allListening(current, wanted) {
				return nil
			}
		}
	}
}

// CheckPortsListening reports whether each requested port currently has a
// listening TCP socket in netnsPath.
func (m *Monitor) CheckPortsListening(ctx context.Context, netnsPath string, ports []int) (bool, error) {
	return CheckPortsListening(ctx, netnsPath, ports)
}

// CheckPortsListening reports whether each requested port has a listening TCP
// socket in netnsPath. It can be used without loading or attaching eBPF.
func CheckPortsListening(ctx context.Context, netnsPath string, ports []int) (bool, error) {
	wanted, err := normalizePorts(ports)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if len(wanted) == 0 {
		return true, nil
	}
	_, listening, err := snapshot(ctx, netnsPath)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return allListening(listening, wanted), nil
}

func (m *Monitor) subscribe(ports map[uint16]struct{}) (*waiter, error) {
	w := &waiter{
		ports:    ports,
		events:   make(chan listenEvent, waiterBuffer),
		overflow: make(chan struct{}, 1),
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, m.closedErrorLocked()
	}
	if len(m.waiters) >= maxWaiters {
		return nil, ErrTooManyWait
	}
	m.waiters[w] = struct{}{}
	return w, nil
}

func (m *Monitor) unsubscribe(w *waiter) {
	m.mu.Lock()
	delete(m.waiters, w)
	m.mu.Unlock()
}

func (m *Monitor) readEvents() {
	defer close(m.readerDone)
	for {
		record, err := m.reader.Read()
		if err != nil {
			m.mu.Lock()
			closed := m.closed
			m.mu.Unlock()
			if !closed && !errors.Is(err, ringbuf.ErrClosed) {
				m.fail(fmt.Errorf("read listener event ring: %w", err))
			}
			return
		}
		if len(record.RawSample) != eventSize {
			m.fail(fmt.Errorf("invalid listener event size %d", len(record.RawSample)))
			return
		}
		sample := record.RawSample
		event := listenEvent{
			netnsCookie: binary.NativeEndian.Uint64(sample[8:16]),
			port:        uint16(binary.NativeEndian.Uint32(sample[24:28])),
		}
		m.mu.Lock()
		for w := range m.waiters {
			if _, ok := w.ports[event.port]; !ok {
				continue
			}
			select {
			case w.events <- event:
			default:
				// Recover from a full userspace waiter queue with a live snapshot.
				select {
				case w.overflow <- struct{}{}:
				default:
				}
			}
		}
		m.mu.Unlock()
	}
}

func (m *Monitor) fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.terminalErr = err
	m.closed = true
	close(m.done)
}

func (m *Monitor) closedError() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closedErrorLocked()
}

func (m *Monitor) closedErrorLocked() error {
	if m.terminalErr != nil {
		return fmt.Errorf("%w: %v", ErrClosed, m.terminalErr)
	}
	return ErrClosed
}

func listenProgram(events *ebpf.Map) asm.Instructions {
	return asm.Instructions{
		asm.Mov.Reg(asm.R6, asm.R1),
		asm.LoadMem(asm.R2, asm.R6, 0, asm.Word),
		asm.JEq.Imm(asm.R2, listenCallback, "listen"),
		asm.Mov.Imm(asm.R0, 1),
		asm.Return(),
		asm.Mov.Reg(asm.R1, asm.R6).WithSymbol("listen"),
		asm.FnKtimeGetNs.Call(),
		asm.Mov.Reg(asm.R7, asm.R0),
		asm.Mov.Reg(asm.R1, asm.R6),
		asm.FnGetNetnsCookie.Call(),
		asm.Mov.Reg(asm.R8, asm.R0),
		asm.Mov.Reg(asm.R1, asm.R6),
		asm.FnGetSocketCookie.Call(),
		asm.Mov.Reg(asm.R9, asm.R0),
		asm.LoadMem(asm.R2, asm.R6, 68, asm.Word),
		asm.StoreMem(asm.R10, -4, asm.R2, asm.Word),
		asm.LoadMapPtr(asm.R1, events.FD()),
		asm.Mov.Imm(asm.R2, eventSize),
		asm.Mov.Imm(asm.R3, 0),
		asm.FnRingbufReserve.Call(),
		asm.JEq.Imm(asm.R0, 0, "done"),
		asm.Mov.Reg(asm.R6, asm.R0),
		asm.StoreMem(asm.R6, 0, asm.R7, asm.DWord),
		asm.StoreMem(asm.R6, 8, asm.R8, asm.DWord),
		asm.StoreMem(asm.R6, 16, asm.R9, asm.DWord),
		asm.LoadMem(asm.R2, asm.R10, -4, asm.Word),
		asm.StoreMem(asm.R6, 24, asm.R2, asm.Word),
		asm.StoreImm(asm.R6, 28, 0, asm.Word),
		asm.Mov.Reg(asm.R1, asm.R6),
		asm.Mov.Imm(asm.R2, 0),
		asm.FnRingbufSubmit.Call(),
		asm.Mov.Imm(asm.R0, 1),
		asm.Return(),
		asm.Mov.Imm(asm.R0, 1).WithSymbol("done"),
		asm.Return(),
	}
}

func normalizePorts(ports []int) (map[uint16]struct{}, error) {
	wanted := make(map[uint16]struct{}, len(ports))
	for _, port := range ports {
		if port < 1 || port > 65535 {
			return nil, fmt.Errorf("port %d is outside the valid range 1..65535", port)
		}
		wanted[uint16(port)] = struct{}{}
	}
	return wanted, nil
}

func allListening(listening, wanted map[uint16]struct{}) bool {
	for port := range wanted {
		if _, ok := listening[port]; !ok {
			return false
		}
	}
	return true
}

func snapshot(ctx context.Context, netnsPath string) (uint64, map[uint16]struct{}, error) {
	if netnsPath == "" {
		return 0, nil, errors.New("network namespace path is empty")
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	type result struct {
		cookie uint64
		ports  map[uint16]struct{}
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		original, err := os.Open("/proc/thread-self/ns/net")
		if err != nil {
			runtime.UnlockOSThread()
			resultCh <- result{err: fmt.Errorf("open current network namespace: %w", err)}
			return
		}
		defer original.Close()
		target, err := os.Open(netnsPath)
		if err != nil {
			runtime.UnlockOSThread()
			resultCh <- result{err: fmt.Errorf("open network namespace %q: %w", netnsPath, err)}
			return
		}
		defer target.Close()
		if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
			runtime.UnlockOSThread()
			resultCh <- result{err: fmt.Errorf("enter network namespace %q: %w", netnsPath, err)}
			return
		}
		cookie, ports, snapshotErr := snapshotCurrentNetns(ctx)
		restoreErr := unix.Setns(int(original.Fd()), unix.CLONE_NEWNET)
		if restoreErr != nil {
			resultCh <- result{err: fmt.Errorf("restore original network namespace: %w", restoreErr)}
			runtime.Goexit()
		}
		runtime.UnlockOSThread()
		resultCh <- result{cookie: cookie, ports: ports, err: snapshotErr}
	}()
	r := <-resultCh
	return r.cookie, r.ports, r.err
}

func snapshotCurrentNetns(ctx context.Context) (uint64, map[uint16]struct{}, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return 0, nil, fmt.Errorf("open socket for network namespace cookie: %w", err)
	}
	cookie, cookieErr := unix.GetsockoptUint64(fd, unix.SOL_SOCKET, unix.SO_NETNS_COOKIE)
	closeErr := unix.Close(fd)
	if cookieErr != nil {
		return 0, nil, fmt.Errorf("read network namespace cookie: %w", cookieErr)
	}
	if closeErr != nil {
		return 0, nil, fmt.Errorf("close network namespace cookie socket: %w", closeErr)
	}
	ports := make(map[uint16]struct{})
	for _, path := range []string{"/proc/thread-self/net/tcp", "/proc/thread-self/net/tcp6"} {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if path == "/proc/thread-self/net/tcp6" && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, nil, fmt.Errorf("read %s in target network namespace: %w", path, err)
		}
		for _, line := range strings.Split(string(data), "\n")[1:] {
			fields := strings.Fields(line)
			if len(fields) < 4 || fields[3] != "0A" { // TCP_LISTEN.
				continue
			}
			port, err := parseLocalPort(fields[1], strings.HasSuffix(path, "tcp6"))
			if err != nil {
				continue
			}
			ports[port] = struct{}{}
		}
	}
	return cookie, ports, nil
}

func parseLocalPort(value string, ipv6 bool) (uint16, error) {
	address, portText, ok := strings.Cut(value, ":")
	if !ok {
		return 0, errors.New("invalid local socket address")
	}
	port, err := strconv.ParseUint(portText, 16, 16)
	if err != nil {
		return 0, err
	}
	addressLength := 8
	if ipv6 {
		addressLength = 32
	}
	if len(address) != addressLength {
		return 0, errors.New("invalid local socket address length")
	}
	return uint16(port), nil
}
