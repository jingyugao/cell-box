package localrunsc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"sort"
	"strings"
)

// network records the host-side resources owned by one local runsc sandbox.
// Host and Guest are plain IPv4 addresses; CIDR is the sandbox's /30.
type network struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Guest    string `json:"guest"`
	CIDR     string `json:"cidr"`
	HostMAC  string `json:"hostMac,omitempty"`
	GuestMAC string `json:"guestMac,omitempty"`
}

var networkNamePattern = regexp.MustCompile(`^cbl[a-f0-9]{12}$`)

const networkPool = "10.231.0.0/16"

func allocateNetwork(ctx context.Context, id string, reserved ...network) (network, error) {
	if len(id) < 12 || !isHex(id[:12]) {
		return network{}, errors.New("local runsc ID must begin with 12 hexadecimal characters")
	}
	name := "cbl" + strings.ToLower(id[:12])

	routesJSON, err := command(ctx, "ip", "-j", "route", "show", "table", "all")
	if err != nil {
		return network{}, fmt.Errorf("list host routes: %w", err)
	}
	var routes []struct {
		Dst string `json:"dst"`
	}
	if err := json.Unmarshal(routesJSON, &routes); err != nil {
		return network{}, fmt.Errorf("decode host routes: %w", err)
	}
	namespaces, err := command(ctx, "ip", "netns", "list")
	if err != nil {
		return network{}, fmt.Errorf("list network namespaces: %w", err)
	}
	if containsNamespace(string(namespaces), name) {
		return network{}, fmt.Errorf("network namespace %s already exists", name)
	}
	linksJSON, err := command(ctx, "ip", "-j", "link", "show")
	if err != nil {
		return network{}, fmt.Errorf("list host links: %w", err)
	}
	var links []struct {
		Name string `json:"ifname"`
	}
	if err := json.Unmarshal(linksJSON, &links); err != nil {
		return network{}, fmt.Errorf("decode host links: %w", err)
	}
	for _, link := range links {
		if link.Name == name {
			return network{}, fmt.Errorf("host link %s already exists", name)
		}
	}
	occupied := make([]netip.Prefix, 0, len(routes))
	for _, existing := range reserved {
		if err := existing.validate(); err != nil {
			return network{}, err
		}
		prefix, _ := netip.ParsePrefix(existing.CIDR)
		occupied = append(occupied, prefix)
	}
	for _, route := range routes {
		if route.Dst == "" || route.Dst == "default" || route.Dst == "0.0.0.0/0" {
			continue
		}
		prefix, err := routePrefix(route.Dst)
		if err != nil {
			return network{}, fmt.Errorf("decode host route destination %q: %w", route.Dst, err)
		}
		if prefix.Addr().Is4() {
			occupied = append(occupied, prefix.Masked())
		}
	}

	pool, _ := netip.ParsePrefix(networkPool)
	for i := uint32(0); i < 1<<14; i++ {
		base := pool.Addr().As4()
		offset := i * 4
		value := uint32(base[0])<<24 | uint32(base[1])<<16 | uint32(base[2])<<8 | uint32(base[3])
		value += offset
		addr := netip.AddrFrom4([4]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
		candidate := netip.PrefixFrom(addr, 30).Masked()
		conflict := false
		for _, prefix := range occupied {
			if candidate.Overlaps(prefix) {
				conflict = true
				break
			}
		}
		if conflict {
			continue
		}
		host := candidate.Addr().Next()
		guest := host.Next()
		mac := strings.ToLower(id[:2] + ":" + id[2:4] + ":" + id[4:6] + ":" + id[6:8] + ":" + id[8:10])
		return network{Name: name, Host: host.String(), Guest: guest.String(), CIDR: candidate.String(), HostMAC: "02:" + mac, GuestMAC: "06:" + mac}, nil
	}
	return network{}, errors.New("no free /30 subnet remains in 10.231.0.0/16")
}

func routePrefix(dst string) (netip.Prefix, error) {
	if prefix, err := netip.ParsePrefix(dst); err == nil {
		return prefix.Masked(), nil
	}
	addr, err := netip.ParseAddr(dst)
	if err != nil {
		return netip.Prefix{}, err
	}
	bits := 128
	if addr.Is4() {
		bits = 32
	}
	return netip.PrefixFrom(addr, bits), nil
}

func isHex(s string) bool {
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F') {
			return false
		}
	}
	return true
}

func (n network) create(ctx context.Context) error {
	if err := n.validate(); err != nil {
		return err
	}
	ipForward, err := command(ctx, "sysctl", "-n", "net.ipv4.ip_forward")
	if err != nil {
		return fmt.Errorf("check IPv4 forwarding prerequisite: %w", err)
	}
	if strings.TrimSpace(string(ipForward)) != "1" {
		return errors.New("local runsc networking requires net.ipv4.ip_forward=1")
	}
	if err := n.checkRouteOverlap(ctx); err != nil {
		return err
	}

	if _, err := command(ctx, "ip", "netns", "add", n.Name); err != nil {
		return fmt.Errorf("create network namespace %s: %w", n.Name, err)
	}
	peer := "cbp" + n.Name[3:]
	if _, err := command(ctx, "ip", "link", "add", n.Name, "type", "veth", "peer", "name", peer); err != nil {
		return fmt.Errorf("create veth pair for %s: %w", n.Name, err)
	}
	if _, err := command(ctx, "ip", "link", "set", peer, "netns", n.Name); err != nil {
		return fmt.Errorf("move guest veth into %s: %w", n.Name, err)
	}
	if _, err := command(ctx, "ip", "-n", n.Name, "link", "set", peer, "name", "eth0"); err != nil {
		return fmt.Errorf("rename guest veth in %s: %w", n.Name, err)
	}
	if n.HostMAC != "" {
		if _, err := command(ctx, "ip", "link", "set", n.Name, "address", n.HostMAC); err != nil {
			return err
		}
	}
	if n.GuestMAC != "" {
		if _, err := command(ctx, "ip", "-n", n.Name, "link", "set", "eth0", "address", n.GuestMAC); err != nil {
			return err
		}
	}
	if _, err := command(ctx, "ip", "addr", "add", n.Host+"/30", "dev", n.Name); err != nil {
		return fmt.Errorf("assign host address for %s: %w", n.Name, err)
	}
	if _, err := command(ctx, "ip", "link", "set", n.Name, "up"); err != nil {
		return fmt.Errorf("bring up host veth for %s: %w", n.Name, err)
	}
	if err := n.configureGuest(ctx); err != nil {
		return err
	}
	if err := n.installFirewall(ctx); err != nil {
		return fmt.Errorf("install firewall rules for %s: %w", n.Name, err)
	}
	return nil
}

// runsc consumes the namespace's addresses and routes when setting up netstack.
// Re-populate them before restore while retaining the same namespace and veth.
func (n network) configureGuest(ctx context.Context) error {
	if err := n.validate(); err != nil {
		return err
	}
	if _, err := command(ctx, "ip", "-n", n.Name, "link", "set", "lo", "up"); err != nil {
		return fmt.Errorf("bring up loopback in %s: %w", n.Name, err)
	}
	if _, err := command(ctx, "ip", "-n", n.Name, "addr", "replace", n.Guest+"/30", "dev", "eth0"); err != nil {
		return fmt.Errorf("assign guest address in %s: %w", n.Name, err)
	}
	if _, err := command(ctx, "ip", "-n", n.Name, "link", "set", "eth0", "up"); err != nil {
		return fmt.Errorf("bring up guest veth in %s: %w", n.Name, err)
	}
	if _, err := command(ctx, "ip", "-n", n.Name, "route", "replace", "default", "via", n.Host, "dev", "eth0"); err != nil {
		return fmt.Errorf("add default route in %s: %w", n.Name, err)
	}
	return nil
}

func (n network) validate() error {
	if !networkNamePattern.MatchString(n.Name) {
		return errors.New("invalid local runsc network name")
	}
	prefix, err := netip.ParsePrefix(n.CIDR)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() != 30 || prefix != prefix.Masked() {
		return errors.New("invalid local runsc network CIDR")
	}
	host, err := netip.ParseAddr(n.Host)
	if err != nil || !host.Is4() || host != prefix.Addr().Next() {
		return errors.New("invalid local runsc host address")
	}
	guest, err := netip.ParseAddr(n.Guest)
	if err != nil || !guest.Is4() || guest != host.Next() {
		return errors.New("invalid local runsc guest address")
	}
	for _, address := range []string{n.HostMAC, n.GuestMAC} {
		if address == "" {
			continue
		}
		mac, err := net.ParseMAC(address)
		if err != nil || len(mac) != 6 || mac[0]&1 != 0 {
			return errors.New("invalid local runsc MAC address")
		}
	}
	return nil
}

type networkLink struct {
	Name    string `json:"ifname"`
	Index   int    `json:"ifindex"`
	Peer    int    `json:"link_index"`
	Address string `json:"address"`
	Info    struct {
		Kind string `json:"info_kind"`
	} `json:"linkinfo"`
}

func networkLinks(ctx context.Context, namespace string) ([]networkLink, error) {
	args := []string{"-j", "-d"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	data, err := command(ctx, "ip", append(args, "link", "show")...)
	if err != nil {
		return nil, err
	}
	var links []networkLink
	if err := json.Unmarshal(data, &links); err != nil {
		return nil, err
	}
	return links, nil
}

func findLink(links []networkLink, name string) (networkLink, bool) {
	for _, link := range links {
		if link.Name == name {
			return link, true
		}
	}
	return networkLink{}, false
}

func (n *network) rememberMACs(ctx context.Context) error {
	hosts, err := networkLinks(ctx, "")
	if err != nil {
		return err
	}
	guests, err := networkLinks(ctx, n.Name)
	if err != nil {
		return err
	}
	host, hostOK := findLink(hosts, n.Name)
	guest, guestOK := findLink(guests, "eth0")
	if !hostOK || !guestOK || host.Info.Kind != "veth" || guest.Info.Kind != "veth" || host.Peer != guest.Index || guest.Peer != host.Index {
		return errors.New("local sandbox network ownership changed")
	}
	if (n.HostMAC != "" && n.HostMAC != host.Address) || (n.GuestMAC != "" && n.GuestMAC != guest.Address) {
		return errors.New("local sandbox network MAC identity changed")
	}
	n.HostMAC, n.GuestMAC = host.Address, guest.Address
	return n.validate()
}

// Called only while the execution is stopped. Rebuild ephemeral network state
// after a host reboot, retaining the checkpoint's subnet, addresses and MACs.
func (n network) ensure(ctx context.Context) error {
	if err := n.validate(); err != nil {
		return err
	}
	namespaces, err := command(ctx, "ip", "netns", "list")
	if err != nil {
		return err
	}
	links, err := networkLinks(ctx, "")
	if err != nil {
		return err
	}
	_, linkExists := findLink(links, n.Name)
	namespaceExists := containsNamespace(string(namespaces), n.Name)
	if !linkExists && !namespaceExists {
		return n.create(ctx)
	}
	if !linkExists || !namespaceExists {
		return errors.New("incomplete local sandbox network; discard the sandbox instead of replacing an unverified interface")
	}
	if err := n.rememberMACs(ctx); err != nil {
		return err
	}
	if err := n.checkRouteOverlap(ctx); err != nil {
		return err
	}
	if _, err := command(ctx, "ip", "addr", "replace", n.Host+"/30", "dev", n.Name); err != nil {
		return err
	}
	if _, err := command(ctx, "ip", "link", "set", n.Name, "up"); err != nil {
		return err
	}
	return n.installFirewall(ctx)
}

func (n network) checkRouteOverlap(ctx context.Context) error {
	routesJSON, err := command(ctx, "ip", "-j", "route", "show", "table", "all")
	if err != nil {
		return fmt.Errorf("recheck host routes before creating %s: %w", n.Name, err)
	}
	var routes []struct {
		Dst string `json:"dst"`
		Dev string `json:"dev"`
	}
	if err := json.Unmarshal(routesJSON, &routes); err != nil {
		return fmt.Errorf("decode host routes before creating %s: %w", n.Name, err)
	}
	candidate, _ := netip.ParsePrefix(n.CIDR)
	for _, route := range routes {
		if route.Dev == n.Name {
			continue
		}
		if route.Dst == "" || route.Dst == "default" || route.Dst == "0.0.0.0/0" {
			continue
		}
		prefix, err := routePrefix(route.Dst)
		if err != nil {
			return fmt.Errorf("decode host route destination %q: %w", route.Dst, err)
		}
		if prefix.Addr().Is4() && candidate.Overlaps(prefix.Masked()) {
			return fmt.Errorf("sandbox network %s conflicts with host route %s", candidate, prefix)
		}
	}
	return nil
}

type firewallRule struct {
	chain string
	args  []string
}

func (n network) firewallRules() []firewallRule {
	tag := []string{"-m", "comment", "--comment", n.Name}
	withTag := func(args []string) []string {
		for i, arg := range args {
			if arg == "-j" {
				out := append([]string(nil), args[:i]...)
				out = append(out, tag...)
				return append(out, args[i:]...)
			}
		}
		return append(append([]string(nil), args...), tag...)
	}
	return []firewallRule{
		{chain: "INPUT", args: withTag([]string{"-i", n.Name, "-j", "DROP"})},
		{chain: "INPUT", args: withTag([]string{"-i", n.Name, "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "ACCEPT"})},
		// WSL may advertise a host-local DNS tunnel in resolv.conf.
		{chain: "INPUT", args: withTag([]string{"-i", n.Name, "-p", "udp", "-m", "udp", "--dport", "53", "-j", "ACCEPT"})},
		{chain: "INPUT", args: withTag([]string{"-i", n.Name, "-p", "tcp", "-m", "tcp", "--dport", "53", "-j", "ACCEPT"})},
		{chain: "FORWARD", args: withTag([]string{"-i", n.Name, "-j", "ACCEPT"})},
		{chain: "FORWARD", args: withTag([]string{"-i", n.Name, "-o", "cbl+", "-j", "DROP"})},
		{chain: "FORWARD", args: withTag([]string{"-o", n.Name, "-j", "DROP"})},
		{chain: "FORWARD", args: withTag([]string{"-o", n.Name, "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "ACCEPT"})},
		{chain: "nat:POSTROUTING", args: withTag([]string{"-s", n.CIDR, "!", "-d", n.CIDR, "-j", "MASQUERADE"})},
	}
}

func (n network) installFirewall(ctx context.Context) error {
	// Add destination rules before the source allow rule. iptables -I places
	// each rule at the head, so this keeps new inbound traffic fenced off.
	rules := n.firewallRules()
	for i := 0; i < len(rules); i++ {
		if err := ensureRule(ctx, rules[i], true); err != nil {
			return err
		}
	}
	return nil
}

func (n network) remove(ctx context.Context) error {
	if err := n.validate(); err != nil {
		return err
	}
	var errs []error
	for _, rule := range n.firewallRules() {
		if err := ensureRule(ctx, rule, false); err != nil {
			errs = append(errs, err)
		}
	}

	namespaces, err := command(ctx, "ip", "netns", "list")
	if err != nil {
		errs = append(errs, fmt.Errorf("list network namespaces: %w", err))
	} else if containsNamespace(string(namespaces), n.Name) {
		if _, err := command(ctx, "ip", "netns", "del", n.Name); err != nil {
			errs = append(errs, fmt.Errorf("delete network namespace %s: %w", n.Name, err))
		}
	}

	linksJSON, linkErr := command(ctx, "ip", "-j", "link", "show")
	if linkErr != nil {
		errs = append(errs, fmt.Errorf("list host links: %w", linkErr))
	} else {
		var links []struct {
			Name string `json:"ifname"`
		}
		if err := json.Unmarshal(linksJSON, &links); err != nil {
			errs = append(errs, fmt.Errorf("decode host links: %w", err))
		} else {
			for _, link := range links {
				if link.Name == n.Name {
					if _, err := command(ctx, "ip", "link", "del", n.Name); err != nil {
						// Dropping the namespace can asynchronously remove its veth
						// between the listing and deletion. Confirm absence before
						// treating that race as successful cleanup.
						after, checkErr := command(ctx, "ip", "-j", "link", "show")
						var remaining []struct {
							Name string `json:"ifname"`
						}
						gone := checkErr == nil && json.Unmarshal(after, &remaining) == nil
						for _, link := range remaining {
							if link.Name == n.Name {
								gone = false
							}
						}
						if !gone {
							errs = append(errs, fmt.Errorf("delete host veth %s: %w", n.Name, err))
						}
					}
					break
				}
			}
		}
	}
	return errors.Join(errs...)
}

func ensureRule(ctx context.Context, rule firewallRule, add bool) error {
	chain := rule.chain
	args := []string{"-w", "5", "-S", chain}
	if strings.HasPrefix(chain, "nat:") {
		chain = strings.TrimPrefix(chain, "nat:")
		args = []string{"-w", "5", "-t", "nat", "-S", chain}
	}
	out, err := command(ctx, "iptables", args...)
	if err != nil {
		return fmt.Errorf("inspect iptables chain %s: %w", rule.chain, err)
	}
	present := matchingRules(string(out), rule.args)
	if add {
		if present {
			return nil
		}
		insert := []string{"-w", "5", "-I", chain, "1"}
		if strings.HasPrefix(rule.chain, "nat:") {
			insert = []string{"-w", "5", "-t", "nat", "-I", chain, "1"}
		}
		if _, err := command(ctx, "iptables", append(insert, rule.args...)...); err != nil {
			return fmt.Errorf("add iptables rule in %s: %w", rule.chain, err)
		}
		return nil
	}
	for present {
		remove := []string{"-w", "5", "-D", chain}
		if strings.HasPrefix(rule.chain, "nat:") {
			remove = []string{"-w", "5", "-t", "nat", "-D", chain}
		}
		if _, err := command(ctx, "iptables", append(remove, rule.args...)...); err != nil {
			return fmt.Errorf("remove iptables rule in %s: %w", rule.chain, err)
		}
		out, err = command(ctx, "iptables", args...)
		if err != nil {
			return fmt.Errorf("inspect iptables chain %s: %w", rule.chain, err)
		}
		present = matchingRules(string(out), rule.args)
	}
	return nil
}

func matchingRules(rules string, expected []string) bool {
	for _, line := range strings.Split(rules, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "-A" {
			continue
		}
		matched := len(fields)-2 == len(expected)
		for i := range expected {
			if !matched || canonicalRuleToken(fields[i+2]) != canonicalRuleToken(expected[i]) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func canonicalRuleToken(token string) string {
	token = strings.Trim(token, "\"'")
	if !strings.Contains(token, ",") {
		return token
	}
	values := strings.Split(token, ",")
	sort.Strings(values)
	return strings.Join(values, ",")
}

func containsNamespace(list, name string) bool {
	for _, line := range strings.Split(list, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == name {
			return true
		}
	}
	return false
}
