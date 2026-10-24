package localrunsc

import (
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/image"
)

// Cache only immutable OCI blobs. Each execution gets a separate writable unpack.
func (p *Provider) prepareImage(ctx context.Context, r record) error {
	hash := sha256.Sum256([]byte(r.Spec.Image))
	cache := filepath.Join(p.base, "images", hex.EncodeToString(hash[:]))
	if _, err := os.Stat(filepath.Join(cache, "index.json")); os.IsNotExist(err) {
		tmp, err := os.MkdirTemp(filepath.Join(p.base, "images"), "pull-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp)
		ref, err := image.ParseRegistryReference(r.Spec.Image, p.insecureRegistry)
		if err != nil {
			return err
		}
		args := []string{"copy"}
		if p.insecureRegistry != "" && ref.Context().RegistryStr() == p.insecureRegistry {
			args = append(args, "--src-tls-verify=false")
		}
		args = append(args, "docker://"+r.Spec.Image, "oci:"+tmp+":image")
		if _, err := command(ctx, "skopeo", args...); err != nil {
			return err
		}
		if err := os.Rename(tmp, cache); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	_, err := command(ctx, "umoci", "unpack", "--image", cache+":image", filepath.Join(p.dir(r.Handle.Name), "bundle"))
	return err
}

func (p *Provider) configure(ctx context.Context, r record) error {
	guest, err := elf.Open(p.guestBinary)
	if err != nil {
		return fmt.Errorf("guest must be a static Linux ELF: %w", err)
	}
	defer guest.Close()
	for _, program := range guest.Progs {
		if program.Type == elf.PT_INTERP {
			return errors.New("local guest binary must be statically linked (build with CGO_ENABLED=0)")
		}
	}
	bundle := filepath.Join(p.dir(r.Handle.Name), "bundle")
	// umoci and runsc honor the service umask when creating the filesystem
	// root and missing mount paths. Guest UIDs must be able to traverse them.
	root, err := os.OpenRoot(filepath.Join(bundle, "rootfs"))
	if err != nil {
		return err
	}
	defer root.Close()
	parents := []string{".", "opt", "opt/cellbox", "opt/cellbox/bin"}
	if r.Spec.SharedReadOnlyHostPath != "" {
		parents = append(parents, "var", "var/lib", "var/lib/cellbox")
	}
	for _, dir := range parents {
		if err := root.MkdirAll(dir, 0755); err != nil {
			return err
		}
		info, err := root.Lstat(dir)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("guest agent parent must be a directory: %s", dir)
		}
		if err := root.Chown(dir, 0, 0); err != nil {
			return err
		}
		if err := root.Chmod(dir, 0755); err != nil {
			return err
		}
	}
	const mountTarget = "opt/cellbox/bin/cellbox-container-agent"
	if info, err := root.Lstat(mountTarget); err == nil && !info.Mode().IsRegular() {
		return errors.New("guest agent mount target must be a regular file")
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	target, err := root.OpenFile(mountTarget, os.O_CREATE|os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	if err := target.Close(); err != nil {
		return err
	}
	if err := root.Chown(mountTarget, 0, 0); err != nil {
		return err
	}
	if err := root.Chmod(mountTarget, 0755); err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(bundle, "config.json"))
	if err != nil {
		return err
	}
	var spec map[string]any
	if err := json.Unmarshal(b, &spec); err != nil {
		return err
	}
	encoded, err := guestConfig(r.Spec.Config)
	if err != nil {
		return err
	}
	env := []any{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
	if process, ok := spec["process"].(map[string]any); ok {
		if imageEnv, ok := process["env"].([]any); ok {
			env = append(env, imageEnv...)
		}
	}
	caps := []string{"CAP_CHOWN", "CAP_FOWNER", "CAP_DAC_OVERRIDE", "CAP_SETUID", "CAP_SETGID", "CAP_KILL"}
	args := []string{guestapi.Binary, "serve", "--config-base64", encoded}
	if r.Spec.Staged {
		args = append(args, "--staged")
	}
	spec["process"] = map[string]any{
		"terminal": false, "user": map[string]any{"uid": 0, "gid": 0}, "cwd": "/",
		"args": args, "env": env,
		"noNewPrivileges": true,
		"capabilities":    map[string]any{"bounding": caps, "effective": caps, "permitted": caps},
		"rlimits":         []any{map[string]any{"type": "RLIMIT_NOFILE", "hard": 65536, "soft": 65536}},
	}
	spec["root"] = map[string]any{"path": "rootfs", "readonly": false}
	spec["hostname"] = "cellbox"
	namespaces := []any{map[string]any{"type": "pid"}, map[string]any{"type": "ipc"}, map[string]any{"type": "uts"}, map[string]any{"type": "mount"}, map[string]any{"type": "network", "path": "/var/run/netns/" + r.Network.Name}}
	resources := map[string]any{}
	if r.Spec.CPU > 0 {
		resources["cpu"] = map[string]any{"period": 100000, "quota": int64(r.Spec.CPU * 100000)}
	}
	if r.Spec.MemoryMiB > 0 {
		resources["memory"] = map[string]any{"limit": r.Spec.MemoryMiB * (1 << 20)}
	}
	linux := map[string]any{"namespaces": namespaces}
	if len(resources) > 0 {
		linux["resources"] = resources
		linux["cgroupsPath"] = "/cellbox-local-" + r.Handle.ID
	}
	spec["linux"] = linux
	mounts := []any{
		map[string]any{"destination": "/proc", "type": "proc", "source": "proc", "options": []string{"nosuid", "noexec", "nodev"}},
		map[string]any{"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "mode=755"}},
		map[string]any{"destination": "/dev/pts", "type": "devpts", "source": "devpts", "options": []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"}},
		map[string]any{"destination": "/dev/shm", "type": "tmpfs", "source": "shm", "options": []string{"nosuid", "noexec", "nodev", "mode=1777", "size=67108864"}},
		map[string]any{"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": []string{"nosuid", "noexec", "nodev", "ro"}},
		map[string]any{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "nodev", "mode=1777"}},
		map[string]any{"destination": guestapi.Binary, "type": "bind", "source": p.guestBinary, "options": []string{"bind", "ro", "nosuid", "nodev"}},
	}
	// Bind a host-controlled resolver file instead of following paths from an image.
	var resolvers []string
	// systemd-resolved's loopback stub is unreachable in a sandbox. Use
	// its uplink file before falling back to public resolvers.
	for _, file := range []string{"/etc/resolv.conf", "/run/systemd/resolve/resolv.conf"} {
		resolver, err := os.ReadFile(file)
		if os.IsNotExist(err) && file != "/etc/resolv.conf" {
			continue
		}
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(resolver), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "nameserver" {
				ip := net.ParseIP(fields[1])
				if ip != nil && ip.To4() != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
					resolvers = append(resolvers, "nameserver "+ip.String())
				}
			}
		}
		if len(resolvers) > 0 {
			break
		}
	}
	if len(resolvers) == 0 {
		resolvers = []string{"nameserver 1.1.1.1", "nameserver 8.8.8.8"}
	}
	resolverPath := filepath.Join(p.dir(r.Handle.Name), "resolv.conf")
	if err := os.WriteFile(resolverPath, []byte(strings.Join(resolvers, "\n")+"\n"), 0644); err != nil {
		return err
	}
	if err := os.Chmod(resolverPath, 0644); err != nil {
		return err
	}
	mounts = append(mounts, map[string]any{"destination": "/etc/resolv.conf", "type": "bind", "source": resolverPath, "options": []string{"bind", "ro", "nosuid", "nodev", "noexec"}})
	for _, mount := range []struct {
		source, target string
		readOnly       bool
	}{
		{r.Spec.SharedReadOnlyHostPath, api.SharedMountPath, true},
		{r.Spec.DebugReadOnlyHostPath, "/home/debug", true},
		{r.Spec.DebugReadWriteHostPath, "/home/debug", false},
	} {
		if mount.source == "" {
			continue
		}
		info, err := os.Stat(mount.source)
		if err != nil {
			return fmt.Errorf("local host mount %s: %w", mount.source, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("local host mount must be a directory: %s", mount.source)
		}
		options := []string{"rbind", "nosuid", "nodev"}
		if mount.readOnly {
			options = append(options, "ro")
		}
		mounts = append(mounts, map[string]any{"destination": mount.target, "type": "bind", "source": mount.source, "options": options})
	}
	volumes := r.Spec.Mounts
	if len(volumes) == 0 && r.Spec.HomeVolumeUID != "" {
		volumes = []api.VolumeMount{{UID: r.Spec.HomeVolumeUID, MountPath: "/home/agent"}}
	}
	for _, m := range volumes {
		if _, err := homevolume.Verify(p.base, m.UID, "local", homevolume.VolumeSpecHash, ""); err != nil {
			return fmt.Errorf("%w: %v", boxprovider.ErrStaleVolume, err)
		}
		source, err := homevolume.Path(p.base, m.UID)
		if err != nil {
			return err
		}
		options := []string{"rbind", "nosuid", "nodev"}
		if m.ReadOnly {
			options = append(options, "ro")
		}
		mounts = append(mounts, map[string]any{"destination": m.MountPath, "type": "bind", "source": source, "options": options})
	}
	spec["mounts"] = mounts
	spec["annotations"] = map[string]string{"cellbox.local/execution": r.Handle.ID, "cellbox.local/spec": r.Fingerprint}
	delete(spec, "hooks")
	b, err = json.Marshal(spec)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(bundle, "config.json"), b, 0600)
}
