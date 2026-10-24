package localrunsc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (p *Provider) prepareBoot() error {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return err
	}
	p.bootID = strings.TrimSpace(string(boot))
	if len(p.bootID) != 36 || !identifier.MatchString(p.bootID) {
		return errors.New("invalid host boot identity")
	}
	root := filepath.Join(p.base, "runsc")
	marker := filepath.Join(root, ".boot-id")
	previous, err := os.ReadFile(marker)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	oldBoot := strings.TrimSpace(string(previous))
	if oldBoot == p.bootID {
		return nil
	}
	if oldBoot != "" {
		if len(oldBoot) != 36 || !identifier.MatchString(oldBoot) {
			return errors.New("invalid saved runtime boot identity")
		}
		// Never ask runsc to kill a PID saved by an earlier boot: the host may
		// have reused it. Keep old metadata/logs for inspection and restore into
		// a fresh runtime root, with the original container identity.
		if err := os.Rename(root, filepath.Join(p.base, "runsc-before-"+oldBoot)); err != nil {
			return fmt.Errorf("archive runtime metadata from previous boot: %w", err)
		}
		if err := os.Mkdir(root, 0700); err != nil {
			return err
		}
		p.bootChanged = true
	}
	f, err := os.OpenFile(marker+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.WriteString(p.bootID + "\n")
	if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err := os.Rename(marker+".tmp", marker); err != nil {
		return err
	}
	for _, path := range []string{root, p.base} {
		dir, err := os.Open(path)
		if err != nil {
			return err
		}
		if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
			return err
		}
	}
	return nil
}
