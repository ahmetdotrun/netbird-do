//go:build ignore

// Collect the license notices shipped with modules linked into the WASM client.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

func main() {
	if err := notices(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func notices() error {
	type module struct{ Path, Version, Dir string }
	modules := map[string]module{"Go": {Path: "Go", Version: runtime.Version(), Dir: runtime.GOROOT()}}
	decoder := json.NewDecoder(os.Stdin)
	for {
		var pkg struct{ Module *module }
		if err := decoder.Decode(&pkg); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if m := pkg.Module; m != nil && m.Version != "" {
			modules[m.Path] = *m
		}
	}
	if _, ok := modules["github.com/netbirdio/netbird"]; !ok {
		return fmt.Errorf("dependency list does not include NetBird")
	}
	paths := make([]string, 0, len(modules))
	for path := range modules {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	fmt.Println("Third-party license notices for the NetBird WASM client.")
	for _, path := range paths {
		m := modules[path]
		entries, err := os.ReadDir(m.Dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			name := strings.ToUpper(entry.Name())
			if entry.IsDir() || !(strings.HasPrefix(name, "LICENSE") || strings.HasPrefix(name, "LICENCE") || strings.HasPrefix(name, "COPYING") || strings.HasPrefix(name, "NOTICE") || name == "AUTHORS" || name == "PATENTS") {
				continue
			}
			text, err := os.ReadFile(filepath.Join(m.Dir, entry.Name()))
			if err != nil {
				return err
			}
			fmt.Printf("\n--- %s %s / %s ---\n\n%s\n", m.Path, m.Version, entry.Name(), text)
		}
	}
	return nil
}
