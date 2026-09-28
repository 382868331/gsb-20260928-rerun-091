// Command objstore is a minimal CLI for the local object storage engine.
//
// Usage:
//
//	objstore put   <root> <key> <file>
//	objstore get   <root> <key> <file->
//	objstore info  <root> <key>
//	objstore verify <root> <key>
package main

import (
	"fmt"
	"io"
	"os"

	"localobjstore"
)

func main() {
	if len(os.Args) < 4 {
		usage()
	}
	cmd, root, key := os.Args[1], os.Args[2], os.Args[3]
	s, rep, err := localobjstore.Open(root, nil)
	if err != nil {
		die(err)
	}
	for _, it := range rep.Items {
		fmt.Fprintf(os.Stderr, "recovery: %s %s -> %s (%s)\n", it.Kind, it.Path, it.Action, it.Reason)
	}
	defer s.Close()

	switch cmd {
	case "put":
		if len(os.Args) != 5 {
			usage()
		}
		f, err := os.Open(os.Args[4])
		if err != nil {
			die(err)
		}
		defer f.Close()
		m, err := s.PutObject(key, f, nil)
		if err != nil {
			die(err)
		}
		fmt.Printf("published %s version=%s size=%d sha256=%s\n", key, m.VersionID, m.Size, m.Sha256)
	case "get":
		r, err := s.GetObject(key)
		if err != nil {
			die(err)
		}
		defer r.Close()
		if err := r.Verify(); err != nil {
			die(err)
		}
		w := os.Stdout
		if len(os.Args) == 5 && os.Args[4] != "-" {
			w, err = os.Create(os.Args[4])
			if err != nil {
				die(err)
			}
			defer w.Close()
		}
		if _, err := io.Copy(w, r); err != nil {
			die(err)
		}
	case "info":
		vs, err := s.ListVersions(key)
		if err != nil {
			die(err)
		}
		cur, _ := s.CurrentVersion(key)
		for _, m := range vs {
			marker := " "
			if m.VersionID == cur.VersionID {
				marker = "*"
			}
			fmt.Printf("%s %s size=%d parts=%d\n", marker, m.VersionID, m.Size, len(m.Parts))
		}
	case "verify":
		r, err := s.GetObject(key)
		if err != nil {
			die(err)
		}
		defer r.Close()
		if err := r.Verify(); err != nil {
			die(err)
		}
		fmt.Println("OK: checksums match")
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: objstore put|get|info|verify <root> <key> [file]")
	os.Exit(2)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
