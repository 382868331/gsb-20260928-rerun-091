// Command objstore is a minimal CLI for the local object storage engine.
//
// Usage:
//
//	objstore put   <root> <key> <file>
//	objstore get   <root> <key> <file->
//	objstore info  <root> <key>
//	objstore verify <root> <key>
//	objstore delete <root> <key>
//	objstore pin   <root> <key> <version-id> <name>
//	objstore unpin <root> <name>
//	objstore pins  <root>
//	objstore gc    <root>
//	objstore recoveries <root>
//	objstore candidates <root> <key>
//	objstore restore    <root> <key> <version-id>
package main

import (
	"fmt"
	"io"
	"os"

	"localobjstore"
)

func main() {
	if len(os.Args) < 3 {
		usage()
	}
	cmd, root := os.Args[1], os.Args[2]
	key := ""
	if len(os.Args) >= 4 {
		key = os.Args[3]
	}
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
	case "delete":
		if key == "" {
			usage()
		}
		if err := s.DeleteObject(key); err != nil {
			die(err)
		}
		fmt.Printf("deleted %s (versions retained until unreferenced and collected)\n", key)
	case "pin":
		if len(os.Args) != 6 {
			usage()
		}
		if err := s.PinVersion(os.Args[5], key, os.Args[4]); err != nil {
			die(err)
		}
		fmt.Printf("pinned %s version=%s as %q\n", key, os.Args[4], os.Args[5])
	case "unpin":
		if key == "" {
			usage()
		}
		if err := s.UnpinVersion(key); err != nil {
			die(err)
		}
		fmt.Printf("unpinned %q\n", key)
	case "pins":
		pins, err := s.ListPins()
		if err != nil {
			die(err)
		}
		for _, p := range pins {
			fmt.Printf("%s %s %s\n", p.Name, p.Key, p.VersionID)
		}
	case "recoveries":
		recs, err := s.ListCorruptions()
		if err != nil {
			die(err)
		}
		for _, r := range recs {
			state := "resolved"
			if r.Active {
				state = "active"
			}
			fmt.Printf("%s %s damaged=%s source=%s restored=%s quarantine=%s reason=%s\n", state, r.Key, r.CorruptedVersionID, r.SourceVersionID, r.RestoredVersionID, r.QuarantinePath, r.Reason)
		}
	case "candidates":
		cs, err := s.ListRecoveryCandidates(key)
		if err != nil {
			die(err)
		}
		for _, c := range cs {
			status := "INVALID"
			if c.Verified {
				status = "OK"
			}
			fmt.Printf("%s %s size=%d sha256=%s %s\n", status, c.Manifest.VersionID, c.Manifest.Size, c.Manifest.Sha256, c.Error)
		}
	case "restore":
		if len(os.Args) != 5 {
			usage()
		}
		m, err := s.RestoreVersion(key, os.Args[4])
		if err != nil {
			die(err)
		}
		fmt.Printf("restored %s from %s as new current %s\n", key, os.Args[4], m.VersionID)
	case "gc":
		grep, err := s.CollectGarbage()
		if err != nil {
			die(err)
		}
		for _, it := range grep.Items {
			fmt.Printf("%s %s %s (%s)\n", it.Action, it.Key, it.VersionID, it.Reason)
		}
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: objstore put|get|info|verify|delete|pin|unpin|pins|gc|recoveries|candidates|restore <root> [key] [args]")
	os.Exit(2)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
