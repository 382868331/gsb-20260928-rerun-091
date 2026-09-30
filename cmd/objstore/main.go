// Command objstore is a minimal CLI for the local object storage engine.
//
// Usage:
//
//	objstore put        <root> <key> <file>
//	objstore get        <root> <key> [file|-]
//	objstore getversion <root> <key> <version-id>
//	objstore info       <root> <key>
//	objstore verify     <root> <key>
//	objstore delete     <root> <key>
//	objstore gc         <root> -
//	objstore snapshot   <root> <key> [version-id|-]
//	objstore snapshots  <root> <key>
//	objstore release    <root> <key> <snapshot-id>
package main

import (
	"context"
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
	case "getversion":
		if len(os.Args) != 5 {
			usage()
		}
		rd, err := s.GetVersion(key, os.Args[4])
		if err != nil {
			die(err)
		}
		if err := rd.Verify(); err != nil {
			die(err)
		}
		if _, err := io.Copy(os.Stdout, rd); err != nil {
			die(err)
		}
		rd.Close()
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
	case "delete", "rm":
		if err := s.DeleteObject(key); err != nil {
			die(err)
		}
		fmt.Printf("deleted %s (versions retained until GC)\n", key)
	case "gc":
		gc, err := s.GC(context.Background(), &localobjstore.GCOptions{})
		for _, it := range gc.Items {
			fmt.Printf("gc: %s %s %s -> %s (%s)\n", it.Kind, it.Key, it.VersionID, it.Action, it.Reason)
		}
		if err != nil {
			die(err)
		}
		if len(gc.Items) == 0 {
			fmt.Println("gc: nothing reclaimable")
		}
	case "snapshot", "snap":
		vid := "-"
		if len(os.Args) == 5 {
			vid = os.Args[4]
		}
		if vid == "-" {
			m, err := s.CurrentVersion(key)
			if err != nil {
				die(err)
			}
			vid = m.VersionID
		}
		r, err := s.Snapshot(key, vid)
		if err != nil {
			die(err)
		}
		fmt.Printf("snapshot %s -> %s %s\n", r.ID, key, r.VersionID)
	case "snapshots":
		rs, err := s.ListSnapshots(key)
		if err != nil {
			die(err)
		}
		for _, r := range rs {
			fmt.Printf("%s %s\n", r.ID, r.VersionID)
		}
	case "release":
		if len(os.Args) != 5 {
			usage()
		}
		if err := s.ReleaseSnapshot(key, os.Args[4]); err != nil {
			die(err)
		}
		fmt.Printf("released snapshot %s\n", os.Args[4])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: objstore put|get|getversion|info|verify|delete|gc|snapshot|snapshots|release <root> <key> [args]")
	os.Exit(2)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
