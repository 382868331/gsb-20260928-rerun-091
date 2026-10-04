// Command objstore is a minimal CLI for the local object storage engine.
//
// Usage:
//
//	objstore put   <root> <key> <file>
//	objstore get   <root> <key> <file->
//	objstore info  <root> <key>
//	objstore verify <root> <key>
//	objstore delete <root> <key>
//	objstore fault <root> <key>
//	objstore candidates <root> <key>
//	objstore restore <root> <key> <version-id>
//	objstore pin   <root> <key> <version-id> <name>
//	objstore unpin <root> <name>
//	objstore pins  <root>
//	objstore gc    <root>
//	objstore rp-create <root> <name> <key1,key2,...>
//	objstore rp-list   <root>
//	objstore rp-show   <root> <name>
//	objstore rp-diff   <root> <name-a> <name-b>
//	objstore rp-inspect <root> <name>
//	objstore rp-restore <root> <name> [--partial]
//	objstore rp-resume  <root> <restore-id>
//	objstore rp-delete  <root> <name>
//	objstore restores   <root>
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

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
	case "fault":
		if key == "" {
			usage()
		}
		rec, ok, err := s.Fault(key)
		if err != nil {
			die(err)
		}
		if !ok {
			fmt.Printf("no fault recorded for %s\n", key)
			break
		}
		fmt.Printf("FAULT %s current version %s quarantined as corrupt at %s\n  reason: %s\n  restore with: objstore restore %s %s <version-id>\n",
			key, rec.VersionID, rec.DetectedAt.Format("2006-01-02T15:04:05Z"), rec.Reason, root, key)
	case "candidates":
		if key == "" {
			usage()
		}
		vs, err := s.InspectVersions(key)
		if err != nil {
			die(err)
		}
		for _, st := range vs {
			if st.Verified {
				fmt.Printf("OK   %s size=%d parts=%d\n", st.Manifest.VersionID, st.Manifest.Size, len(st.Manifest.Parts))
			} else {
				fmt.Printf("BAD  %s (%v)\n", st.Manifest.VersionID, st.Err)
			}
		}
	case "restore":
		if len(os.Args) != 5 {
			usage()
		}
		m, err := s.RestoreVersion(key, os.Args[4])
		if err != nil {
			die(err)
		}
		fmt.Printf("restored %s as new version=%s from=%s size=%d\n", key, m.VersionID, m.RestoredFrom, m.Size)
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
	case "gc":
		grep, err := s.CollectGarbage()
		if err != nil {
			die(err)
		}
		for _, it := range grep.Items {
			fmt.Printf("%s %s %s (%s)\n", it.Action, it.Key, it.VersionID, it.Reason)
		}
	case "rp-create":
		if len(os.Args) != 5 {
			usage()
		}
		var members []localobjstore.RestoreMemberInput
		for _, k := range strings.Split(os.Args[4], ",") {
			members = append(members, localobjstore.RestoreMemberInput{Key: k})
		}
		rp, err := s.CreateRestorePoint(key, members)
		if err != nil {
			die(err)
		}
		fmt.Printf("created restore point %q id=%s members=%d\n", rp.Name, rp.ID, len(rp.Members))
	case "rp-list":
		points, err := s.ListRestorePoints()
		if err != nil {
			die(err)
		}
		for _, rp := range points {
			fmt.Printf("%s id=%s members=%d created=%s\n", rp.Name, rp.ID, len(rp.Members), rp.CreatedAt.Format("2006-01-02T15:04:05Z"))
		}
	case "rp-show":
		if key == "" {
			usage()
		}
		rp, err := s.GetRestorePoint(key)
		if err != nil {
			die(err)
		}
		fmt.Printf("restore point %q id=%s created=%s\n", rp.Name, rp.ID, rp.CreatedAt.Format("2006-01-02T15:04:05Z"))
		for _, m := range rp.Members {
			refs := "-"
			if len(m.References) > 0 {
				refs = strings.Join(m.References, ",")
			}
			fmt.Printf("  %s version=%s refs=%s\n", m.Key, m.VersionID, refs)
		}
	case "rp-diff":
		if len(os.Args) != 5 {
			usage()
		}
		diff, err := s.DiffRestorePoints(key, os.Args[4])
		if err != nil {
			die(err)
		}
		if len(diff.MemberChanges) == 0 && len(diff.ReferenceChanges) == 0 {
			fmt.Println("no differences")
		}
		for _, c := range diff.MemberChanges {
			switch c.Change {
			case "added":
				fmt.Printf("added   %s -> %s\n", c.Key, c.ToVersion)
			case "removed":
				fmt.Printf("removed %s <- %s\n", c.Key, c.FromVersion)
			default:
				fmt.Printf("changed %s %s -> %s\n", c.Key, c.FromVersion, c.ToVersion)
			}
		}
		for _, c := range diff.ReferenceChanges {
			switch c.Change {
			case "added":
				fmt.Printf("ref added    %s -> %s\n", c.From, c.To)
			case "removed":
				fmt.Printf("ref removed  %s -> %s\n", c.From, c.To)
			default:
				fmt.Printf("ref reversed %s -> %s (now %s -> %s)\n", c.From, c.To, c.To, c.From)
			}
		}
	case "rp-inspect":
		if key == "" {
			usage()
		}
		plan, err := s.InspectRestorePoint(key)
		if err != nil {
			die(err)
		}
		for _, h := range plan.Members {
			if h.Status == "readable" {
				fmt.Printf("OK        %s %s\n", h.Key, h.VersionID)
			} else {
				fmt.Printf("%-9s %s %s (%v)\n", strings.ToUpper(h.Status), h.Key, h.VersionID, h.Err)
			}
		}
		if plan.Complete {
			fmt.Println("complete: full restore possible")
		} else {
			fmt.Println("incomplete: full restore will fail; partial restore leaves gaps")
		}
	case "rp-restore":
		if key == "" {
			usage()
		}
		var opts *localobjstore.RestoreOptions
		if len(os.Args) == 5 && os.Args[4] == "--partial" {
			opts = &localobjstore.RestoreOptions{AllowPartial: true}
		}
		rec, err := s.RestorePoint(key, opts)
		if err != nil {
			die(err)
		}
		printRestore(rec)
	case "rp-resume":
		if key == "" {
			usage()
		}
		rec, err := s.ResumeRestore(key)
		if err != nil {
			die(err)
		}
		printRestore(rec)
	case "rp-delete":
		if key == "" {
			usage()
		}
		drep, err := s.DeleteRestorePoint(key)
		if err != nil {
			die(err)
		}
		for _, m := range drep.Retained {
			fmt.Printf("retained %s %s (%s)\n", m.Key, m.VersionID, strings.Join(m.Reasons, ", "))
		}
		for _, m := range drep.Released {
			fmt.Printf("released %s %s (reclaimable by gc)\n", m.Key, m.VersionID)
		}
	case "restores":
		recs, err := s.ListRestores()
		if err != nil {
			die(err)
		}
		for _, rec := range recs {
			fmt.Printf("%s point=%s state=%s\n", rec.ID, rec.PointName, rec.State)
		}
	default:
		usage()
	}
}
func printRestore(rec localobjstore.RestoreRecord) {
	fmt.Printf("restore %s point=%q state=%s\n", rec.ID, rec.PointName, rec.State)
	for _, m := range rec.Members {
		line := fmt.Sprintf("  %-9s %s %s", m.State, m.Key, m.VersionID)
		if m.Reason != "" {
			line += " (" + m.Reason + ")"
		}
		fmt.Println(line)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: objstore put|get|info|verify|delete|fault|candidates|restore|pin|unpin|pins|gc|rp-create|rp-list|rp-show|rp-diff|rp-inspect|rp-restore|rp-resume|rp-delete|restores <root> [key] [args]")
	os.Exit(2)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
