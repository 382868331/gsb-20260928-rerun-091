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
	case "group-create":
		// objstore group-create <root> <name> <key[:ref1,ref2]>...
		if len(os.Args) < 5 {
			usage()
		}
		var specs []localobjstore.GroupMemberSpec
		for _, arg := range os.Args[4:] {
			sp := localobjstore.GroupMemberSpec{Key: arg}
			if i := indexByte(arg, ':'); i >= 0 {
				sp.Key = arg[:i]
				for _, r := range splitComma(arg[i+1:]) {
					sp.Refs = append(sp.Refs, r)
				}
			}
			specs = append(specs, sp)
		}
		rp, err := s.CreateRecoveryPoint(key, specs)
		if err != nil {
			die(err)
		}
		fmt.Printf("confirmed recovery point %q id=%s members=%d\n", rp.Name, rp.ID, len(rp.Members))
	case "groups":
		ps, err := s.ListRecoveryPoints()
		if err != nil {
			die(err)
		}
		for _, p := range ps {
			fmt.Printf("%s id=%s members=%d created=%s\n", p.Name, p.ID, len(p.Members), p.CreatedAt.Format("2006-01-02T15:04:05Z"))
		}
	case "group-show":
		rp, err := s.DescribeRecoveryPoint(key)
		if err != nil {
			die(err)
		}
		fmt.Printf("recovery point %q id=%s created=%s\n", rp.Name, rp.ID, rp.CreatedAt.Format("2006-01-02T15:04:05Z"))
		for _, m := range rp.Members {
			fmt.Printf("  %s version=%s sha256=%s refs=%v\n", m.Key, m.VersionID, m.Sha256, m.Refs)
		}
	case "group-diff":
		if len(os.Args) != 5 {
			usage()
		}
		d, err := s.DiffRecoveryPoints(key, os.Args[4])
		if err != nil {
			die(err)
		}
		for _, m := range d.Added {
			fmt.Printf("+ %s version=%s\n", m.Key, m.VersionID)
		}
		for _, m := range d.Removed {
			fmt.Printf("- %s version=%s\n", m.Key, m.VersionID)
		}
		for _, c := range d.Changed {
			fmt.Printf("~ %s %s -> %s refs+%v refs-%v\n", c.Key, c.FromVersion, c.ToVersion, c.RefsAdded, c.RefsRemoved)
		}
	case "group-plan":
		plan, err := s.PlanGroupRestore(key)
		if err != nil {
			die(err)
		}
		for _, mh := range plan.Members {
			if mh.Status == "ready" {
				fmt.Printf("READY     %s version=%s\n", mh.Member.Key, mh.Member.VersionID)
			} else {
				fmt.Printf("%-9s %s version=%s (%v)\n", mh.Status, mh.Member.Key, mh.Member.VersionID, mh.Err)
			}
		}
		if plan.Complete() {
			fmt.Println("plan: complete, all members restorable")
		} else {
			fmt.Println("plan: incomplete, restore needs --partial or fails")
		}
	case "group-restore":
		// objstore group-restore <root> <name> [--partial]
		partial := len(os.Args) == 5 && os.Args[4] == "--partial"
		rep2, err := s.RestoreGroup(key, partial)
		if err != nil {
			for _, mh := range rep2.Plan.Members {
				fmt.Fprintf(os.Stderr, "%s %s (%v)\n", mh.Status, mh.Member.Key, mh.Err)
			}
			die(err)
		}
		j := rep2.Journal
		fmt.Printf("restore %s of %q: %s\n", j.ID, j.GroupName, j.Status)
		for _, m := range j.Members {
			fmt.Printf("  %-9s %s %s -> %s %s\n", m.Status, m.Key, m.SourceVersion, m.NewVersion, m.Reason)
		}
	case "group-delete":
		drep, err := s.DeleteRecoveryPoint(key)
		if err != nil {
			die(err)
		}
		fmt.Printf("deleted recovery point %q\n", drep.Name)
		for _, m := range drep.Released {
			fmt.Printf("  released %s %s (now reclaimable)\n", m.Key, m.VersionID)
		}
		for _, r := range drep.Retained {
			fmt.Printf("  retained %s %s held by %v\n", r.Key, r.VersionID, r.HeldBy)
		}
	case "group-journals":
		js, err := s.ListRestoreJournals()
		if err != nil {
			die(err)
		}
		for _, j := range js {
			fmt.Printf("%s group=%s status=%s started=%s\n", j.ID, j.GroupName, j.Status, j.StartedAt.Format("2006-01-02T15:04:05Z"))
			for _, m := range j.Members {
				fmt.Printf("  %-9s %s %s -> %s %s\n", m.Status, m.Key, m.SourceVersion, m.NewVersion, m.Reason)
			}
		}
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: objstore put|get|info|verify|delete|fault|candidates|restore|pin|unpin|pins|gc|group-create|groups|group-show|group-diff|group-plan|group-restore|group-delete|group-journals <root> [key] [args]")
	os.Exit(2)
}

func indexByte(str string, b byte) int {
	for i := 0; i < len(str); i++ {
		if str[i] == b {
			return i
		}
	}
	return -1
}

func splitComma(str string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(str); i++ {
		if i == len(str) || str[i] == ',' {
			if i > start {
				out = append(out, str[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
