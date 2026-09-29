package objectstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestCrashDuringReceivingIsQuarantined(t *testing.T) {
	if os.Getenv("OBJECTSTORE_HELPER2") == "1" {
		s, _ := Open(context.Background(), os.Getenv("OBJECTSTORE_ROOT"))
		u, _ := s.CreateUpload("k")
		_, _ = u.PutPart(1, bytes.NewReader([]byte("only-half")))
		// Process exits without completing.
		os.Exit(42)
	}
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashDuringReceivingIsQuarantined$")
	cmd.Env = append(os.Environ(), "OBJECTSTORE_HELPER2=1", "OBJECTSTORE_ROOT="+root)
	_ = cmd.Run()
	s, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReadObject("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("half object visible: %v", err)
	}
	q, err := os.ReadDir(filepath.Join(root, "quarantine"))
	if err != nil || len(q) != 1 {
		t.Fatalf("expected quarantine, got %v %v", q, err)
	}
}

func TestCrossProcessConcurrentPublish(t *testing.T) {
	if os.Getenv("OBJECTSTORE_HELPER2") == "1" {
		s, oerr := Open(context.Background(), os.Getenv("OBJECTSTORE_ROOT"))
		if oerr != nil {
			os.Stderr.WriteString("open: " + oerr.Error() + "\n")
			os.Exit(20)
		}
		v0, err := s.CurrentVersion("cp-key")
		if err != nil {
			os.Stderr.WriteString("current: " + err.Error() + "\n")
			os.Exit(21)
		}
		u, cerr := s.CreateUpload("cp-key", IfMatch(v0.Version))
		if cerr != nil {
			os.Stderr.WriteString("create: " + cerr.Error() + "\n")
			os.Exit(22)
		}
		payload := os.Getenv("OBJECTSTORE_PAYLOAD")
		if payload == "" {
			os.Stderr.WriteString("empty payload env\n")
			os.Exit(5)
		}
		if _, perr := u.PutPart(1, strings.NewReader(payload)); perr != nil {
			os.Stderr.WriteString("putpart: " + perr.Error() + "\n")
			os.Exit(6)
		}
		if _, err := u.CompleteUpload(); err != nil {
			if errors.Is(err, ErrConflict) {
				os.Exit(3)
			}
			os.Stderr.WriteString("complete: " + err.Error() + "\n")
			os.Exit(4)
		}
		os.Exit(0)
	}

	root := t.TempDir()
	// Seed the object.
	s0, _ := Open(context.Background(), root)
	complete(t, s0, "cp-key", [][]byte{[]byte("seed")})
	s0.Close()

	const n = 6
	var wg sync.WaitGroup
	codes := make([]int, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrossProcessConcurrentPublish$")
			cmd.Env = append(os.Environ(),
				"OBJECTSTORE_HELPER2=1",
				"OBJECTSTORE_ROOT="+root,
				"OBJECTSTORE_PAYLOAD=payload-"+string(rune('A'+i)),
			)
			var out bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &out
			err := cmd.Run()
			if e, ok := err.(*exec.ExitError); ok {
				codes[i] = e.ExitCode()
				if e.ExitCode() != 3 {
					t.Logf("helper %d output: %s", i, out.String())
				}
			} else if err == nil {
				codes[i] = 0
			}
		}(i)
	}
	wg.Wait()

	wins, conflicts := 0, 0
	for _, c := range codes {
		switch c {
		case 0:
			wins++
		case 3:
			conflicts++
		default:
			t.Fatalf("unexpected helper exit %d", c)
		}
	}
	if wins != 1 || conflicts != n-1 {
		t.Fatalf("cross-process: want 1 win %d conflicts, got %d/%d", n-1, wins, conflicts)
	}
	s, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	_, data := readAll(t, s, "cp-key")
	if !strings.HasPrefix(string(data), "payload-") {
		t.Fatalf("unexpected winner content %q", data)
	}
}
