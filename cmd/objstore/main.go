// Command objstore is a small CLI for the local object storage engine. It is
// intentionally thin: the package (see README) is the product; the CLI makes
// multipart upload, read, verify and recovery inspectable on disk.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	obj "localobjectstore"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	root := fs.String("root", envOr("OBJSTORE_ROOT", "./objstore"), "store root directory")
	key := fs.String("key", "", "object key")
	file := fs.String("file", "", "source file (default stdin)")
	ifMatch := fs.String("if-match", "", "optimistic concurrency: expected current version (use ! for create-only)")
	_ = fs.Parse(os.Args[2:])

	s, err := obj.Open(context.Background(), *root)
	if err != nil {
		fatal("open", err)
	}
	defer s.Close()

	switch cmd {
	case "put":
		putObject(s, *key, *file, *ifMatch)
	case "get":
		getObject(s, *key)
	case "get-version":
		getVersion(s, fs.Args())
	case "stat":
		statObject(s, *key)
	case "verify":
		verifyObject(s, fs.Args())
	case "recover":
		r, err := s.Recover(context.Background())
		if err != nil {
			fatal("recover", err)
		}
		fmt.Printf("published=%d quarantined=%d\n", r.Published, len(r.Quarantined))
	default:
		usage()
		os.Exit(2)
	}
}

func putObject(s *obj.Store, key, file, ifMatch string) {
	if key == "" {
		fmt.Fprintln(os.Stderr, "missing -key")
		os.Exit(2)
	}
	var r io.Reader = os.Stdin
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			fatal("open file", err)
		}
		defer f.Close()
		r = f
	}
	var opts []obj.UploadOption
	if ifMatch != "" {
		if ifMatch == "!" {
			ifMatch = ""
		}
		opts = append(opts, obj.IfMatch(ifMatch))
	}
	u, err := s.CreateUpload(key, opts...)
	if err != nil {
		fatal("create upload", err)
	}
	part, err := u.PutPart(1, r)
	if err != nil {
		fatal("put part", err)
	}
	v, err := u.CompleteUpload()
	if err != nil {
		fatal("complete", err)
	}
	fmt.Printf("version=%s size=%d part-sha256=%s\n", v.Version, v.Size, part.SHA256)
}

func getObject(s *obj.Store, key string) {
	v, rc, err := s.ReadObject(key)
	if err != nil {
		fatal("read", err)
	}
	defer rc.Close()
	if _, err := io.Copy(os.Stdout, rc); err != nil {
		fatal("stream", err)
	}
	_ = v
}

func getVersion(s *obj.Store, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: get-version <version-id>")
		os.Exit(2)
	}
	rc, err := s.ReadVersion(args[0])
	if err != nil {
		fatal("read version", err)
	}
	defer rc.Close()
	if _, err := io.Copy(os.Stdout, rc); err != nil {
		fatal("stream", err)
	}
}

func statObject(s *obj.Store, key string) {
	v, err := s.CurrentVersion(key)
	if err != nil {
		fatal("stat", err)
	}
	fmt.Printf("key=%s version=%s size=%d published=%s\n", v.Key, v.Version, v.Size, v.PublishedAt.Format("2006-01-02T15:04:05Z07:00"))
}

func verifyObject(s *obj.Store, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: verify <version-id>")
		os.Exit(2)
	}
	if err := s.VerifyVersion(args[0]); err != nil {
		fatal("verify", err)
	}
	fmt.Println("OK")
}

func fatal(stage string, err error) {
	status := 1
	switch {
	case errors.Is(err, obj.ErrNotFound):
		status = 4
	case errors.Is(err, obj.ErrConflict):
		status = 5
	case errors.Is(err, obj.ErrCorrupt):
		status = 6
	}
	fmt.Fprintf(os.Stderr, "%s: %v\n", stage, err)
	os.Exit(status)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func usage() {
	fmt.Fprintln(os.Stderr, `objstore - local object storage engine

usage:
  objstore put     -root DIR -key K [-file F] [-if-match V|!]
  objstore get     -root DIR -key K
  objstore stat    -root DIR -key K
  objstore get-version -root DIR <version-id>
  objstore verify   -root DIR <version-id>
  objstore recover  -root DIR`)
}
