package localobjstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// validateDir checks a version directory structurally: identity fields, part
// numbering, recorded sizes and present part files.
func validateDir(versionDir string, m Manifest) error {
	if m.VersionID == "" || len(m.Parts) == 0 {
		return fmt.Errorf("%w: empty manifest", ErrCorrupted)
	}
	if filepath.Base(versionDir) != m.VersionID {
		return fmt.Errorf("%w: version id directory mismatch", ErrCorrupted)
	}
	var total int64
	for i, p := range m.Parts {
		if p.Number != int32(i+1) {
			return fmt.Errorf("%w: part numbering gap at %d", ErrCorrupted, i+1)
		}
		path := filepath.Join(versionDir, dataDirName, partFileName(p.Number))
		st, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("%w: missing part %d: %v", ErrCorrupted, p.Number, err)
		}
		if st.Size() != p.Size {
			return fmt.Errorf("%w: part %d size mismatch", ErrCorrupted, p.Number)
		}
		total += p.Size
	}
	if total != m.Size {
		return fmt.Errorf("%w: manifest size mismatch", ErrCorrupted)
	}
	return nil
}

// verifyDirBytes hashes each part and the concatenated whole-object digest.
func verifyDirBytes(versionDir string, m Manifest) error {
	total := sha256.New()
	var size int64
	for _, p := range m.Parts {
		path := filepath.Join(versionDir, dataDirName, partFileName(p.Number))
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupted, err)
		}
		h := sha256.New()
		n, err := io.Copy(h, f)
		f.Close()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupted, err)
		}
		if n != p.Size || hex.EncodeToString(h.Sum(nil)) != p.Sha256 {
			return fmt.Errorf("%w: part %d checksum mismatch", ErrCorrupted, p.Number)
		}
		f2, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupted, err)
		}
		_, _ = io.Copy(total, f2)
		f2.Close()
		size += n
	}
	if size != m.Size || hex.EncodeToString(total.Sum(nil)) != m.Sha256 {
		return fmt.Errorf("%w: whole-object checksum mismatch", ErrCorrupted)
	}
	return nil
}
