package objectstore

import "fmt"

func formatPartNumber(n uint32) string { return fmt.Sprintf("%08d", n) }

func sanitizeReason(r string) string {
	for _, c := range r {
		if c == '/' || c == ' ' {
			return "fragment"
		}
	}
	if r == "" {
		return "fragment"
	}
	return r
}
