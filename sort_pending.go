package objectstore

import "sort"

type pendingEntry struct {
	dir  string
	meta uploadMeta
}

func sortPending(in []pendingEntry) {
	sort.SliceStable(in, func(i, j int) bool {
		if in[i].meta.CreatedAt.Equal(in[j].meta.CreatedAt) {
			return in[i].meta.ID < in[j].meta.ID
		}
		return in[i].meta.CreatedAt.Before(in[j].meta.CreatedAt)
	})
}

func partName(n uint32) string {
	return formatPartNumber(n)
}
