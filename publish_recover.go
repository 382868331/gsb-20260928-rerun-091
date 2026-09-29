package objectstore

// shouldPublishOnRecover decides whether an assembled session left by a crash
// may advance the key pointer. It replays the original compare-and-set: a
// create-only upload publishes only when nothing exists; a version-match
// upload publishes only while the expected predecessor is still current.
// Otherwise another publish won, so this version stays content-addressable
// but never becomes current.
func shouldPublishOnRecover(m *uploadMeta, cur *pointer) bool {
	switch m.ExpectedVersion {
	case "":
		return cur == nil
	case "*":
		return cur != nil
	default:
		return cur != nil && cur.Version == m.ExpectedVersion
	}
}

// recoverPublish performs the locked, atomic pointer advance for a session
// left in the assembled state by a crash. A second crash inside this window
// is also safe: blob and pointer are both atomic, and reruns are idempotent.
func (s *Store) recoverPublish(m *uploadMeta) (bool, error) {
	lk, err := acquireLock(s.path(keyDir(m.Key), "lock"))
	if err != nil {
		return false, err
	}
	defer lk.release()

	cur, err := s.currentPointer(m.Key)
	if err != nil {
		return false, err
	}
	if !shouldPublishOnRecover(m, cur) {
		return false, nil
	}
	ptr := pointer{Version: m.Version, Size: m.Size, PublishedAt: nowUTC(), UploadID: m.ID}
	if err := s.writePointer(m.Key, ptr); err != nil {
		return false, err
	}
	return true, nil
}
