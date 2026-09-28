package localobjstore

import (
	"io"
	"os"
	"os/exec"
)

type helperProcess struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
}

func (h *helperProcess) Start() error { return h.cmd.Start() }

func newTestHelperProcess(dir string) *helperProcess {
	cmd := exec.Command(os.Args[0], "-test.run=TestSubprocessKilledMidUpload")
	cmd.Env = append(os.Environ(), "OBJSTORE_CRASH_CHILD=1", "OBJSTORE_DIR="+dir)
	r, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr
	return &helperProcess{cmd: cmd, stdout: r}
}
