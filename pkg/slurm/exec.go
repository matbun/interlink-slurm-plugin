package slurm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"al.essio.dev/pkg/shellescape"
	"github.com/gorilla/mux"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	utilexec "k8s.io/utils/exec"
)

// CapabilityExec is listed in the ping answer when ExecPrefix is set: the
// plugin answers /exec.
const CapabilityExec = "exec"

// execScriptName is the script, in a pod's files directory, that runs a
// command in container name as the job runs that container: its runtime,
// options, environment, mounts and image, with the command given in place of
// the container's own.
func execScriptName(name string) string { return "exec-" + name + ".sh" }

// writeExecScript writes the exec script of container name, whose runtime
// command is runtimeCommand as Create.go assembles it, for a job that runs in
// workDir: CommandPrefix first, as the job's script runs it (a module load
// that puts the runtime on PATH, for example). Nothing is written when the
// site has no ExecPrefix.
func writeExecScript(config SlurmConfig, filesPath, workDir, name string, runtimeCommand []string) error {
	if len(config.ExecPrefix) == 0 {
		return nil
	}
	cmd := append([]string{}, runtimeCommand...)
	// singularity run would start the image's entrypoint instead of the command
	if config.ContainerRuntime != "enroot" && len(cmd) > 1 && cmd[1] == "run" {
		cmd[1] = "exec"
	}
	script := "#!/bin/bash\n" +
		"# kubectl exec into container " + name + ": the command given, in the container's runtime\n" +
		"cd " + shellescape.Quote(workDir) + " || exit 1\n"
	if strings.TrimSpace(config.Commandprefix) != "" {
		script += config.Commandprefix + "\n"
	}
	script += "exec " + strings.Join(cmd, " ") + ` "$@"` + "\n"
	return os.WriteFile(filepath.Join(filesPath, execScriptName(name)), []byte(script), 0o700)
}

var (
	execName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)
	execNode = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9.]*[A-Za-z0-9])?$`)
)

// ExecHandler answers /exec with the kubelet's exec protocol (the
// virtual-kubelet library's handler) for container container of the pod of
// UID pod in namespace, whose job runs on compute node node: the command runs
// on that node through ExecPrefix, in the container's runtime
// (writeExecScript). Query: namespace, pod, container and node, then the
// kubelet's command, tty, input, output and error.
func (h *SidecarHandler) ExecHandler(w http.ResponseWriter, r *http.Request) {
	if len(h.Config.ExecPrefix) == 0 {
		http.Error(w, "this site runs no command on a compute node (the plugin has no ExecPrefix)", http.StatusNotImplemented)
		return
	}
	q := r.URL.Query()
	namespace, uid, container, node := q.Get("namespace"), q.Get("pod"), q.Get("container"), q.Get("node")
	if !execName.MatchString(namespace) || !execName.MatchString(uid) || !execName.MatchString(container) || !execNode.MatchString(node) {
		http.Error(w, "namespace, pod, container and node are required, as Kubernetes and host names", http.StatusBadRequest)
		return
	}
	// the pod's files directory, at the same path on the node as the job's script
	script := filepath.Join(h.Config.DataRootFolder+namespace+"-"+uid, execScriptName(container))
	if _, err := os.Stat(script); err != nil {
		http.Error(w, fmt.Sprintf("container %s of pod %s has no exec script here: its job was not submitted by this plugin with ExecPrefix set", container, uid), http.StatusNotFound)
		return
	}
	r = mux.SetURLVars(r, map[string]string{"namespace": namespace, "pod": uid, "container": container})
	vkapi.HandleContainerExec(func(ctx context.Context, _, _, _ string, cmd []string, attach vkapi.AttachIO) error {
		return h.runExec(ctx, node, script, cmd, attach)
	})(w, r)
}

// runExec runs ExecPrefix, then node, then the shell line that runs cmd
// through script, with the exec's streams; EXEC_TTY is 1 in its environment
// when a terminal is asked for, and the line first sets the terminal's size.
// A non-zero exit is the command's exit code.
func (h *SidecarHandler) runExec(ctx context.Context, node, script string, cmd []string, attach vkapi.AttachIO) error {
	words := []string{"exec", "bash", shellescape.Quote(script)}
	for _, c := range cmd {
		words = append(words, shellescape.Quote(c))
	}
	line, tty := strings.Join(words, " "), "0"
	if attach.TTY() {
		tty = "1"
		if s, ok := firstSize(ctx, attach.Resize()); ok {
			line = fmt.Sprintf("stty cols %d rows %d 2>/dev/null; %s", s.Width, s.Height, line)
		}
	}
	args := append(append([]string{}, h.Config.ExecPrefix[1:]...), node, line)
	c := exec.CommandContext(ctx, h.Config.ExecPrefix[0], args...)
	c.Env = append(os.Environ(), "EXEC_TTY="+tty)
	if in := attach.Stdin(); in != nil {
		c.Stdin = in
	}
	if out := attach.Stdout(); out != nil {
		c.Stdout = out
	}
	if errOut := attach.Stderr(); errOut != nil {
		c.Stderr = errOut
	}
	err := c.Run()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.Exited() {
		return utilexec.CodeExitError{Err: err, Code: exitErr.ExitCode()}
	}
	return err
}

// firstSize is the terminal's first size, which the client sends as it
// starts; the later ones are drained, so a terminal resized afterwards keeps
// its first size.
func firstSize(ctx context.Context, sizes <-chan vkapi.TermSize) (vkapi.TermSize, bool) {
	if sizes == nil {
		return vkapi.TermSize{}, false
	}
	var first vkapi.TermSize
	ok := false
	select {
	case first, ok = <-sizes:
	case <-time.After(time.Second):
	case <-ctx.Done():
	}
	go func() {
		for {
			select {
			case _, more := <-sizes:
				if !more {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return first, ok
}
