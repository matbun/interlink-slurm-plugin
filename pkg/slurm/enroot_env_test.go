package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A value that breaks or changes if a shell parses it.
const envTestSpecial = `a  b 'c' "d" $HOME ${X:-y} $(touch pwned) ` + "`touch pwned`" + ` \ * ;|&`

// enrootEnvfmt is common::envfmt of enroot 4.2.1 (src/common.sh), which enroot runs over
// the container's environment file twice (src/runtime.sh, end of _do_environ and of
// _do_hooks): among others each pass removes one pair of quotes around a value.
const enrootEnvfmt = `enroot_envfmt() {
  sed -i -e 's/^[[:space:]]\+//' \
    -e '/^#\|^$/d' \
    -e '/^[[:alpha:]_][[:alnum:]_]*=/!d' \
    -e '/^ENROOT_/d' \
    -e 's/^\([[:alpha:]_][[:alnum:]_]*\)=[\"\x27]\(.*\)[\"\x27][[:space:]]*$/\1=\2/' \
    "$1"
}
`

// fakeEnroot stands in for enroot: import and create do nothing, and start sources the
// last --conf and writes what its environ() prints to $FAKE_ENROOT_ENV.
const fakeEnroot = `#!/bin/bash
set -euo pipefail
[ "$1" = start ] || exit 0
shift
conf=
while [ $# -gt 0 ]; do
  case "$1" in
  -c|--conf) conf=$2; shift 2 ;;
  --conf=*) conf=${1#*=}; shift ;;
  -e|--env|-m|--mount) shift 2 ;;
  --) shift; break ;;
  -?*) shift ;;
  *) break ;;
  esac
done
[ -n "$conf" ] && source "$conf"
environ > "$FAKE_ENROOT_ENV"
`

func envTestContainer() v1.Container {
	return v1.Container{
		Name:  testContainer,
		Image: busyboxImage,
		Env: []v1.EnvVar{
			{Name: "PLAIN", Value: "hello"},
			{Name: "SPECIAL", Value: envTestSpecial},
			{Name: "EMPTY", Value: ""},
			{Name: "MASTER_PORT", Value: "1234"},
			{Name: "DQ", Value: `"quoted"`},
			{Name: "SQ", Value: `'single'`},
			{Name: "MIXQ", Value: `'a' and "b"`},
			{Name: "TRAILSP", Value: `"x"   `},
			{Name: "JSONSTR", Value: `"{\"k\": \"v\"}"`},
			{Name: "BSLASH", Value: `\a\`},
			{Name: "DBSLASH", Value: `\\`},
			{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"},
			{Name: "MULTI", Value: "x\nINJECTED=1"},
			{Name: "bad.name", Value: "v"},
		},
	}
}

func envTestPodData(container v1.Container) commonIL.RetrievedPodData {
	return commonIL.RetrievedPodData{
		Pod: v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "envpod", Namespace: metav1.NamespaceDefault, UID: "1b2c3d4e-5f60-4711-8899-aabbccddeeff"},
			Spec:       v1.PodSpec{Containers: []v1.Container{container}},
		},
		Containers: []commonIL.RetrievedContainer{{Name: container.Name}},
	}
}

func requireBash(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
}

// enrootTestConfig writes the fake enroot and returns a config that uses it.
func enrootTestConfig(t *testing.T) SlurmConfig {
	t.Helper()
	fake := filepath.Join(t.TempDir(), "enroot")
	if err := os.WriteFile(fake, []byte(fakeEnroot), 0o755); err != nil {
		t.Fatal(err)
	}
	return SlurmConfig{ContainerRuntime: "enroot", EnrootPath: fake, EnrootDefaultOptions: []string{"--rw"}, BashPath: "/bin/bash"}
}

// enrootRuntimeCommand assembles an enroot container command as Create.go does.
func enrootRuntimeCommand(config SlurmConfig, data commonIL.RetrievedPodData, container v1.Container, workDir string) []string {
	runtimeCommand := prepareRuntimeCommand(config, container, data.Pod.ObjectMeta)
	runtimeCommand = append(runtimeCommand, prepareEnvs(context.Background(), config, data, container, workDir)...)
	return append(runtimeCommand, "", container.Name+string(data.Pod.UID))
}

// renderEnrootJob writes job.sh for a pod with one enroot container into a new
// directory and returns it.
func renderEnrootJob(t *testing.T, config SlurmConfig, data commonIL.RetrievedPodData, container v1.Container) string {
	t.Helper()
	workDir := t.TempDir()
	pod := data.Pod
	commands := []ContainerCommand{{
		runtimeCommand:   enrootRuntimeCommand(config, data, container, workDir),
		containerName:    container.Name,
		containerCommand: []string{"true"},
		containerImage:   container.Image,
	}}
	if _, err := produceSLURMScript(context.Background(), config, pod, workDir, pod.ObjectMeta, commands, ResourceLimits{CPU: 1, Memory: 1024 * 1024}, false, false, nil); err != nil {
		t.Fatalf("produceSLURMScript() error: %v", err)
	}
	return workDir
}

// runFakeEnroot runs script with bash and the host environment hostEnv, and returns
// the lines the environ() of the --conf given to enroot start printed.
func runFakeEnroot(t *testing.T, dir, script string, hostEnv ...string) []string {
	t.Helper()
	envOut := filepath.Join(t.TempDir(), "environ")
	cmd := exec.Command("bash", "-c", script)
	cmd.Dir = dir
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "FAKE_ENROOT_ENV=" + envOut}, hostEnv...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s failed: %v\n%s", script, err, out)
	}
	got, err := os.ReadFile(envOut)
	if err != nil {
		t.Fatalf("enroot start did not run: %v", err)
	}
	return strings.Split(strings.TrimSuffix(string(got), "\n"), "\n")
}

// The enroot env file is a configuration script whose environ() gives every value
// unchanged once enroot has formatted it; a variable enroot cannot carry is left out
// rather than split.
func TestCreateEnvFileEnroot(t *testing.T) {
	requireBash(t)
	workDir := t.TempDir()
	container := envTestContainer()

	envs, _, err := createEnvFile(context.Background(), SlurmConfig{ContainerRuntime: "enroot"}, envTestPodData(container), container, workDir)
	if err != nil {
		t.Fatalf("createEnvFile() error: %v", err)
	}
	envfilePath := workDir + "/main_envfile.properties"
	if want := []string{"--conf", envfilePath}; !slices.Equal(envs, want) {
		t.Errorf("envs = %q, want %q", envs, want)
	}

	out := filepath.Join(workDir, "environ")
	cmd := exec.Command("bash", "-c", enrootEnvfmt+`set -euo pipefail; source "$1"; environ > "$2"; enroot_envfmt "$2"; enroot_envfmt "$2"`, "bash", envfilePath, out)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	cmd.Dir = workDir
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("environ() of the conf script failed: %v\n%s", err, b)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	want := "PLAIN=hello\nSPECIAL=" + envTestSpecial + "\nEMPTY=\nMASTER_PORT=1234\n" +
		"DQ=\"quoted\"\nSQ='single'\nMIXQ='a' and \"b\"\nTRAILSP=\"x\"   \nJSONSTR=\"{\\\"k\\\": \\\"v\\\"}\"\n" +
		"BSLASH=\\a\\\nDBSLASH=\\\\\n" +
		"NVIDIA_VISIBLE_DEVICES=all\n"
	if string(got) != want {
		t.Errorf("environment\n%q\nwant\n%q", got, want)
	}
	if _, err := os.Stat(filepath.Join(workDir, "pwned")); err == nil {
		t.Error("a value was executed by the shell")
	}
}

// The environ() of the --conf that job.sh hands enroot prints the host's rank variables,
// the gang's mpi-env and then the pod's env, and nothing else of the host. Which of them the container keeps is up to enroot and
// the site's hooks. A host value that starts and ends with a quote gets two pairs of
// quotes, so it is unchanged after enroot's two envfmt passes, and one with a newline
// is left out.
func TestEnrootJobScriptEnviron(t *testing.T) {
	requireBash(t)
	config := enrootTestConfig(t)
	container := v1.Container{
		Name:  testContainer,
		Image: busyboxImage,
		Env:   []v1.EnvVar{{Name: "PLAIN", Value: "hello"}, {Name: "MASTER_PORT", Value: "1234"}},
	}
	workDir := renderEnrootJob(t, config, envTestPodData(container), container)

	got := runFakeEnroot(t, workDir, "exec bash job.sh",
		"RANK=1", "WORLD_SIZE=2", "LOCAL_RANK=", "MASTER_ADDR=10.0.0.1\nINJECTED=1", "MASTER_PORT=6379", "GANG_NODES=n1 n2",
		"GANG_HEAD_NODE=n1\nINJECTED=1", `RAY_ADDRESS='n1:6379'`,
		"CUDA_VISIBLE_DEVICES=1", "SLURM_PROCID=0", "PMIX_RANK=1", "PMI_RANK=1", "OMPI_COMM_WORLD_RANK=1",
		"GANG_LOG_DIR=/l", "HOST_ONLY=1", enrootGangEnvPrefix+"NCCL_DEBUG=INFO", enrootGangEnvPrefix+`NCCL_X="v"`,
		enrootGangEnvPrefix+"NCCL_NL=x\nINJECTED=1", enrootGangEnvPrefix+`BS=\a\`)

	want := []string{
		"RANK='1'", "WORLD_SIZE='2'", "MASTER_PORT='6379'", `RAY_ADDRESS='''n1:6379'''`,
		`BS=''\a\''`, "NCCL_DEBUG='INFO'", `NCCL_X=''"v"''`, "PLAIN='hello'", "MASTER_PORT='1234'",
	}
	if !slices.Equal(got, want) {
		t.Errorf("environ()\n%q\nwant\n%q", got, want)
	}

	environ := filepath.Join(t.TempDir(), "environ")
	if err := os.WriteFile(environ, []byte(strings.Join(got, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-c", enrootEnvfmt+`set -euo pipefail; enroot_envfmt "$1"; enroot_envfmt "$1"`, "bash", environ)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("enroot_envfmt failed: %v\n%s", err, b)
	}
	formatted, err := os.ReadFile(environ)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"RANK=1", "RAY_ADDRESS='n1:6379'", `NCCL_X="v"`, `BS=\a\`, "PLAIN=hello"} {
		if !slices.Contains(strings.Split(string(formatted), "\n"), line) {
			t.Errorf("after two envfmt passes, no line %q in\n%s", line, formatted)
		}
	}
}

// The gang's slurm-job.vk.io/mpi-env is exported under enrootGangEnvPrefix, and the
// environ() of a rank's enroot container, which has no env of its own, prints it.
func TestEnrootGangMPIEnvInEnviron(t *testing.T) {
	requireBash(t)
	config := enrootTestConfig(t)
	dataRoot := t.TempDir() + string(os.PathSeparator)
	extra := map[string]string{testMPIEnvAnnotation: "NCCL_DEBUG=INFO;FI_PROVIDER=cxi"}
	entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
		"h": mkGangMemberWithRC(t, dataRoot, "h", GangRoleHead, 2, extra, []string{testPython}, []string{testScript}),
		"w": mkGangMemberWithRC(t, dataRoot, "w", GangRoleWorker, 2, extra, []string{testPython}, []string{testScript}),
	}}
	path, err := produceGangSLURMScript(context.Background(), config, entry, assignRanks(entry.Members))
	if err != nil {
		t.Fatalf("produceGangSLURMScript: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var exports []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "export "+enrootGangEnvPrefix) {
			exports = append(exports, line)
		}
	}
	if len(exports) != 2 || strings.Contains(string(b), "SINGULARITYENV_") {
		t.Fatalf("mpi-env exports for enroot = %q\n---\n%s", exports, b)
	}

	container := v1.Container{Name: testContainer, Image: busyboxImage}
	workDir := renderEnrootJob(t, config, envTestPodData(container), container)
	got := runFakeEnroot(t, workDir, strings.Join(exports, "\n")+"\nexec bash job.sh", "RANK=1")
	if want := []string{"RANK='1'", "FI_PROVIDER='cxi'", "NCCL_DEBUG='INFO'"}; !slices.Equal(got, want) {
		t.Errorf("environ() = %q, want %q", got, want)
	}
}

// enroot runs only the last --conf and the plugin's carries the pod's env, so a
// configuration script from the site or the pod is refused.
func TestCheckEnrootOptions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		runtime  string
		defaults []string
		options  string
		want     string // empty: accepted
	}{
		{"none", "enroot", []string{"--rw"}, "--mount /a:/a", ""},
		{singularityRuntime, singularityRuntime, []string{"--conf /site.sh"}, "-c /pod.sh", ""},
		{"long", "enroot", nil, "--conf /a.sh", `"--conf" in annotation slurm-job.vk.io/enroot-options`},
		{"short", "enroot", nil, "--rw -c /a.sh", `"-c" in annotation slurm-job.vk.io/enroot-options`},
		{"equals", "enroot", nil, "--conf=/a.sh", `"--conf=/a.sh" in annotation slurm-job.vk.io/enroot-options`},
		{"defaults", "enroot", []string{"--rw", "--conf /site.sh"}, "", `"--conf" in EnrootDefaultOptions`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := SlurmConfig{ContainerRuntime: tc.runtime, EnrootDefaultOptions: tc.defaults}
			meta := metav1.ObjectMeta{Annotations: map[string]string{"slurm-job.vk.io/enroot-options": tc.options}}
			err := checkEnrootOptions(config, meta)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("checkEnrootOptions() = %v, want nil", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("checkEnrootOptions() = %v, want an error naming %s", err, tc.want)
			}
		})
	}
}

// SubmitHandler refuses a pod whose enroot options carry a configuration script
// before it writes or submits anything.
func TestSubmitHandlerRefusesEnrootConf(t *testing.T) {
	submitted := filepath.Join(t.TempDir(), "submitted")
	sbatch := filepath.Join(t.TempDir(), "sbatch")
	if err := os.WriteFile(sbatch, []byte("#!/bin/sh\ntouch "+submitted+"\necho 'Submitted batch job 42'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	dataRoot := t.TempDir() + "/"
	container := v1.Container{Name: testContainer, Image: busyboxImage}
	data := envTestPodData(container)
	data.Pod.Annotations = map[string]string{"slurm-job.vk.io/enroot-options": "--rw --conf=/pod.sh"}
	body, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}

	jids := map[string]*JidStruct{}
	h := &SidecarHandler{
		Config: SlurmConfig{Sbatchpath: sbatch, ContainerRuntime: "enroot", EnrootPath: "enroot", DataRootFolder: dataRoot},
		JIDs:   &jids,
		Ctx:    context.Background(),
	}
	rec := httptest.NewRecorder()
	h.SubmitHandler(rec, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body)))

	if rec.Code == http.StatusOK || !strings.Contains(rec.Body.String(), `"--conf=/pod.sh"`) {
		t.Errorf("SubmitHandler() code = %d, body %q, want a refusal naming --conf=/pod.sh", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(submitted); err == nil {
		t.Error("sbatch ran")
	}
	if _, err := os.Stat(dataRoot + data.Pod.Namespace + "-" + string(data.Pod.UID)); err == nil {
		t.Error("the pod's directory was created")
	}
}

// Singularity keeps its shell-quoted env file and gets no enroot arguments.
func TestPrepareEnvsSingularityUnchanged(t *testing.T) {
	workDir := t.TempDir()
	container := v1.Container{
		Name: testContainer,
		Env: []v1.EnvVar{
			{Name: "SPECIAL", Value: `it's $HOME`},
			{Name: "MULTI", Value: "x\ny"},
			{Name: "RANK", Value: "0"},
		},
	}

	envs := prepareEnvs(context.Background(), SlurmConfig{ContainerRuntime: singularityRuntime}, envTestPodData(container), container, workDir)
	envfilePath := workDir + "/main_envfile.properties"
	if want := []string{"--env-file", envfilePath}; !slices.Equal(envs, want) {
		t.Errorf("envs = %q, want %q", envs, want)
	}
	got, err := os.ReadFile(envfilePath)
	if err != nil {
		t.Fatal(err)
	}
	want := "SPECIAL='it'\"'\"'s $HOME'\nMULTI='x\ny'\nRANK=0\n"
	if string(got) != want {
		t.Errorf("env file\n%q\nwant\n%q", got, want)
	}
	if strings.Contains(strings.Join(prepareRuntimeCommand(SlurmConfig{ContainerRuntime: singularityRuntime}, container, metav1.ObjectMeta{}), " "), "--env") {
		t.Error("singularity command carries --env")
	}
	if exports := gangMPIEnvExports(metav1.ObjectMeta{Annotations: map[string]string{testMPIEnvAnnotation: "NCCL_DEBUG=INFO"}}, singularityRuntime); !slices.Equal(exports, []string{"export SINGULARITYENV_NCCL_DEBUG=INFO"}) {
		t.Errorf("singularity mpi-env exports = %q", exports)
	}
}
