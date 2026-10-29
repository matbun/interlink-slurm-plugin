package shadow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// The naming and validation below are the virtual kubelet's (pkg/virtualkubelet/shadow.go),
// so a shadow has the name it had when the virtual kubelet rendered it in the pod's
// own namespace.

// resourcePrefix is prepended to shadow resource names, so they cannot collide with
// the workload's own objects.
const resourcePrefix = "shadow-"

// nodeConfigMapSuffix names the per-shadow ConfigMap carrying the compute node.
const nodeConfigMapSuffix = "-node"

// Keys of the node ConfigMap.
const (
	nodeNameKey  = "compute-node"
	socketDirKey = "socket-dir"
)

// maxComputeNodeNameLen is the longest DNS name.
const maxComputeNodeNameLen = 253

// computeNodeNamePattern matches a hostname or an IP literal and nothing else. The
// shadow interpolates the node into shell commands, locally and on the login node,
// so anything outside this set is refused rather than escaped.
var computeNodeNamePattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._:-]*[A-Za-z0-9])?$`)

func isValidComputeNodeName(nodeName string) bool {
	return len(nodeName) <= maxComputeNodeNameLen && computeNodeNamePattern.MatchString(nodeName)
}

// maxSocketDirLen keeps the longest socket path the shadow builds,
// <dir>/fwd-65535.sock, within the 107 bytes a unix socket path can hold.
const maxSocketDirLen = 92

// socketDirPattern matches an absolute path of plain segments, none starting with
// a dot, which also keeps out "." and "..".
var socketDirPattern = regexp.MustCompile(`^(/[A-Za-z0-9_][A-Za-z0-9._-]*)+$`)

func isValidSocketDir(dir string) bool {
	return len(dir) <= maxSocketDirLen && socketDirPattern.MatchString(dir)
}

func sanitizeDNSName(name string) string {
	name = strings.ToLower(name)

	var builder strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
		} else {
			builder.WriteRune('-')
		}
	}
	name = strings.Trim(builder.String(), "-")

	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}

	if len(name) > 63 {
		name = strings.TrimRight(name[:63], "-")
	}
	if name == "" {
		name = "default"
	}
	return name
}

// uniqueTruncate shortens a name and adds an 8-character hash to prevent naming collisions.
func uniqueTruncate(s string, maxLen int, full string) string {
	if len(s) <= maxLen {
		return s
	}
	h := sha256.Sum256([]byte(full))
	suffix := hex.EncodeToString(h[:4])
	keep := maxLen - len(suffix) - 1
	return strings.TrimRight(s[:keep], "-") + "-" + suffix
}

// resourceName is the base name of the shadow of workloadName in namespace, as the
// virtual kubelet's computeShadowResourceNamesForSameNamespace derives it from a
// pod name.
func resourceName(workloadName, namespace string) string {
	sanitizedNamespace := sanitizeDNSName(namespace)
	sanitizedName := sanitizeDNSName(workloadName)

	name := resourcePrefix + sanitizedName + "-" + sanitizedNamespace
	// Hash on the unsanitized names: sanitizeDNSName truncates, long names could still collide.
	full := resourcePrefix + workloadName + "-" + namespace

	if len(name) > 63 {
		const maxNameLen, maxNsLen = 28, 28
		if len(sanitizedName) > maxNameLen {
			sanitizedName = uniqueTruncate(sanitizedName, maxNameLen, full)
		}
		if len(sanitizedNamespace) > maxNsLen {
			sanitizedNamespace = sanitizedNamespace[:maxNsLen]
		}
		name = strings.TrimRight(resourcePrefix+sanitizedName+"-"+sanitizedNamespace, "-")
	}

	if len(fmt.Sprintf("%s-%s", name, namespace)) > 63 {
		maxNameLen := 63 - len(namespace) - 1
		if maxNameLen > 9 && len(name) > maxNameLen { // >9: room for the 8-char hash suffix
			name = uniqueTruncate(name, maxNameLen, full)
		}
	}
	return name
}

func nodeConfigMapName(name string) string {
	return name + nodeConfigMapSuffix
}
