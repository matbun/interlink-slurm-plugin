package shadow

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	// annExtraPorts lists ports to forward beyond the containerPorts: "8080,9090:metrics:UDP".
	annExtraPorts = "interlink.eu/wstunnel-extra-ports"
	// annSocketPorts lists exposed TCP ports the workload serves on unix sockets.
	annSocketPorts = "interlink.eu/shadow-socket-ports"
)

const protocolTCP = "TCP"

// portMapping is one port the shadow forwards, as the template sees it.
type portMapping struct {
	Port     int32
	Name     string
	Protocol string
	// Socket marks a port the workload serves on <socket dir>/fwd-<port>.sock
	// instead of 127.0.0.1:<port>.
	Socket bool
}

// exposedPorts returns the ports of the template's containers and init containers
// and of the extra-ports annotation, the first occurrence of a port and protocol
// winning, as the virtual kubelet's extractPortMappings. They are sorted, so the
// rendered shadow does not change from one call to the next.
func exposedPorts(tmpl corev1.PodTemplateSpec) []portMapping {
	seen := map[string]bool{}
	var ports []portMapping
	add := func(m portMapping) {
		key := fmt.Sprintf("%d-%s", m.Port, m.Protocol)
		if seen[key] {
			return
		}
		seen[key] = true
		ports = append(ports, m)
	}
	for _, c := range append(slices.Clone(tmpl.Spec.Containers), tmpl.Spec.InitContainers...) {
		for _, p := range c.Ports {
			protocol := string(p.Protocol)
			if protocol == "" {
				protocol = protocolTCP
			}
			add(portMapping{Port: p.ContainerPort, Name: p.Name, Protocol: protocol})
		}
	}
	for _, m := range parseExtraPorts(tmpl.Annotations[annExtraPorts]) {
		add(m)
	}
	slices.SortFunc(ports, func(a, b portMapping) int {
		return cmp.Or(cmp.Compare(a.Port, b.Port), strings.Compare(a.Protocol, b.Protocol))
	})
	return ports
}

// parseExtraPorts reads "port", "port:name" or "port:name:protocol" entries.
func parseExtraPorts(value string) []portMapping {
	var ports []portMapping
	for _, field := range strings.Split(value, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		parts := strings.Split(field, ":")
		port, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			continue
		}
		m := portMapping{Port: int32(port), Protocol: protocolTCP}
		if len(parts) > 1 {
			m.Name = parts[1]
		}
		if len(parts) > 2 {
			m.Protocol = strings.ToUpper(parts[2])
		}
		ports = append(ports, m)
	}
	return ports
}

func hasTCPPort(ports []portMapping) bool {
	return slices.ContainsFunc(ports, func(m portMapping) bool { return m.Protocol != "UDP" })
}

func firstTCPPort(ports []portMapping) int32 {
	for _, m := range ports {
		if m.Protocol != "UDP" {
			return m.Port
		}
	}
	return 0
}

func hasSocketPorts(ports []portMapping) bool {
	return slices.ContainsFunc(ports, func(m portMapping) bool { return m.Socket })
}

// markSocketPorts flags the exposed TCP ports listed in interlink.eu/shadow-socket-ports.
// An annotation that cannot be honoured is an error, as the virtual kubelet refuses
// the pod: forwarding such a port to 127.0.0.1 instead would reach whoever listens
// there, which is what the socket was meant to rule out.
func markSocketPorts(value, forwardTarget string, ports []portMapping) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if forwardTarget != ForwardTargetNode {
		return fmt.Errorf("%s needs Shadow.ForwardTarget %q", annSocketPorts, ForwardTargetNode)
	}
	for _, field := range strings.Split(value, ",") {
		field = strings.TrimSpace(field)
		n, err := strconv.ParseUint(field, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid %s: %q is not a port number", annSocketPorts, field)
		}
		i := slices.IndexFunc(ports, func(m portMapping) bool { return m.Protocol == protocolTCP && uint64(m.Port) == n })
		if i < 0 {
			return fmt.Errorf("%s lists port %d, which is not an exposed TCP port", annSocketPorts, n)
		}
		ports[i].Socket = true
	}
	return nil
}
