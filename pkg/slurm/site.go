package slurm

import (
	"fmt"
	"os"
	"strconv"
)

// SiteConfig is where the ssh shims connect.
type SiteConfig struct {
	LoginHost string `yaml:"LoginHost"`
	User      string `yaml:"User"`
	Port      int    `yaml:"Port"`
	KeyFile   string `yaml:"KeyFile"`
}

const defaultSSHPort = 22

// validateSites fills the site's default port, refuses a configuration with more
// than one site and exports the site for the shims. It does nothing when Sites is
// empty.
func validateSites(c *SlurmConfig) error {
	if len(c.Sites) == 0 {
		return nil
	}
	if len(c.Sites) > 1 {
		return fmt.Errorf("Sites has %d entries: the shims reach one site, run one plugin per site", len(c.Sites))
	}
	for name, site := range c.Sites {
		if site.LoginHost == "" || site.User == "" {
			return fmt.Errorf("Sites.%s: LoginHost and User are required", name)
		}
		if site.Port == 0 {
			site.Port = defaultSSHPort
		}
		if site.Port < 1 || site.Port > 65535 {
			return fmt.Errorf("Sites.%s: Port %d is not a TCP port", name, site.Port)
		}
		c.Sites[name] = site
		return exportSiteEnv(name, site)
	}
	return nil
}

// exportSiteEnv sets the environment the sbatch, squeue and scancel shims read
// their target from.
func exportSiteEnv(name string, site SiteConfig) error {
	for k, v := range map[string]string{
		"REMOTE_USER":  site.User,
		"REMOTE_LOGIN": site.LoginHost,
		"REMOTE_PORT":  strconv.Itoa(site.Port),
		"SSH_KEY":      site.KeyFile,
	} {
		if err := os.Setenv(k, v); err != nil {
			return fmt.Errorf("site %s: export %s: %w", name, k, err)
		}
	}
	return nil
}
