package slurm

import (
	"os"
	"testing"
)

func TestValidateSites(t *testing.T) {
	shimEnv := []string{"REMOTE_USER", "REMOTE_LOGIN", "REMOTE_PORT", "SSH_KEY"}
	for _, k := range shimEnv {
		t.Setenv(k, "")
	}
	base := func() SlurmConfig {
		return SlurmConfig{Sites: map[string]SiteConfig{"hpc-a": {
			LoginHost: "login.hpc-a.example.org", User: "alice", KeyFile: "/etc/interlink/keys/hpc-a/ssh-privatekey",
		}}}
	}

	c := base()
	if err := validateSites(&c); err != nil {
		t.Fatal(err)
	}
	if c.Sites["hpc-a"].Port != 22 {
		t.Errorf("default port: %d", c.Sites["hpc-a"].Port)
	}
	for k, want := range map[string]string{"REMOTE_USER": "alice", "REMOTE_LOGIN": "login.hpc-a.example.org", "REMOTE_PORT": "22", "SSH_KEY": "/etc/interlink/keys/hpc-a/ssh-privatekey"} {
		if got := os.Getenv(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}

	bad := map[string]func(*SlurmConfig){
		"two sites": func(c *SlurmConfig) { c.Sites["hpc-b"] = c.Sites["hpc-a"] },
		"no user": func(c *SlurmConfig) {
			s := c.Sites["hpc-a"]
			s.User = ""
			c.Sites["hpc-a"] = s
		},
		"no login host": func(c *SlurmConfig) {
			s := c.Sites["hpc-a"]
			s.LoginHost = ""
			c.Sites["hpc-a"] = s
		},
		"port out of range": func(c *SlurmConfig) {
			s := c.Sites["hpc-a"]
			s.Port = 70000
			c.Sites["hpc-a"] = s
		},
	}
	for name, mutate := range bad {
		c := base()
		mutate(&c)
		if err := validateSites(&c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	// Without Sites the environment the plugin was started with stays.
	t.Setenv("REMOTE_LOGIN", "login.from-env.example.org")
	none := SlurmConfig{}
	if err := validateSites(&none); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("REMOTE_LOGIN"); got != "login.from-env.example.org" {
		t.Errorf("REMOTE_LOGIN overwritten without Sites: %q", got)
	}
}
