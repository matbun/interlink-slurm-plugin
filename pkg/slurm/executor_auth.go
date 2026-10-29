package slurm

import (
	"crypto/sha256"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/log"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// tokenCacheTTL is how long a token that passed its review is trusted again
// without one.
const tokenCacheTTL = time.Minute

// tokenCache remembers the tokens that passed, by digest.
type tokenCache struct {
	mu      sync.Mutex
	expires map[[sha256.Size]byte]time.Time
}

func (c *tokenCache) valid(key [sha256.Size]byte, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	exp, ok := c.expires[key]
	if ok && now.After(exp) {
		delete(c.expires, key)
		return false
	}
	return ok
}

func (c *tokenCache) store(key [sha256.Size]byte, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.expires == nil {
		c.expires = map[[sha256.Size]byte]time.Time{}
	}
	for k, exp := range c.expires {
		if now.After(exp) {
			delete(c.expires, k)
		}
	}
	c.expires[key] = now.Add(tokenCacheTTL)
}

// authorize lets the request through, or answers 401, 403 or 503 and returns
// false. Mode network accepts every caller.
func (d *Executor) authorize(w http.ResponseWriter, r *http.Request) bool {
	auth := d.config().Executor.Auth
	if auth.Mode == ExecutorAuthNetwork {
		return true
	}
	deny := func(code int, msg string) bool {
		writeJSON(w, code, ExecutorError{APIVersion: ExecutorAPIVersion, Message: msg})
		return false
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	token = strings.TrimSpace(token)
	if !ok || token == "" {
		return deny(http.StatusUnauthorized, "a bearer token is required")
	}
	key := sha256.Sum256([]byte(token))
	now := time.Now()
	if d.tokens.valid(key, now) {
		return true
	}
	if d.Kube == nil {
		return deny(http.StatusServiceUnavailable, "the executor has no Kubernetes client to review tokens")
	}
	audience := auth.Audience
	if audience == "" {
		audience = defaultExecutorAudience
	}
	review, err := d.Kube.AuthenticationV1().TokenReviews().Create(r.Context(), &authv1.TokenReview{
		Spec: authv1.TokenReviewSpec{Token: token, Audiences: []string{audience}},
	}, metav1.CreateOptions{})
	if err != nil {
		log.G(d.Sidecar.Ctx).Warning("executor: token review failed: ", err)
		return deny(http.StatusServiceUnavailable, "token review failed: "+err.Error())
	}
	if !review.Status.Authenticated {
		msg := "token not authenticated"
		if review.Status.Error != "" {
			msg += ": " + review.Status.Error
		}
		return deny(http.StatusUnauthorized, msg)
	}
	if len(review.Status.Audiences) > 0 && !slices.Contains(review.Status.Audiences, audience) {
		return deny(http.StatusUnauthorized, "token not issued for audience "+audience)
	}
	user := review.Status.User.Username
	if !slices.Contains(auth.AllowedServiceAccounts, user) {
		log.G(d.Sidecar.Ctx).Warning("executor: refused a call from ", user)
		return deny(http.StatusForbidden, "user "+user+" may not call this executor")
	}
	d.tokens.store(key, now)
	return true
}
