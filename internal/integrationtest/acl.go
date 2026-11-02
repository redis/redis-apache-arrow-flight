package integrationtest

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	goredis "github.com/redis/go-redis/v9"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// ACLUser manages a disposable Software user through its supported management API.
// Administrative credentials are separate from the data-plane test identity.
type ACLUser struct {
	t                        testing.TB
	base, admin, secret      string
	client                   *http.Client
	name, password           string
	rules                    []string
	uid, role, acl, database int
	permissions              []map[string]int
}

func NewACLUser(t testing.TB, name string, rules ...string) *ACLUser {
	t.Helper()
	u := &ACLUser{t: t, name: name, base: os.Getenv("REDIS_ENTERPRISE_API"), admin: os.Getenv("REDIS_ENTERPRISE_ADMIN_USER"), secret: os.Getenv("REDIS_ENTERPRISE_ADMIN_PASSWORD")}
	if !strings.HasPrefix(u.base, "https://") || u.admin == "" || u.secret == "" {
		t.Fatal("Enterprise ACL tests require HTTPS management API and separate admin credentials")
	}
	roots := x509.NewCertPool()
	pem, err := os.ReadFile(os.Getenv("REDIS_ENTERPRISE_CA_FILE"))
	if err != nil || !roots.AppendCertsFromPEM(pem) {
		t.Fatal("Enterprise management API requires a valid test CA")
	}
	u.client = &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	var databases []struct {
		UID         int              `json:"uid"`
		Name        string           `json:"name"`
		Permissions []map[string]int `json:"roles_permissions"`
	}
	u.call("GET", "/bdbs", nil, &databases)
	for _, b := range databases {
		if b.Name == os.Getenv("REDIS_ENTERPRISE_DATABASE") {
			u.database = b.UID
			u.permissions = b.Permissions
		}
	}
	if u.database == 0 {
		t.Fatal("Enterprise ACL test database was not found")
	}
	t.Cleanup(u.Close)
	var acl struct {
		UID int `json:"uid"`
	}
	u.call("POST", "/redis_acls", map[string]any{"name": name, "acl": "+@all ~*"}, &acl)
	u.acl = acl.UID
	var role struct {
		UID int `json:"uid"`
	}
	u.call("POST", "/roles", map[string]any{"name": name, "management": "none"}, &role)
	u.role = role.UID
	u.permissions = append(u.permissions, map[string]int{"role_uid": u.role, "redis_acl_uid": u.acl})
	u.call("PUT", fmt.Sprintf("/bdbs/%d", u.database), map[string]any{"roles_permissions": u.permissions}, nil)
	u.apply(rules)
	var user struct {
		UID int `json:"uid"`
	}
	u.call("POST", "/users", map[string]any{"name": name, "email": name + "@flight-test.local", "password": u.password, "role_uids": []int{u.role}}, &user)
	u.uid = user.UID
	u.wait(false)
	return u
}

func (u *ACLUser) call(method, path string, body any, out any) {
	u.t.Helper()
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	req, err := http.NewRequest(method, u.base+path, bytes.NewReader(data))
	if err != nil {
		u.t.Fatal("invalid Enterprise management request")
	}
	req.SetBasicAuth(u.admin, u.secret)
	req.Header.Set("Content-Type", "application/json")
	var res *http.Response
	deadline := time.Now().Add(20 * time.Second)
	for {
		req.Body = io.NopCloser(bytes.NewReader(data))
		res, err = u.client.Do(req)
		if err != nil {
			u.t.Fatal("Enterprise management API transport failed")
		}
		if res.StatusCode != http.StatusConflict || time.Now().After(deadline) {
			break
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		io.Copy(io.Discard, res.Body)
		u.t.Fatalf("Enterprise management %s %s returned HTTP %d", method, path, res.StatusCode)
	}

	if out != nil {
		if err = json.NewDecoder(res.Body).Decode(out); err != nil {
			u.t.Fatal("invalid Enterprise management response")
		}
	}
}
func (u *ACLUser) apply(rules []string) {
	for _, r := range rules {
		switch {
		case strings.HasPrefix(r, ">"):
			u.password = strings.TrimPrefix(r, ">")
		case r == "on" || r == "off" || r == "resetpass":
		case r == "resetchannels": // Software's ACL API configures channels via the ACL string.
		default:
			u.rules = append(u.rules, r)
		}
	}
	u.call("PUT", fmt.Sprintf("/redis_acls/%d", u.acl), map[string]any{"acl": strings.Join(u.rules, " ")}, nil)
}
func (u *ACLUser) Set(rules ...string) {
	u.t.Helper()
	u.apply(rules)
	for _, r := range rules {
		if r == "off" {
			u.call("DELETE", fmt.Sprintf("/users/%d", u.uid), nil, nil)
			u.uid = 0
			u.wait(true)
			return
		}
	}
	u.call("PUT", fmt.Sprintf("/users/%d", u.uid), map[string]any{"password": u.password}, nil)
	// Management changes are asynchronous at database proxies. Wait until the new
	// password/permissions are observable via a fresh connection before assertions.
	u.wait(false)
}
func (u *ACLUser) Close() {
	defer u.client.CloseIdleConnections()
	if u.role == 0 {
		if u.acl != 0 {
			u.call("DELETE", fmt.Sprintf("/redis_acls/%d", u.acl), nil, nil)
		}
		return
	}
	if u.uid != 0 {
		u.call("DELETE", fmt.Sprintf("/users/%d", u.uid), nil, nil)
		u.uid = 0
	}
	// Retrieve current grants so one fixture cannot remove another user's role.
	var b struct {
		Permissions []map[string]int `json:"roles_permissions"`
	}
	u.call("GET", fmt.Sprintf("/bdbs/%d", u.database), nil, &b)
	kept := []map[string]int{}
	for _, p := range b.Permissions {
		if p["role_uid"] != u.role {
			kept = append(kept, p)
		}
	}
	u.call("PUT", fmt.Sprintf("/bdbs/%d", u.database), map[string]any{"roles_permissions": kept}, nil)
	u.call("DELETE", fmt.Sprintf("/roles/%d", u.role), nil, nil)
	if u.acl != 0 {
		u.call("DELETE", fmt.Sprintf("/redis_acls/%d", u.acl), nil, nil)
	}
}

func (u *ACLUser) wait(deleted bool) {
	u.t.Helper()
	opts, err := goredis.ParseURL(os.Getenv("REDIS_URI"))
	if err != nil {
		u.t.Fatal("Invalid Redis TLS fixture URI")
	}
	opts.Username, opts.Password = u.name, u.password
	opts.MaxRetries = -1
	opts.DisableIdentity = true
	opts.TLSConfig = u.client.Transport.(*http.Transport).TLSClientConfig.Clone()
	// ServerName follows the data endpoint, rather than the management URL.
	wantDenied := false
	probeGet := false
	for _, r := range u.rules {
		if r == "-get" {
			probeGet = true
			wantDenied = true
		}
		if r == "+get" {
			probeGet = false
			wantDenied = false
		}
		if r == "-ping" {
			wantDenied = true
		}
		if r == "+ping" {
			wantDenied = false
		}
	}
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		c := goredis.NewClient(opts)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = c.Ping(ctx).Err()
		if err == nil && probeGet {
			err = c.Get(ctx, "adbc:{meta}:__flight_acl_probe__").Err()
		}
		cancel()
		c.Close()
		if deleted && err != nil && strings.Contains(err.Error(), "WRONGPASS") {
			return
		}
		if !deleted && ((!wantDenied && err == nil) || (wantDenied && err != nil && (strings.Contains(err.Error(), "NOPERM") || strings.Contains(err.Error(), "is not allowed")))) {
			return
		}
		if time.Now().After(deadline) {
			u.t.Fatalf("Enterprise ACL update did not become observable at the data endpoint: denied=%v deleted=%v error=%v", wantDenied, deleted, err)
		}
	}
}
