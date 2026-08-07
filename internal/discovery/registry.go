package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/semver/v3"
)

// RegistrySource queries OCI registries for image tags.
type RegistrySource struct {
	URL            string
	Repositories   []string
	TagFilter      string
	TagSeek        string
	TopX           int32
	MaxScan        int32
	ImageTemplate  string
	VersionPattern string
	HTTPClient     *http.Client
}

// NewRegistrySource creates a new registry discovery source.
func NewRegistrySource(url string, repos []string, tagFilter, tagSeek string, topX, maxScan int32, imageTemplate, versionPattern string, httpClient *http.Client) *RegistrySource {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &RegistrySource{
		URL:            strings.TrimSuffix(url, "/"),
		Repositories:   repos,
		TagFilter:      tagFilter,
		TagSeek:        tagSeek,
		TopX:           topX,
		MaxScan:        maxScan,
		ImageTemplate:  imageTemplate,
		VersionPattern: versionPattern,
		HTTPClient:     httpClient,
	}
}

// tagListResponse represents the OCI Distribution API tag list response.
type tagListResponse struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// tagListPageSize is the number of tags requested per page. Registries cap the
// effective page size (GitLab caps at 100), so this is an upper bound.
const tagListPageSize = 1000

// defaultMaxScan bounds how many tags are fetched per repository when MaxScan is
// unset. Registries can hold tens of thousands of tags; pair tagSeek with a
// budget to fetch only the relevant range.
const defaultMaxScan = 1000

// Fetch queries the registry for tags and returns discovered images.
func (rs *RegistrySource) Fetch(ctx context.Context) ([]ImageResult, error) {
	var allResults []ImageResult

	for _, repo := range rs.Repositories {
		results, err := rs.fetchRepo(ctx, repo)
		if err != nil {
			return nil, fmt.Errorf("fetching tags for %s: %w", repo, err)
		}
		allResults = append(allResults, results...)
	}

	// Sort by score descending (higher index = more recent)
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Score > allResults[j].Score
	})

	return allResults, nil
}

// listTags returns up to MaxScan tags for a repository, following the OCI
// Distribution `Link` header (rel="next") to paginate. Registries do not
// guarantee tag ordering and many (e.g. GitLab) return only a page at a time.
// TagSeek is passed as the `last` cursor so callers can skip irrelevant earlier
// tags without fetching them.
func (rs *RegistrySource) listTags(ctx context.Context, repo string) ([]string, error) {
	budget := int(rs.MaxScan)
	if budget <= 0 {
		budget = defaultMaxScan
	}

	q := url.Values{}
	q.Set("n", strconv.Itoa(tagListPageSize))
	if rs.TagSeek != "" {
		q.Set("last", rs.TagSeek)
	}
	next := fmt.Sprintf("%s/v2/%s/tags/list?%s", rs.URL, repo, q.Encode())

	var tags []string
	var token string
	for next != "" && len(tags) < budget {
		resp, err := rs.authorizedGet(ctx, next, &token)
		if err != nil {
			return nil, fmt.Errorf("listing tags: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return nil, fmt.Errorf("registry returned status %d: %s", resp.StatusCode, string(body))
		}

		var tagList tagListResponse
		if err := json.NewDecoder(resp.Body).Decode(&tagList); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("decoding response: %w", err)
		}
		linkHeader := resp.Header.Get("Link")
		_ = resp.Body.Close()

		tags = append(tags, tagList.Tags...)
		next = rs.nextPageURL(linkHeader)
	}

	if len(tags) > budget {
		tags = tags[:budget]
	}
	return tags, nil
}

// tokenResponse models the OCI token endpoint reply. Registries return the
// bearer token under "token" and/or "access_token".
type tokenResponse struct {
	Token       string `json:"token"`
	AccessToken string `json:"access_token"`
}

// authorizedGet performs a GET and, if the registry answers with a 401 Bearer
// challenge, transparently obtains an anonymous token from the challenge realm
// and retries once. The token is cached in *token for reuse across paginated
// requests. Registries like GitLab require a bearer token even for anonymous
// pulls of public repositories.
func (rs *RegistrySource) authorizedGet(ctx context.Context, urlStr string, token *string) (*http.Response, error) {
	resp, err := rs.rawGet(ctx, urlStr, *token)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	challenge := resp.Header.Get("Www-Authenticate")
	_ = resp.Body.Close()
	newToken, err := rs.fetchBearerToken(ctx, challenge)
	if err != nil {
		return nil, err
	}
	if newToken == "" {
		// Not a bearer challenge we can satisfy; re-issue so the caller sees the 401 body.
		return rs.rawGet(ctx, urlStr, *token)
	}
	*token = newToken
	return rs.rawGet(ctx, urlStr, newToken)
}

// rawGet issues a single GET, attaching a bearer token when one is provided.
func (rs *RegistrySource) rawGet(ctx context.Context, urlStr, token string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return rs.HTTPClient.Do(req)
}

// fetchBearerToken parses a Www-Authenticate Bearer challenge and requests an
// anonymous token from its realm. Returns "" when the header is not a Bearer
// challenge or carries no realm.
func (rs *RegistrySource) fetchBearerToken(ctx context.Context, challenge string) (string, error) {
	realm, params := parseBearerChallenge(challenge)
	if realm == "" {
		return "", nil
	}

	u, err := url.Parse(realm)
	if err != nil {
		return "", fmt.Errorf("parsing token realm: %w", err)
	}
	q := u.Query()
	if svc := params["service"]; svc != "" {
		q.Set("service", svc)
	}
	if scope := params["scope"]; scope != "" {
		q.Set("scope", scope)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("creating token request: %w", err)
	}
	resp, err := rs.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("requesting token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("token endpoint returned status %d: %s", resp.StatusCode, string(body))
	}

	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", fmt.Errorf("decoding token response: %w", err)
	}
	if tr.Token != "" {
		return tr.Token, nil
	}
	return tr.AccessToken, nil
}

// parseBearerChallenge parses an RFC 6750 "Bearer" Www-Authenticate header,
// returning the realm and the remaining directives (service, scope, ...). The
// realm is empty when the scheme is not Bearer.
func parseBearerChallenge(header string) (realm string, params map[string]string) {
	params = map[string]string{}
	const prefix = "Bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", params
	}
	for _, part := range strings.Split(header[len(prefix):], ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.Trim(strings.TrimSpace(kv[1]), `"`)
		if key == "realm" {
			realm = val
		} else {
			params[key] = val
		}
	}
	return realm, params
}

// nextPageURL parses an RFC 5988 `Link` header and returns the absolute URL of
// the rel="next" page, or "" when there is no next page. The registry returns a
// relative URI which is resolved against the registry base URL.
func (rs *RegistrySource) nextPageURL(linkHeader string) string {
	if linkHeader == "" {
		return ""
	}
	for _, part := range strings.Split(linkHeader, ",") {
		segs := strings.Split(part, ";")
		if len(segs) < 2 {
			continue
		}
		isNext := false
		for _, p := range segs[1:] {
			if strings.Contains(strings.ToLower(p), `rel="next"`) || strings.Contains(strings.ToLower(p), "rel=next") {
				isNext = true
				break
			}
		}
		if !isNext {
			continue
		}
		raw := strings.TrimSpace(segs[0])
		raw = strings.TrimPrefix(raw, "<")
		raw = strings.TrimSuffix(raw, ">")
		if raw == "" {
			return ""
		}
		if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
			return raw
		}
		return rs.URL + raw
	}
	return ""
}

func (rs *RegistrySource) fetchRepo(ctx context.Context, repo string) ([]ImageResult, error) {
	tags, err := rs.listTags(ctx, repo)
	if err != nil {
		return nil, err
	}

	// Filter tags
	if rs.TagFilter != "" {
		re, err := regexp.Compile(rs.TagFilter)
		if err != nil {
			return nil, fmt.Errorf("compiling tag filter: %w", err)
		}
		var filtered []string
		for _, tag := range tags {
			if re.MatchString(tag) {
				filtered = append(filtered, tag)
			}
		}
		tags = filtered
	}

	// Sort newest-first. Tags carrying a (possibly prefixed) version are ordered
	// by version desc; tags with no parseable version fall back to push order.
	var versionRe *regexp.Regexp
	if rs.VersionPattern != "" {
		re, err := regexp.Compile(rs.VersionPattern)
		if err != nil {
			return nil, fmt.Errorf("compiling version pattern: %w", err)
		}
		versionRe = re
	}
	tags = sortTagsNewestFirst(tags, versionRe)

	// Limit to topX by keeping the first N tags (newest).
	if rs.TopX > 0 && int32(len(tags)) > rs.TopX {
		tags = tags[:rs.TopX]
	}

	// Build image refs. Higher score = newer (index 0 is newest).
	results := make([]ImageResult, 0, len(tags))
	for i, tag := range tags {
		imageRef, err := rs.buildImageRef(repo, tag)
		if err != nil {
			return nil, fmt.Errorf("building image ref for tag %s: %w", tag, err)
		}
		results = append(results, ImageResult{
			Image: imageRef,
			Score: int64(len(tags) - i),
		})
	}

	return results, nil
}

// reEmbeddedSemver extracts a semver-ish version from anywhere inside a tag,
// e.g. "x86_64-v17.5.0" -> "17.5.0". This handles arch/flavor-prefixed tags
// like GitLab runner helper images (x86_64-v17.5.0, ubuntu-x86_64-v16.11.0).
var reEmbeddedSemver = regexp.MustCompile(`(\d+)\.(\d+)(?:\.(\d+))?(?:[-+][0-9A-Za-z.-]+)?`)

// parseTagVersion tries to interpret a tag as a version. When versionRe is set,
// its first capture group is used as the version substring. Otherwise it
// attempts a strict semver parse, then falls back to extracting an embedded
// semver substring. Returns nil when no version can be found.
func parseTagVersion(tag string, versionRe *regexp.Regexp) *semver.Version {
	if versionRe != nil {
		m := versionRe.FindStringSubmatch(tag)
		if len(m) >= 2 {
			if v, err := semver.NewVersion(m[1]); err == nil {
				return v
			}
		}
		return nil
	}
	if v, err := semver.NewVersion(tag); err == nil {
		return v
	}
	if m := reEmbeddedSemver.FindString(tag); m != "" {
		if v, err := semver.NewVersion(m); err == nil {
			return v
		}
	}
	return nil
}

// sortTagsNewestFirst orders tags newest-first. Tags carrying a (possibly
// prefixed) semver version sort by version descending; tags without a parseable
// version keep their original push order (best effort) and are appended after
// the versioned tags. versionRe, when non-nil, overrides version extraction
// using its first capture group.
func sortTagsNewestFirst(tags []string, versionRe *regexp.Regexp) []string {
	type vt struct {
		tag string
		ver *semver.Version
		idx int
	}
	parsed := make([]vt, len(tags))
	for i, t := range tags {
		parsed[i] = vt{tag: t, ver: parseTagVersion(t, versionRe), idx: i}
	}
	sort.SliceStable(parsed, func(i, j int) bool {
		a, b := parsed[i], parsed[j]
		if a.ver != nil && b.ver != nil {
			if a.ver.Equal(b.ver) {
				return a.tag < b.tag // stable tie-break for prefixed variants
			}
			return a.ver.GreaterThan(b.ver)
		}
		if a.ver != nil {
			return true // versioned before non-versioned
		}
		if b.ver != nil {
			return false
		}
		return a.idx > b.idx // both unversioned: push order, newest last -> reverse
	})
	out := make([]string, len(parsed))
	for i, p := range parsed {
		out[i] = p.tag
	}
	return out
}

// templateData provides variables for the image template.
type templateData struct {
	Registry   string
	Repository string
	Tag        string
}

func (rs *RegistrySource) buildImageRef(repo, tag string) (string, error) {
	if rs.ImageTemplate != "" {
		tmpl, err := template.New("image").Parse(rs.ImageTemplate)
		if err != nil {
			return "", fmt.Errorf("parsing image template: %w", err)
		}

		data := templateData{
			Registry:   rs.URL,
			Repository: repo,
			Tag:        tag,
		}

		var buf strings.Builder
		if err := tmpl.Execute(&buf, data); err != nil {
			return "", fmt.Errorf("executing image template: %w", err)
		}
		return buf.String(), nil
	}

	// Default: registry/repo:tag
	registry := strings.TrimPrefix(rs.URL, "https://")
	registry = strings.TrimPrefix(registry, "http://")
	return fmt.Sprintf("%s/%s:%s", registry, repo, tag), nil
}
