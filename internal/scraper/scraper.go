package scraper

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

type Config struct {
	Seeds           []string
	OutputDir       string
	MaxPages        int
	MaxDepth        int
	PageDelay       time.Duration
	RequestTimeout  time.Duration
	DownloadWorkers int
	UserAgent       string
	ProxyURL        string
	ExtraHeaders    map[string]string
	MaxRetries      int
}

type Stats struct {
	PagesVisited     int
	AssetsDiscovered int
	AssetsDownloaded int
	AssetsSkipped    int
	DownloadErrors   int
}

type scopeRule struct {
	host   string
	prefix string
}

type pageTask struct {
	u     *url.URL
	depth int
}

type Scraper struct {
	cfg    Config
	logger *slog.Logger
	client *http.Client

	mu          sync.Mutex
	seenPages   map[string]struct{}
	seenAssets  map[string]struct{}
	stats       Stats
	scopeRules  []scopeRule
	seedTargets []pageTask
}

var (
	assetExtensions = map[string]struct{}{
		".jpg": {}, ".jpeg": {}, ".png": {}, ".gif": {}, ".bmp": {}, ".webp": {}, ".svg": {}, ".tif": {}, ".tiff": {}, ".avif": {},
		".mp4": {}, ".mov": {}, ".webm": {}, ".mkv": {}, ".avi": {}, ".m4v": {}, ".mpg": {}, ".mpeg": {},
		".pdf": {}, ".doc": {}, ".docx": {}, ".xls": {}, ".xlsx": {}, ".ppt": {}, ".pptx": {}, ".txt": {}, ".rtf": {},
		".odt": {}, ".ods": {}, ".odp": {}, ".csv": {}, ".tsv": {}, ".xml": {}, ".zip": {},
	}

	pageExtensions = map[string]struct{}{
		".html": {}, ".htm": {}, ".php": {}, ".asp": {}, ".aspx": {}, ".jsp": {}, ".cfm": {}, ".shtml": {}, ".xhtml": {},
	}

	documentContentTypes = []string{
		"application/pdf",
		"application/msword",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.ms-excel",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"application/vnd.ms-powerpoint",
		"application/vnd.openxmlformats-officedocument.presentationml.presentation",
		"text/plain",
		"text/csv",
		"application/rtf",
		"application/xml",
		"text/xml",
		"application/zip",
		"application/octet-stream",
	}

	commonContentTypeExt = map[string]string{
		"application/pdf":    ".pdf",
		"application/msword": ".doc",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
		"application/vnd.ms-excel": ".xls",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":         ".xlsx",
		"application/vnd.ms-powerpoint":                                             ".ppt",
		"application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
		"text/plain":      ".txt",
		"text/csv":        ".csv",
		"application/rtf": ".rtf",
		"application/xml": ".xml",
		"text/xml":        ".xml",
		"application/zip": ".zip",
		"video/mp4":       ".mp4",
		"video/webm":      ".webm",
		"video/quicktime": ".mov",
		"image/jpeg":      ".jpg",
		"image/png":       ".png",
		"image/gif":       ".gif",
		"image/webp":      ".webp",
		"image/svg+xml":   ".svg",
		"image/avif":      ".avif",
	}
)

func New(cfg Config, logger *slog.Logger) (*Scraper, error) {
	if len(cfg.Seeds) == 0 {
		return nil, errors.New("at least one seed URL is required")
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "downloads"
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.DownloadWorkers <= 0 {
		cfg.DownloadWorkers = 6
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if logger == nil {
		logger = slog.Default()
	}

	scopes, seeds, err := parseScope(cfg.Seeds)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
	}
	if cfg.ProxyURL != "" {
		p, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy url: %w", err)
		}
		transport.Proxy = http.ProxyURL(p)
	}

	client := &http.Client{
		Timeout:   cfg.RequestTimeout,
		Transport: transport,
	}

	return &Scraper{
		cfg:         cfg,
		logger:      logger,
		client:      client,
		seenPages:   make(map[string]struct{}),
		seenAssets:  make(map[string]struct{}),
		scopeRules:  scopes,
		seedTargets: seeds,
	}, nil
}

func (s *Scraper) Run(ctx context.Context) (Stats, error) {
	if err := os.MkdirAll(s.cfg.OutputDir, 0o755); err != nil {
		return Stats{}, fmt.Errorf("create output directory: %w", err)
	}

	queue := make([]pageTask, 0, len(s.seedTargets))
	for _, seed := range s.seedTargets {
		if s.reservePage(normalizeURL(seed.u)) {
			queue = append(queue, seed)
		}
	}

	downloadQueue := make(chan *url.URL, 256)
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.DownloadWorkers; i++ {
		workerID := i + 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.downloadWorker(ctx, workerID, downloadQueue)
		}()
	}

crawlLoop:
	for len(queue) > 0 {
		if ctx.Err() != nil {
			break
		}
		if s.cfg.MaxPages > 0 && s.snapshotStats().PagesVisited >= s.cfg.MaxPages {
			break
		}

		task := queue[0]
		queue = queue[1:]
		s.bumpPagesVisited()

		pages, assets, err := s.fetchAndExtract(ctx, task.u)
		if err != nil {
			s.logger.Warn("page fetch failed", "url", task.u.String(), "err", err)
			continue
		}

		s.logger.Info("page scanned", "url", task.u.String(), "depth", task.depth, "assets_found", len(assets), "links_found", len(pages))

		for _, assetURL := range assets {
			assetKey := normalizeURL(assetURL)
			if !s.reserveAsset(assetKey) {
				continue
			}
			s.bumpAssetsDiscovered()
			select {
			case downloadQueue <- assetURL:
			case <-ctx.Done():
				break crawlLoop
			}
		}

		if s.cfg.MaxDepth == 0 || task.depth < s.cfg.MaxDepth {
			for _, nextURL := range pages {
				if !s.inScope(nextURL) || !isLikelyPage(nextURL) {
					continue
				}
				if s.reservePage(normalizeURL(nextURL)) {
					queue = append(queue, pageTask{u: nextURL, depth: task.depth + 1})
				}
			}
		}

		if s.cfg.PageDelay > 0 {
			t := time.NewTimer(s.cfg.PageDelay)
			select {
			case <-ctx.Done():
				t.Stop()
				break crawlLoop
			case <-t.C:
			}
		}
	}

	close(downloadQueue)
	wg.Wait()

	stats := s.snapshotStats()
	if ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
		return stats, ctx.Err()
	}
	return stats, nil
}

func (s *Scraper) fetchAndExtract(ctx context.Context, pageURL *url.URL) ([]*url.URL, []*url.URL, error) {
	resp, err := s.doRequest(ctx, pageURL)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	ct := mediaType(resp.Header.Get("Content-Type"))
	if !isHTMLResponse(ct, pageURL.Path) {
		if isAssetResponse(ct, pageURL.Path) {
			return nil, []*url.URL{pageURL}, nil
		}
		return nil, nil, nil
	}

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("parse html: %w", err)
	}

	pages := make([]*url.URL, 0, 32)
	assets := make([]*url.URL, 0, 32)

	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			tag := strings.ToLower(n.Data)
			for _, attr := range n.Attr {
				key := strings.ToLower(strings.TrimSpace(attr.Key))
				if !isURLAttribute(key) {
					continue
				}
				for _, raw := range extractRawURLs(key, attr.Val) {
					u, err := resolveURL(pageURL, raw)
					if err != nil || !isHTTPURL(u) {
						continue
					}
					switch classifyLink(tag, key, u) {
					case "asset":
						assets = append(assets, u)
					case "page":
						pages = append(pages, u)
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}

	walk(doc)
	return dedupeURLs(pages), dedupeURLs(assets), nil
}

func (s *Scraper) downloadWorker(ctx context.Context, workerID int, q <-chan *url.URL) {
	for {
		select {
		case <-ctx.Done():
			return
		case assetURL, ok := <-q:
			if !ok {
				return
			}
			if err := s.downloadAsset(ctx, assetURL); err != nil {
				s.bumpDownloadErrors()
				s.logger.Warn("download failed", "worker", workerID, "url", assetURL.String(), "err", err)
			}
		}
	}
}

func (s *Scraper) downloadAsset(ctx context.Context, assetURL *url.URL) error {
	resp, err := s.doRequest(ctx, assetURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	ct := mediaType(resp.Header.Get("Content-Type"))
	if !isAssetResponse(ct, assetURL.Path) {
		s.bumpAssetsSkipped()
		_, _ = io.Copy(io.Discard, resp.Body)
		s.logger.Info("skipped non-asset response", "url", assetURL.String(), "content_type", ct)
		return nil
	}

	destPath := s.destinationPath(assetURL, ct)
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("create destination dir: %w", err)
	}

	if _, err := os.Stat(destPath); err == nil {
		s.bumpAssetsSkipped()
		_, _ = io.Copy(io.Discard, resp.Body)
		s.logger.Info("asset already exists, skipping", "path", destPath)
		return nil
	}

	tmpPath := destPath + ".part"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	n, copyErr := io.Copy(f, resp.Body)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write file: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close file: %w", closeErr)
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("finalize file: %w", err)
	}

	s.bumpAssetsDownloaded()
	s.logger.Info("asset downloaded", "url", assetURL.String(), "path", destPath, "bytes", n)
	return nil
}

func (s *Scraper) doRequest(ctx context.Context, target *url.URL) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= s.cfg.MaxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
		if err != nil {
			return nil, err
		}
		if s.cfg.UserAgent != "" {
			req.Header.Set("User-Agent", s.cfg.UserAgent)
		}
		for k, v := range s.cfg.ExtraHeaders {
			req.Header.Set(k, v)
		}

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			if attempt < s.cfg.MaxRetries {
				if waitErr := sleepWithContext(ctx, backoffForAttempt(attempt)); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			break
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			if attempt < s.cfg.MaxRetries {
				if waitErr := sleepWithContext(ctx, backoffForAttempt(attempt)); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			break
		}

		return resp, nil
	}
	if lastErr == nil {
		lastErr = errors.New("request failed")
	}
	return nil, lastErr
}

func (s *Scraper) destinationPath(u *url.URL, contentType string) string {
	host := sanitizeSegment(strings.ToLower(u.Hostname()))
	cleanPath := path.Clean("/" + strings.TrimSpace(u.Path))
	if cleanPath == "/." {
		cleanPath = "/"
	}

	parts := strings.Split(strings.Trim(cleanPath, "/"), "/")
	if len(parts) == 1 && parts[0] == "" {
		parts = nil
	}

	safeParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if decoded, err := url.PathUnescape(part); err == nil {
			part = decoded
		}
		safeParts = append(safeParts, sanitizeSegment(part))
	}

	fileName := "index"
	dirs := safeParts
	if len(safeParts) > 0 {
		fileName = safeParts[len(safeParts)-1]
		dirs = safeParts[:len(safeParts)-1]
	}

	ext := strings.ToLower(path.Ext(fileName))
	if ext == "" {
		if fromType := extensionForContentType(contentType); fromType != "" {
			fileName += fromType
		} else {
			fileName += ".bin"
		}
	}

	if u.RawQuery != "" {
		base := strings.TrimSuffix(fileName, path.Ext(fileName))
		ext = path.Ext(fileName)
		fileName = fmt.Sprintf("%s_q%s%s", base, shortHash(u.RawQuery), ext)
	}

	fileName = shortenFileName(fileName, 180)

	outDir := filepath.Join(s.cfg.OutputDir, host)
	if len(dirs) > 0 {
		outDir = filepath.Join(outDir, filepath.Join(dirs...))
	}
	return filepath.Join(outDir, fileName)
}

func (s *Scraper) inScope(u *url.URL) bool {
	host := strings.ToLower(u.Hostname())
	p := cleanPath(u.Path)
	for _, r := range s.scopeRules {
		if r.host != host {
			continue
		}
		if r.prefix == "/" || p == r.prefix || strings.HasPrefix(p, r.prefix+"/") {
			return true
		}
	}
	return false
}

func parseScope(seeds []string) ([]scopeRule, []pageTask, error) {
	scopes := make([]scopeRule, 0, len(seeds))
	queue := make([]pageTask, 0, len(seeds))

	for _, raw := range seeds {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			return nil, nil, fmt.Errorf("invalid seed url %q: %w", raw, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, nil, fmt.Errorf("seed url must be http(s): %q", raw)
		}
		if u.Hostname() == "" {
			return nil, nil, fmt.Errorf("seed url missing host: %q", raw)
		}
		u.Fragment = ""
		scopes = append(scopes, scopeRule{
			host:   strings.ToLower(u.Hostname()),
			prefix: cleanPath(u.Path),
		})
		queue = append(queue, pageTask{u: u, depth: 0})
	}

	// dedupe scope rules
	slices.SortFunc(scopes, func(a, b scopeRule) int {
		if a.host == b.host {
			return strings.Compare(a.prefix, b.prefix)
		}
		return strings.Compare(a.host, b.host)
	})
	uniq := make([]scopeRule, 0, len(scopes))
	for _, r := range scopes {
		if len(uniq) == 0 || uniq[len(uniq)-1] != r {
			uniq = append(uniq, r)
		}
	}

	return uniq, queue, nil
}

func resolveURL(base *url.URL, raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "#" {
		return nil, errors.New("empty url")
	}
	low := strings.ToLower(raw)
	if strings.HasPrefix(low, "javascript:") || strings.HasPrefix(low, "mailto:") || strings.HasPrefix(low, "tel:") {
		return nil, errors.New("unsupported scheme")
	}
	if strings.HasPrefix(raw, "//") {
		raw = base.Scheme + ":" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	resolved := base.ResolveReference(u)
	resolved.Fragment = ""
	return resolved, nil
}

func extractRawURLs(attrKey, value string) []string {
	if attrKey != "srcset" {
		v := strings.TrimSpace(value)
		if v == "" {
			return nil
		}
		return []string{v}
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Fields(part)
		if len(fields) > 0 {
			out = append(out, fields[0])
		}
	}
	return out
}

func classifyLink(tag, attrKey string, u *url.URL) string {
	if attrKey == "poster" {
		return "asset"
	}
	if tag == "img" || tag == "source" || tag == "video" || tag == "audio" || tag == "track" || tag == "embed" || tag == "object" {
		return "asset"
	}
	if tag == "a" || tag == "link" {
		if isAssetURLCandidate(u) {
			return "asset"
		}
		return "page"
	}
	if tag == "iframe" {
		return "page"
	}
	if isAssetURLCandidate(u) {
		return "asset"
	}
	if isLikelyPage(u) {
		return "page"
	}
	return ""
}

func isURLAttribute(attr string) bool {
	switch attr {
	case "href", "src", "data-src", "data-href", "poster", "srcset":
		return true
	default:
		return false
	}
}

func isHTTPURL(u *url.URL) bool {
	return u != nil && (u.Scheme == "http" || u.Scheme == "https")
}

func isLikelyPage(u *url.URL) bool {
	ext := strings.ToLower(path.Ext(u.Path))
	if ext == "" {
		return true
	}
	if _, ok := pageExtensions[ext]; ok {
		return true
	}
	if _, ok := assetExtensions[ext]; ok {
		return false
	}
	return false
}

func isAssetURLCandidate(u *url.URL) bool {
	if u == nil {
		return false
	}
	ext := strings.ToLower(path.Ext(u.Path))
	if _, ok := assetExtensions[ext]; ok {
		return true
	}
	return queryLooksAsset(u.RawQuery)
}

func queryLooksAsset(rawQuery string) bool {
	if rawQuery == "" {
		return false
	}
	q := strings.ToLower(rawQuery)
	for ext := range assetExtensions {
		if strings.Contains(q, ext) {
			return true
		}
	}
	for _, marker := range []string{"download", "attachment", "filename=", "file=", "doc=", "pdf=", "video=", "image="} {
		if strings.Contains(q, marker) {
			return true
		}
	}
	return false
}

func isHTMLResponse(contentType, pathValue string) bool {
	if strings.HasPrefix(contentType, "text/html") || strings.HasPrefix(contentType, "application/xhtml+xml") {
		return true
	}
	ext := strings.ToLower(path.Ext(pathValue))
	_, ok := pageExtensions[ext]
	return ok
}

func isAssetResponse(contentType, pathValue string) bool {
	if isAssetContentType(contentType) {
		return true
	}
	ext := strings.ToLower(path.Ext(pathValue))
	_, ok := assetExtensions[ext]
	return ok
}

func isAssetContentType(contentType string) bool {
	if contentType == "" {
		return false
	}
	if strings.HasPrefix(contentType, "image/") || strings.HasPrefix(contentType, "video/") {
		return true
	}
	return slices.Contains(documentContentTypes, contentType)
}

func extensionForContentType(contentType string) string {
	if contentType == "" {
		return ""
	}
	if ext, ok := commonContentTypeExt[contentType]; ok {
		return ext
	}
	if exts, err := mime.ExtensionsByType(contentType); err == nil && len(exts) > 0 {
		return exts[0]
	}
	return ""
}

func mediaType(ct string) string {
	if ct == "" {
		return ""
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
	}
	return strings.ToLower(strings.TrimSpace(mt))
}

func dedupeURLs(in []*url.URL) []*url.URL {
	if len(in) < 2 {
		return in
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]*url.URL, 0, len(in))
	for _, u := range in {
		k := normalizeURL(u)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, u)
	}
	return out
}

func normalizeURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	copyURL := *u
	copyURL.Fragment = ""
	if copyURL.Path == "" {
		copyURL.Path = "/"
	}
	copyURL.Host = strings.ToLower(copyURL.Host)
	copyURL.Scheme = strings.ToLower(copyURL.Scheme)
	return copyURL.String()
}

func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	out := path.Clean("/" + strings.TrimSpace(p))
	if out == "." || out == "" {
		return "/"
	}
	return out
}

func sanitizeSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "file"
	}
	var b strings.Builder
	b.Grow(len(s))
	prevUnderscore := false
	for _, r := range s {
		safe := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
		if safe {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}
		if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "._")
	if out == "" {
		out = "file"
	}
	if len(out) > 120 {
		out = out[:120]
	}
	return out
}

func shortHash(input string) string {
	sum := sha1.Sum([]byte(input))
	return hex.EncodeToString(sum[:])[:10]
}

func shortenFileName(name string, limit int) string {
	if len(name) <= limit {
		return name
	}
	ext := path.Ext(name)
	base := strings.TrimSuffix(name, ext)
	if len(ext) > limit {
		return name[:limit]
	}
	maxBase := limit - len(ext)
	if maxBase < 1 {
		return name[:limit]
	}
	return base[:maxBase] + ext
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func backoffForAttempt(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 500 * time.Millisecond
	case 2:
		return 1500 * time.Millisecond
	default:
		return 3 * time.Second
	}
}

func (s *Scraper) reservePage(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.seenPages[key]; exists {
		return false
	}
	s.seenPages[key] = struct{}{}
	return true
}

func (s *Scraper) reserveAsset(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.seenAssets[key]; exists {
		return false
	}
	s.seenAssets[key] = struct{}{}
	return true
}

func (s *Scraper) bumpPagesVisited() {
	s.mu.Lock()
	s.stats.PagesVisited++
	s.mu.Unlock()
}

func (s *Scraper) bumpAssetsDiscovered() {
	s.mu.Lock()
	s.stats.AssetsDiscovered++
	s.mu.Unlock()
}

func (s *Scraper) bumpAssetsDownloaded() {
	s.mu.Lock()
	s.stats.AssetsDownloaded++
	s.mu.Unlock()
}

func (s *Scraper) bumpAssetsSkipped() {
	s.mu.Lock()
	s.stats.AssetsSkipped++
	s.mu.Unlock()
}

func (s *Scraper) bumpDownloadErrors() {
	s.mu.Lock()
	s.stats.DownloadErrors++
	s.mu.Unlock()
}

func (s *Scraper) snapshotStats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}
