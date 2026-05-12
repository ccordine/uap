package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"uap/internal/scraper"
)

type headerFlags []string

func (h *headerFlags) String() string {
	return strings.Join(*h, ",")
}

func (h *headerFlags) Set(v string) error {
	v = strings.TrimSpace(v)
	if v == "" {
		return fmt.Errorf("header cannot be empty")
	}
	*h = append(*h, v)
	return nil
}

func main() {
	var (
		seedsArg        string
		outDir          string
		maxPages        int
		maxDepth        int
		pageDelay       time.Duration
		timeout         time.Duration
		downloadWorkers int
		userAgent       string
		proxyURL        string
		maxRetries      int
		headersArg      headerFlags
	)

	flag.StringVar(&seedsArg, "seeds", "https://war.gov/ufo,https://war.gov/ufo/release", "Comma-separated seed URLs")
	flag.StringVar(&outDir, "out", "downloads", "Directory to store downloaded files")
	flag.IntVar(&maxPages, "max-pages", 0, "Maximum pages to crawl (0 means unlimited)")
	flag.IntVar(&maxDepth, "max-depth", 0, "Maximum crawl depth from seed URLs (0 means unlimited)")
	flag.DurationVar(&pageDelay, "page-delay", 750*time.Millisecond, "Delay between page fetches")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "HTTP request timeout")
	flag.IntVar(&downloadWorkers, "download-workers", 6, "Number of parallel download workers")
	flag.StringVar(&userAgent, "user-agent", "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0", "HTTP User-Agent")
	flag.StringVar(&proxyURL, "proxy", "", "Optional proxy URL, e.g. http://127.0.0.1:8080 or socks5://host:port")
	flag.IntVar(&maxRetries, "max-retries", 3, "Number of retries for transient request failures")
	flag.Var(&headersArg, "header", "Additional request header (repeatable), format: 'Key: Value'")
	flag.Parse()

	seeds := splitAndTrim(seedsArg)
	if len(seeds) == 0 {
		fmt.Fprintln(os.Stderr, "at least one seed URL is required")
		os.Exit(2)
	}

	extraHeaders, err := parseHeaders(headersArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid header: %v\n", err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := scraper.Config{
		Seeds:           seeds,
		OutputDir:       outDir,
		MaxPages:        maxPages,
		MaxDepth:        maxDepth,
		PageDelay:       pageDelay,
		RequestTimeout:  timeout,
		DownloadWorkers: downloadWorkers,
		UserAgent:       userAgent,
		ProxyURL:        proxyURL,
		ExtraHeaders:    extraHeaders,
		MaxRetries:      maxRetries,
	}

	s, err := scraper.New(cfg, logger)
	if err != nil {
		logger.Error("invalid config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	stats, runErr := s.Run(ctx)
	if runErr != nil && runErr != context.Canceled {
		logger.Error("scrape failed", "err", runErr)
		os.Exit(1)
	}

	logger.Info("scrape finished",
		"pages_visited", stats.PagesVisited,
		"assets_discovered", stats.AssetsDiscovered,
		"assets_downloaded", stats.AssetsDownloaded,
		"assets_skipped", stats.AssetsSkipped,
		"download_errors", stats.DownloadErrors,
	)
}

func splitAndTrim(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseHeaders(raw []string) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(raw))
	for _, line := range raw {
		idx := strings.Index(line, ":")
		if idx <= 0 || idx == len(line)-1 {
			return nil, fmt.Errorf("%q must be in 'Key: Value' format", line)
		}
		k := strings.TrimSpace(line[:idx])
		v := strings.TrimSpace(line[idx+1:])
		if k == "" || v == "" {
			return nil, fmt.Errorf("%q has empty key or value", line)
		}
		out[k] = v
	}
	return out, nil
}
