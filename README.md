# UFO Asset Scraper (Go)

This service crawls pages under:

- `https://war.gov/ufo`
- `https://war.gov/ufo/release`

It discovers in-scope pages and downloads linked images, videos, PDFs, and document files.

## What it does

- Crawls breadth-first, page-by-page from seed URLs
- Stays in seed path scope on the same host
- Detects assets via HTML tags, URL extensions, and response content-type
- Deduplicates pages and assets
- Downloads assets concurrently
- Preserves host/path structure under an output directory

## Run

```bash
go run ./cmd/ufo-scraper \
  -out ./downloads \
  -max-pages 0 \
  -max-depth 0 \
  -download-workers 8
```

## Useful flags

- `-seeds` comma-separated seeds (default includes both UFO paths)
- `-out` output directory
- `-max-pages` cap number of crawled pages (`0` = unlimited)
- `-max-depth` crawl depth from seeds (`0` = unlimited)
- `-page-delay` delay between page requests (default `750ms`)
- `-download-workers` parallel download count
- `-timeout` per-request timeout
- `-max-retries` retry count for transient failures (`429/5xx` + network errors)
- `-user-agent` override UA string
- `-proxy` proxy URL (helpful if access is geofenced)
- `-header "Key: Value"` add custom request headers (repeatable)

## Example with Thailand egress/proxy

```bash
go run ./cmd/ufo-scraper \
  -proxy socks5://127.0.0.1:1080 \
  -header "Accept-Language: th-TH,th;q=0.9,en-US;q=0.8" \
  -header "Referer: https://war.gov/"
```
# uap
