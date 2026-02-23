# blackroad-web-archiver

Production-grade web snapshot tool. Archives URLs with SHA-256 checksums, link extraction, and ZIP bundle export. Uses Python stdlib only (no requests).

## Features

- Archive any URL with configurable crawl depth
- SHA-256 checksumming of every snapshot
- Extract all links from HTML pages
- Same-origin crawl filtering
- Compare two snapshots with unified diff
- Export job bundle as ZIP archive
- SQLite persistence in `~/.blackroad/web_archiver.db`

## Usage

```bash
# Archive a URL (depth 1)
python web_archiver.py archive https://example.com

# Archive with crawl depth 2
python web_archiver.py archive https://example.com --depth 2

# List all archived jobs
python web_archiver.py list

# Get job details
python web_archiver.py get <job-id>

# Export as ZIP
python web_archiver.py export <job-id> --output archive.zip

# Compare two snapshots
python web_archiver.py compare <job-id-1> <job-id-2>

# Show extracted links
python web_archiver.py links <job-id>
```

## Testing

```bash
pip install pytest
pytest tests/ -v
```

## Architecture

- **`web_archiver.py`** — Core library + CLI (450+ lines)
- **SQLite tables**: `archive_jobs`, `crawled_pages`, `extracted_links`
- **No external dependencies** — uses `urllib.request`, `html.parser`, `difflib`
