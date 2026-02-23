"""Tests for BlackRoad Web Archiver."""
import os
import json
import pytest
from web_archiver import (
    extract_title, extract_links, compare_snapshots, list_jobs, stats,
    get_db, ArchiveJob, _sha256, _job_id,
)

SAMPLE_HTML = b"""<!DOCTYPE html>
<html>
<head><title>Test Page</title></head>
<body>
  <h1>Hello World</h1>
  <a href="https://example.com/page1">Page 1</a>
  <a href="/relative-page">Relative</a>
  <a href="https://other.com/external">External</a>
  <a href="#">Hash link</a>
  <a href="javascript:void(0)">JS link</a>
</body>
</html>"""

SAMPLE_HTML_2 = b"""<!DOCTYPE html>
<html>
<head><title>Updated Page</title></head>
<body>
  <h1>Hello World - Updated</h1>
  <a href="https://example.com/page2">Page 2</a>
</body>
</html>"""


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test_archiver.db")


def test_extract_title():
    assert extract_title(SAMPLE_HTML.decode()) == "Test Page"


def test_extract_title_missing():
    assert extract_title("<html><body>no title</body></html>") == "(untitled)"


def test_extract_links():
    links = extract_links(SAMPLE_HTML.decode(), "https://example.com")
    urls = [u for u, _ in links]
    assert "https://example.com/page1" in urls
    assert "https://example.com/relative-page" in urls
    assert "https://other.com/external" in urls
    # Hash and JS links should be excluded
    assert "#" not in urls
    assert not any("javascript:" in u for u in urls)


def test_extract_links_deduplication():
    html = """<html><body>
        <a href="https://example.com/page">Link 1</a>
        <a href="https://example.com/page">Link 2</a>
    </body></html>"""
    links = extract_links(html, "https://example.com")
    urls = [u for u, _ in links]
    assert len(urls) == len(set(urls))


def test_extract_links_relative_resolution():
    html = '<html><body><a href="/about">About</a></body></html>'
    links = extract_links(html, "https://mysite.com/page")
    assert ("https://mysite.com/about", "About") in links


def test_extract_links_text():
    html = '<html><body><a href="https://example.com">Click here</a></body></html>'
    links = extract_links(html, "https://example.com")
    assert len(links) == 1
    assert links[0][1] == "Click here"


def test_sha256_consistency():
    h1 = _sha256(b"test content")
    h2 = _sha256(b"test content")
    assert h1 == h2
    assert len(h1) == 64


def test_sha256_different():
    assert _sha256(b"content a") != _sha256(b"content b")


def test_db_schema(tmp_path):
    db_path = str(tmp_path / "fresh.db")
    conn = get_db(db_path)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "archive_jobs" in tables
    assert "crawled_pages" in tables
    assert "extracted_links" in tables


def test_list_jobs_empty(tmp_db):
    assert list_jobs(db_path=tmp_db) == []


def test_list_jobs_with_filter(tmp_db):
    conn = get_db(tmp_db)
    now = "2024-01-01T00:00:00Z"
    conn.execute("""
        INSERT INTO archive_jobs (id, url, created_at, status, crawl_depth)
        VALUES ('j1', 'https://a.com', ?, 'success', 1)
    """, (now,))
    conn.execute("""
        INSERT INTO archive_jobs (id, url, created_at, status, crawl_depth)
        VALUES ('j2', 'https://b.com', ?, 'failed', 1)
    """, (now,))
    conn.commit()

    all_jobs = list_jobs(db_path=tmp_db)
    assert len(all_jobs) == 2

    success_jobs = list_jobs(status="success", db_path=tmp_db)
    assert len(success_jobs) == 1
    assert success_jobs[0].url == "https://a.com"

    failed_jobs = list_jobs(status="failed", db_path=tmp_db)
    assert len(failed_jobs) == 1


def test_stats_empty(tmp_db):
    s = stats(db_path=tmp_db)
    assert s["total_jobs"] == 0
    assert s["successful"] == 0
    assert s["failed"] == 0


def test_stats_populated(tmp_db):
    conn = get_db(tmp_db)
    now = "2024-01-01T00:00:00Z"
    conn.execute("""
        INSERT INTO archive_jobs (id, url, created_at, status, crawl_depth, content_length)
        VALUES ('j1', 'https://a.com', ?, 'success', 1, 5000)
    """, (now,))
    conn.execute("""
        INSERT INTO archive_jobs (id, url, created_at, status, crawl_depth, content_length)
        VALUES ('j2', 'https://b.com', ?, 'failed', 1, 0)
    """, (now,))
    conn.commit()
    s = stats(db_path=tmp_db)
    assert s["total_jobs"] == 2
    assert s["successful"] == 1
    assert s["failed"] == 1
    assert s["total_content_bytes"] == 5000


def test_archive_job_dataclass():
    job = ArchiveJob(
        id="test123", url="https://example.com", title="Example",
        snapshot_html="/tmp/snap.html", screenshot_path="",
        crawl_depth=1, created_at="2024-01-01T00:00:00Z",
        checksum="abc123", status="success",
        content_length=1024, links_found=5, error_message=""
    )
    d = job.to_dict()
    assert d["id"] == "test123"
    assert d["url"] == "https://example.com"
    assert d["status"] == "success"


def test_compare_snapshots_missing_jobs(tmp_db):
    with pytest.raises(ValueError):
        compare_snapshots("nonexistent1", "nonexistent2", db_path=tmp_db)


def test_compare_snapshots_same(tmp_db, tmp_path):
    conn = get_db(tmp_db)
    # Create two jobs pointing to same file
    snap_file = str(tmp_path / "snap.html")
    with open(snap_file, "w") as fh:
        fh.write(SAMPLE_HTML.decode())

    checksum = _sha256(SAMPLE_HTML)
    now = "2024-01-01T00:00:00Z"
    conn.execute("""
        INSERT INTO archive_jobs
            (id, url, title, snapshot_html, screenshot_path, crawl_depth,
             created_at, checksum, status, content_length, links_found, error_message)
        VALUES (?, ?, ?, ?, '', 1, ?, ?, 'success', 0, 0, '')
    """, ("j1", "https://a.com", "Test", snap_file, now, checksum))
    conn.execute("""
        INSERT INTO archive_jobs
            (id, url, title, snapshot_html, screenshot_path, crawl_depth,
             created_at, checksum, status, content_length, links_found, error_message)
        VALUES (?, ?, ?, ?, '', 1, ?, ?, 'success', 0, 0, '')
    """, ("j2", "https://a.com", "Test", snap_file, now, checksum))
    conn.commit()

    result = compare_snapshots("j1", "j2", db_path=tmp_db)
    assert result["same_checksum"] is True
    assert result["lines_added"] == 0
    assert result["lines_removed"] == 0


def test_compare_snapshots_different(tmp_db, tmp_path):
    conn = get_db(tmp_db)
    snap1 = str(tmp_path / "snap1.html")
    snap2 = str(tmp_path / "snap2.html")
    with open(snap1, "wb") as fh:
        fh.write(SAMPLE_HTML)
    with open(snap2, "wb") as fh:
        fh.write(SAMPLE_HTML_2)

    now = "2024-01-01T00:00:00Z"
    conn.execute("""
        INSERT INTO archive_jobs
            (id, url, title, snapshot_html, screenshot_path, crawl_depth,
             created_at, checksum, status, content_length, links_found, error_message)
        VALUES ('j1', 'https://a.com', 'T', ?, '', 1, ?, ?, 'success', 0, 0, '')
    """, (snap1, now, _sha256(SAMPLE_HTML)))
    conn.execute("""
        INSERT INTO archive_jobs
            (id, url, title, snapshot_html, screenshot_path, crawl_depth,
             created_at, checksum, status, content_length, links_found, error_message)
        VALUES ('j2', 'https://a.com', 'T2', ?, '', 1, ?, ?, 'success', 0, 0, '')
    """, (snap2, now, _sha256(SAMPLE_HTML_2)))
    conn.commit()

    result = compare_snapshots("j1", "j2", db_path=tmp_db)
    assert result["same_checksum"] is False
    assert result["lines_added"] > 0 or result["lines_removed"] > 0


def test_job_id_format():
    jid = _job_id("https://example.com")
    assert "_" in jid
    assert len(jid) > 10


def test_extract_links_empty_html():
    links = extract_links("", "https://example.com")
    assert links == []


def test_extract_links_no_anchors():
    html = "<html><body><p>No links here</p></body></html>"
    links = extract_links(html, "https://example.com")
    assert links == []
