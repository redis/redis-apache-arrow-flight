"""Publish the existing README and docs without duplicating Markdown sources."""

import posixpath
import re
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from mkdocs.structure.files import File, Files

ROOT = Path(__file__).resolve().parent.parent
# Inline links in the repository use simple, unquoted relative destinations.
LINK = re.compile(r"(\]\()([^\s)]+)(\))")
FENCE = re.compile(r"^\s*(`{3,}|~{3,})")


def on_files(files, config):
    # Keep docs/ in published source paths so existing relative links still work.
    published = Files(
        File(
            f"docs/{file.src_uri}",
            str(ROOT),
            config.site_dir,
            config.use_directory_urls,
        ) if file.abs_src_path and Path(file.abs_src_path).is_relative_to(ROOT / "docs") else file
        for file in files
    )
    published.append(
        File.generated(config, "index.md", content=(ROOT / "README.md").read_text())
    )
    return published


def on_page_markdown(markdown, page, config, files):
    def rewrite(match):
        target = urlsplit(match[2])
        if target.scheme or target.netloc or not target.path or target.path.startswith("/"):
            return match[0]
        path = posixpath.normpath(
            posixpath.join(posixpath.dirname(page.file.src_uri), target.path)
        )
        if path == "README.md":
            destination = posixpath.relpath("index.md", posixpath.dirname(page.file.src_uri) or ".")
            url = urlunsplit(("", "", destination, target.query, target.fragment))
        elif files.get_file_from_path(path) is not None or (path.startswith("docs/") and path.endswith(".md")):
            return match[0]
        else:
            # Source code, plans, and benchmark artifacts stay in the repository.
            # MkDocs would otherwise leave these as broken site-relative URLs.
            url = urlunsplit(("", "", f"{config.repo_url}/blob/main/{path}", target.query, target.fragment))
        return f"{match[1]}{url}{match[3]}"

    lines = []
    fence = None
    for line in markdown.splitlines(keepends=True):
        marker = FENCE.match(line)
        if marker:
            token = marker[1]
            if fence is None:
                fence = token
            elif token[0] == fence[0] and len(token) >= len(fence):
                fence = None
            lines.append(line)
        else:
            lines.append(line if fence else LINK.sub(rewrite, line))
    return "".join(lines)
