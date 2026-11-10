# Documentation website

The documentation website is hosted at
<https://redis.github.io/redis-apache-arrow-flight/>.
MkDocs renders the repository README as the home page and each Markdown file
under `docs/` as a separate HTML page. Edit those original files; there is no
second copy of their content to maintain. Source-code links point to GitHub.

## Preview and build

```sh
python3 -m venv .local/docs-venv
.local/docs-venv/bin/python -m pip install -r requirements-docs.txt
.local/docs-venv/bin/python -m mkdocs serve
```

Open the local address printed by MkDocs. To run the same build as CI:

```sh
.local/docs-venv/bin/python -m mkdocs build --strict
```

The generated site is in `site/`, which is ignored by Git. New Markdown files in
`docs/` are published automatically; add important guides to `nav` in
`mkdocs.yml` so readers can find them in the menu. Documents outside `docs/`
(apart from the README) remain on GitHub.

## Publishing

In the repository's **Settings → Pages → Build and deployment**, select
**GitHub Actions** as the source. The Documentation workflow validates pull
requests and publishes after documentation changes reach `main`. It can also
be run manually on `main`. Repository or environment policies may require
deployment approval.

## Google discovery

The public HTML includes canonical page URLs, descriptive titles, navigation
links, and a generated [sitemap](https://redis.github.io/redis-apache-arrow-flight/sitemap.xml).
The site's search box searches its documentation independently of Google.

After the first deployment, a site owner can add the URL-prefix property
`https://redis.github.io/redis-apache-arrow-flight/` in Google Search Console,
complete Google's ownership verification, and submit `sitemap.xml`. Verification
may require updating the publishing hook to place Google's supplied HTML file
at the exact verification URL Google provides. Google controls indexing and its
timing.
