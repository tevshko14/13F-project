# Font Licenses

## Geist Sans + Geist Mono

- **Files**: `Geist-Variable.woff2`, `GeistMono-Variable.woff2`
- **License**: SIL Open Font License 1.1
- **Source**: <https://github.com/vercel/geist-font>
- **Mirror used to fetch**: <https://cdn.jsdelivr.net/npm/geist@1/>

The Geist typeface family is a free typeface designed by Vercel and
released under the SIL Open Font License, which permits embedding and
redistribution of the binaries in software products as long as the font
files themselves are not sold.

These are **variable** woff2 files: one file per family covers all
weights (300–800 for Geist Sans, 400–700 for Geist Mono).  Loaded via
`@font-face` in `static/css/redesign/tokens.css` with `font-display:swap`
so site rendering is never blocked on font fetch.

Please keep this file alongside the .woff2 binaries.
