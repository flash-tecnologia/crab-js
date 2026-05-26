# Crab PDF Studio

This folder is ready for Netlify Drop.

1. Open https://app.netlify.com/drop
2. Drag this whole folder into the page.
3. Open the generated Netlify URL.

For a Git-connected Netlify site, use this folder as the site base directory. The included
`netlify.toml` sets the publish directory to this folder and configures the same headers as `_headers`.

Source:

- https://github.com/flash-tecnologia/crab-js
- https://github.com/flash-tecnologia/crab-js/tree/main/packages/pdf-crab-js
- https://github.com/flash-tecnologia/crab-js/tree/main/packages/html-to-pdf-crab-js

The \_headers file enables the COOP/COEP headers required by SharedArrayBuffer.

The `assets/Tuffy.ttf` font is included because `html-to-pdf-crab-js` needs an explicit font in
the browser WASM path; without it, text may be missing from the rendered PDF.
