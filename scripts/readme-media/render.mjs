// The README's still images, rendered from hand-written HTML by a headless Chromium (Playwright), at twice their
// CSS size so they stay sharp on any screen.
//
//   node render.mjs marks  <page.html> <outdir>   every <figure data-out="name.png"> in the page → <outdir>/name.png,
//                                                  on a transparent background (the wordmark, light and dark)
//   node render.mjs figure <page.html> <out.png>  the page's one <figure>, framed: rounded corners, a soft shadow and
//                                                  a transparent margin baked in, since GitHub has no CSS for a
//                                                  README's images (the "how it works" diagram)
//
// Playwright is not a dependency of hpc-bridge. Point NODE_PATH at any node_modules that has it (`playwright` or
// `@playwright/test`) with its Chromium installed; build.sh does. The fonts are the system's (no web fonts), so a
// render on another OS sets the type in other faces: re-render on macOS to match the committed PNGs.

import { createRequire } from "node:module";
import { mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

// ESM ignores NODE_PATH; a CommonJS require honours it.
const require = createRequire(import.meta.url);
function playwright() {
  for (const name of ["playwright", "@playwright/test"]) {
    try {
      return require(name);
    } catch {
      /* try the next */
    }
  }
  console.error("render.mjs: Playwright not found. Set NODE_PATH to a node_modules that has `playwright`.");
  process.exit(2);
}

/** The frame every framed still gets, in CSS px: a transparent margin that holds the shadow, which falls downward. */
const FRAME = { top: 16, side: 20, bottom: 28, radius: 14 };
const SHADOW = "0 0 0 1px #10233a1f, 0 2px 5px #10233a26, 0 8px 20px -6px #10233a59";

async function settle(page) {
  await page.evaluate("document.fonts.ready.then(() => true)");
}

async function marks(browser, html, outdir) {
  mkdirSync(outdir, { recursive: true });
  const page = await browser.newPage({ viewport: { width: 1400, height: 1000 }, deviceScaleFactor: 2 });
  await page.goto(pathToFileURL(html).href);
  await settle(page);
  const figs = page.locator("figure[data-out]");
  const n = await figs.count();
  if (n === 0) throw new Error(`${html}: no <figure data-out="…">`);
  for (let i = 0; i < n; i++) {
    const fig = figs.nth(i);
    const out = join(outdir, await fig.getAttribute("data-out"));
    await fig.screenshot({ path: out, omitBackground: true });
    console.log(`made ${out}`);
  }
  await page.close();
}

async function figure(browser, html, out) {
  mkdirSync(dirname(out), { recursive: true });
  const page = await browser.newPage({ viewport: { width: 1400, height: 1200 }, deviceScaleFactor: 2 });
  await page.goto(pathToFileURL(html).href);
  await settle(page);
  const png = await page.locator("figure").first().screenshot();
  await page.close();
  // Frame it: the screenshot (at 2x) as an <img> at its CSS size, with the corners and shadow, on nothing.
  const [w, h] = [png.readUInt32BE(16) / 2, png.readUInt32BE(20) / 2];
  const frame = await browser.newPage({
    viewport: { width: Math.ceil(w) + 2 * FRAME.side, height: Math.ceil(h) + FRAME.top + FRAME.bottom },
    deviceScaleFactor: 2,
  });
  await frame.setContent(
    `<style>html, body { margin: 0; background: transparent; } img { display: block; margin: ${FRAME.top}px ${FRAME.side}px; width: ${w}px; height: ${h}px; border-radius: ${FRAME.radius}px; box-shadow: ${SHADOW}; }</style><img src="data:image/png;base64,${png.toString("base64")}">`,
  );
  await frame.locator("img").evaluate((img) => img.decode());
  await frame.screenshot({ path: out, omitBackground: true });
  await frame.close();
  console.log(`made ${out}`);
}

const [mode, src, dest] = process.argv.slice(2);
if (!["marks", "figure"].includes(mode) || !src || !dest) {
  console.error("usage: node render.mjs marks <page.html> <outdir> | figure <page.html> <out.png>");
  process.exit(2);
}
const { chromium } = playwright();
const browser = await chromium.launch();
try {
  if (mode === "marks") await marks(browser, resolve(src), resolve(dest));
  else await figure(browser, resolve(src), resolve(dest));
} finally {
  await browser.close();
}
