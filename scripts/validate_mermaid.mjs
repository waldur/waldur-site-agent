#!/usr/bin/env node
// Validate mermaid code blocks embedded in markdown files.
// Run as: node scripts/validate_mermaid.mjs <file.md> [<file.md> ...]
// Uses mermaid.parse() with a jsdom shim — no Chromium download.

import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve, join } from "node:path";
import { pathToFileURL } from "node:url";

// pre-commit / prek install node deps in `<env>/lib/node_modules`, while the
// script lives in the repo. Node's default ESM resolver only walks up from the
// script's directory, so explicitly check the env's prefix too.
function findModuleUrl(name) {
  const req = createRequire(import.meta.url);
  try {
    return pathToFileURL(req.resolve(name)).href;
  } catch {
    /* fall through */
  }
  const candidates = [
    join(dirname(dirname(process.execPath)), "lib", "node_modules", name),
    join(dirname(process.execPath), "node_modules", name),
  ];
  for (const root of candidates) {
    const pkg = join(root, "package.json");
    if (!existsSync(pkg)) continue;
    const { main, module: mod, exports: exp } = req(pkg);
    let entry = mod || main || "index.js";
    if (exp && typeof exp === "object") {
      const dot = exp["."];
      if (typeof dot === "string") entry = dot;
      else if (dot && typeof dot === "object") {
        entry = dot.import || dot.default || dot.require || entry;
      }
    }
    return pathToFileURL(resolve(root, entry)).href;
  }
  throw new Error(`cannot locate module '${name}'`);
}

const { JSDOM } = await import(findModuleUrl("jsdom"));

const dom = new JSDOM("<!DOCTYPE html><html><body></body></html>", {
  pretendToBeVisual: true,
  url: "http://localhost/",
});

const stub = (name, value) => {
  if (value === undefined) return;
  try {
    Object.defineProperty(globalThis, name, {
      value,
      writable: true,
      configurable: true,
    });
  } catch {
    // Already defined as a non-configurable getter in this Node version — skip.
  }
};

stub("window", dom.window);
stub("document", dom.window.document);
stub("navigator", dom.window.navigator);
stub("DOMParser", dom.window.DOMParser);
stub("Node", dom.window.Node);
stub("SVGElement", dom.window.SVGElement);

const { default: mermaid } = await import(findModuleUrl("mermaid"));
mermaid.initialize({ startOnLoad: false, securityLevel: "loose" });

const BLOCK_RE = /^```mermaid[^\n]*\n([\s\S]*?)\n```/gm;

let failures = 0;

for (const file of process.argv.slice(2)) {
  let text;
  try {
    text = await readFile(file, "utf8");
  } catch (err) {
    console.error(`${file}: cannot read (${err.message})`);
    failures += 1;
    continue;
  }

  let index = 0;
  for (const match of text.matchAll(BLOCK_RE)) {
    index += 1;
    const offset = match.index ?? 0;
    const line = text.slice(0, offset).split("\n").length;
    try {
      await mermaid.parse(match[1]);
    } catch (err) {
      const message = err && err.message ? err.message : String(err);
      console.error(`${file}:${line}: mermaid block #${index} failed to parse`);
      for (const detail of message.split("\n")) {
        if (detail.trim()) console.error(`  ${detail}`);
      }
      failures += 1;
    }
  }
}

process.exit(failures === 0 ? 0 : 1);
