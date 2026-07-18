/* shelf3d.js — the "shelves" tab as a walkable first-person library.
   A candlelit rotunda somewhere between the Beast's castle library and a
   dark-academia reading room: towering stacks, a candle chandelier, moonlit
   gothic windows, an enchanted rose under glass — and every book a physical
   object bound in cloth with its real cover. You walk it in first person
   (WASD + mouse-look on desktop, dual-thumb controls on touch); GSAP flights
   send books arcing through the air when the grouping changes. Falls back to
   the DOM bookcase (ShelfDOM) when WebGL isn't available. Reuses app.js
   globals (api, authorsOf, openShelfBook). */

import * as THREE from "./vendor/three.module.min.js";

(() => {
  "use strict";

  /* ---------- shared config (kept in step with the DOM fallback) ---------- */

  const FORMAT_ORDER = ["hardcover", "paperback", "mass market", "special edition", "ebook", "audiobook", "other", "unknown"];

  const PALETTE = [
    "#6e3b3f", "#7a4a33", "#2f5d43", "#55603a", "#8a6d3b",
    "#3b5372", "#2f5b5e", "#5d3b5e", "#4b3a63", "#71543e",
  ];

  const ENRICH_ROUNDS = 6;
  const ENRICH_BATCH = 12;

  /* ---------- world dimensions (meters) ---------- */

  const BAY = {
    w: 1.5,          // outer width of one bookcase bay
    depth: 0.30,
    side: 0.05,      // side panel thickness
    rowH: 0.32,      // vertical space per shelf row
    shelfT: 0.03,    // plank thickness
    baseH: 0.16,     // plinth
    crownH: 0.10,
    rows: 4,         // shelf rows per bay
    gap: 0.14,       // spacing between neighbouring bays along the arc
  };
  BAY.run = BAY.w - 2 * BAY.side - 0.08;                       // usable book run per row
  BAY.h = BAY.baseH + BAY.rows * BAY.rowH + BAY.crownH;        // full case height

  const BOOK_GAP = 0.006;   // breathing room between spines
  const TEX_SIZE = 256;     // per-book canvas texture (POT for mipmaps)

  // texture regions as u-ranges on the 256px canvas
  const REG = { cloth: [0, 16], pages: [16, 32], spine: [32, 96], cover: [96, 256] };

  const EYE = 1.55;         // first-person eye height

  /* ---------- dom ---------- */

  const viewEl = document.getElementById("view-shelves");
  const countEl = document.getElementById("shelves-count");
  const worldEl = document.getElementById("world3d");
  const caseEl = document.getElementById("bookcase");
  const hintEl = document.getElementById("world3d-hint");
  const crossEl = document.getElementById("world3d-crosshair");
  const focusEl = document.getElementById("world3d-focus");
  const stickEl = document.getElementById("world3d-stick");
  const stickNub = document.getElementById("world3d-stick-nub");

  const reducedMotion = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const coarsePointer = () => window.matchMedia("(pointer: coarse)").matches;
  const viewVisible = () => !viewEl.classList.contains("hidden");

  /* ---------- small helpers ---------- */

  function hashStr(s) {
    let h = 0;
    for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
    return Math.abs(h);
  }

  const clothOf = (b) => PALETTE[hashStr(`${b.title || ""}|${authorsOf(b)}`) % PALETTE.length];

  function shade(hex, f) {
    const n = parseInt(hex.slice(1), 16);
    const ch = (x) => Math.max(0, Math.min(255, Math.round(x * f)));
    return `rgb(${ch(n >> 16)},${ch((n >> 8) & 255)},${ch(n & 255)})`;
  }

  // deterministic per-book jitter in [-1, 1) so layouts are stable between builds
  const jitter = (b, salt) => ((hashStr(`${b.id}|${salt}`) % 1000) / 500) - 1;

  function upgradeCover(raw) {
    if (!raw) return "";
    try {
      const u = new URL(raw);
      if (u.hostname === "covers.openlibrary.org") {
        u.protocol = "https:";
        u.pathname = u.pathname.replace(/-M(\.jpg)$/i, "-L$1");
      } else if (u.hostname.endsWith("books.google.com") || u.hostname.endsWith("books.googleusercontent.com")) {
        u.protocol = "https:";
        u.searchParams.set("zoom", "1");
        u.searchParams.delete("edge");
      }
      // distinct url for the CORS-mode fetch so it never collides with the
      // opaque no-cors copies the service worker cached for <img> tags
      if (u.protocol === "http:" || u.protocol === "https:") u.searchParams.set("cors3d", "1");
      return u.toString();
    } catch {
      return raw;
    }
  }

  /* ---------- grouping (same rules as the DOM fallback) ---------- */

  function surnameOf(item) {
    const first = authorsOf(item).split(",")[0].trim();
    if (!first) return "~";
    const parts = first.split(/\s+/);
    return parts[parts.length - 1].toLowerCase();
  }

  const byTitle = (a, b) =>
    (a.title || "").toLowerCase().localeCompare((b.title || "").toLowerCase());
  const byAuthorTitle = (a, b) => surnameOf(a).localeCompare(surnameOf(b)) || byTitle(a, b);
  const byDateTitle = (a, b) =>
    (a.published_date || "9999").localeCompare(b.published_date || "9999") || byTitle(a, b);

  const fmtRank = (key) => {
    const i = FORMAT_ORDER.indexOf(key);
    return i === -1 ? FORMAT_ORDER.length : i;
  };

  function computeGroups(books, mode) {
    const map = new Map();
    for (const b of books) {
      let key;
      if (mode === "genre") key = (b.genre || "").trim().toLowerCase() || "uncategorized";
      else if (mode === "format") key = b.format || "unknown";
      else key = authorsOf(b).split(",")[0].trim() || "unknown author";
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(b);
    }
    const groups = [...map.entries()].map(([key, items]) => ({ key, items }));

    if (mode === "genre") {
      groups.forEach((g) => g.items.sort(byAuthorTitle));
      groups.sort((a, b) => {
        const ua = a.key === "uncategorized", ub = b.key === "uncategorized";
        if (ua !== ub) return ua ? 1 : -1;
        return b.items.length - a.items.length || a.key.localeCompare(b.key);
      });
    } else if (mode === "format") {
      groups.forEach((g) => g.items.sort(byAuthorTitle));
      groups.sort((a, b) => fmtRank(a.key) - fmtRank(b.key));
    } else {
      groups.forEach((g) => g.items.sort(byDateTitle));
      groups.sort((a, b) =>
        surnameOf(a.items[0]).localeCompare(surnameOf(b.items[0])) ||
        a.key.toLowerCase().localeCompare(b.key.toLowerCase()));
    }
    return groups;
  }

  /* ---------- book physical size ---------- */

  function sizeOf(b) {
    const pages = b.page_count || 448;
    const h = 0.215 + (hashStr(`${b.id}|h`) % 100) / 100 * 0.06;       // 21.5–27.5cm tall
    const th = Math.max(0.013, Math.min(0.052, 0.010 + pages * 0.00007));
    const w = h * 0.68;                                                 // cover width
    return { w, h, th };
  }

  /* ==========================================================================
     canvas textures — everything in the room is drawn, no image assets
     ========================================================================== */

  function canvas2d(w, h) {
    const c = document.createElement("canvas");
    c.width = w; c.height = h;
    return [c, c.getContext("2d")];
  }

  function noise(ctx, w, h, alpha, n = 900) {
    ctx.save();
    ctx.globalAlpha = alpha;
    for (let i = 0; i < n; i++) {
      ctx.fillStyle = Math.random() > 0.5 ? "#000" : "#fff";
      ctx.fillRect(Math.random() * w, Math.random() * h, 1, 1);
    }
    ctx.restore();
  }

  function srgbTex(c) {
    const t = new THREE.CanvasTexture(c);
    t.colorSpace = THREE.SRGBColorSpace;
    return t;
  }

  // vertical-grain walnut for the cases and wainscot
  function woodTexture(base = "#4a3320", dark = "#2f2013", light = "#5f452a") {
    const [c, ctx] = canvas2d(256, 512);
    const g = ctx.createLinearGradient(0, 0, 256, 0);
    g.addColorStop(0, dark); g.addColorStop(0.5, base); g.addColorStop(1, dark);
    ctx.fillStyle = g;
    ctx.fillRect(0, 0, 256, 512);
    for (let i = 0; i < 60; i++) {
      const x = Math.random() * 256, wl = 0.5 + Math.random() * 2;
      ctx.strokeStyle = Math.random() > 0.5 ? light : dark;
      ctx.globalAlpha = 0.05 + Math.random() * 0.12;
      ctx.lineWidth = wl;
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.bezierCurveTo(x + 8 * Math.sin(i), 170, x - 8 * Math.sin(i * 2), 340, x + 4 * Math.sin(i * 3), 512);
      ctx.stroke();
    }
    ctx.globalAlpha = 1;
    noise(ctx, 256, 512, 0.05);
    const t = srgbTex(c);
    t.wrapS = t.wrapT = THREE.RepeatWrapping;
    return t;
  }

  // concentric parquet rings for the rotunda floor
  function floorTexture() {
    const S = 1024;
    const [c, ctx] = canvas2d(S, S);
    ctx.fillStyle = "#1e150d";
    ctx.fillRect(0, 0, S, S);
    const cx = S / 2, cy = S / 2;
    for (let r = 30; r < S * 0.75; r += 26) {
      ctx.strokeStyle = `rgba(${48 + Math.random() * 22},${34 + Math.random() * 14},${20 + Math.random() * 9},${0.3 + Math.random() * 0.25})`;
      ctx.lineWidth = 22;
      ctx.beginPath();
      ctx.arc(cx, cy, r, 0, Math.PI * 2);
      ctx.stroke();
      ctx.strokeStyle = "rgba(10,7,4,0.28)";
      ctx.lineWidth = 1.4;
      ctx.beginPath();
      ctx.arc(cx, cy, r + 12, 0, Math.PI * 2);
      ctx.stroke();
    }
    // radial seams
    ctx.strokeStyle = "rgba(10,7,4,0.22)";
    ctx.lineWidth = 1.6;
    for (let a = 0; a < Math.PI * 2; a += Math.PI / 14) {
      ctx.beginPath();
      ctx.moveTo(cx + Math.cos(a) * 40, cy + Math.sin(a) * 40);
      ctx.lineTo(cx + Math.cos(a) * S, cy + Math.sin(a) * S);
      ctx.stroke();
    }
    // warm pool of candlelight in the middle, vignette at the rim
    const glow = ctx.createRadialGradient(cx, cy, 0, cx, cy, S * 0.55);
    glow.addColorStop(0, "rgba(255,186,102,0.10)");
    glow.addColorStop(0.45, "rgba(255,186,102,0.02)");
    glow.addColorStop(1, "rgba(0,0,0,0.55)");
    ctx.fillStyle = glow;
    ctx.fillRect(0, 0, S, S);
    noise(ctx, S, S, 0.04, 3000);
    const t = srgbTex(c);
    t.anisotropy = 8;
    return t;
  }

  // soft round sprite for dust motes, candle flames, halos
  function dustTexture() {
    const [c, ctx] = canvas2d(32, 32);
    const g = ctx.createRadialGradient(16, 16, 0, 16, 16, 16);
    g.addColorStop(0, "rgba(255,235,200,1)");
    g.addColorStop(0.4, "rgba(255,235,200,0.4)");
    g.addColorStop(1, "rgba(255,235,200,0)");
    ctx.fillStyle = g;
    ctx.fillRect(0, 0, 32, 32);
    return new THREE.CanvasTexture(c);
  }

  // brass label plaque for a group
  function plaqueTexture(text, n) {
    const [c, ctx] = canvas2d(256, 64);
    const g = ctx.createLinearGradient(0, 0, 0, 64);
    g.addColorStop(0, "#8a6d2e"); g.addColorStop(0.5, "#c9a84c"); g.addColorStop(1, "#7a5f26");
    ctx.fillStyle = g;
    ctx.fillRect(0, 0, 256, 64);
    ctx.strokeStyle = "rgba(60,42,10,0.9)";
    ctx.lineWidth = 4;
    ctx.strokeRect(5, 5, 246, 54);
    ctx.fillStyle = "#2b1f08";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    let label = text.toLowerCase() + (n ? ` · ${n}` : "");
    ctx.font = "600 26px Georgia, 'Times New Roman', serif";
    while (ctx.measureText(label).width > 230 && label.length > 4) label = label.slice(0, -2).trim() + "…";
    ctx.fillText(label, 128, 34);
    return srgbTex(c);
  }

  // the illusion of towering upper stacks: two shadowed shelf rows of
  // anonymous spines, tiled around the rotunda above the real cases
  function stacksTexture() {
    const W = 512, H = 256;
    const [c, ctx] = canvas2d(W, H);
    ctx.fillStyle = "#171009";
    ctx.fillRect(0, 0, W, H);
    const dyes = ["#4a2a2c", "#513322", "#24402f", "#3a4128", "#5d4a28", "#2b3a4e", "#233f41", "#3f2a40", "#332946", "#4c3a2b"];
    for (let row = 0; row < 2; row++) {
      const y0 = 12 + row * 128, bh = 96;
      // plank above the row
      ctx.fillStyle = "#241708";
      ctx.fillRect(0, y0 + bh, W, 14);
      ctx.fillStyle = "rgba(201,168,76,0.10)";
      ctx.fillRect(0, y0 + bh, W, 2);
      let x = 0;
      while (x < W) {
        const bw = 7 + Math.random() * 12;
        const h = bh * (0.72 + Math.random() * 0.26);
        const lean = Math.random() > 0.93 ? (Math.random() * 4 - 2) : 0;
        ctx.save();
        ctx.translate(x + bw / 2, y0 + bh);
        ctx.rotate(lean * 0.03);
        ctx.fillStyle = dyes[(Math.random() * dyes.length) | 0];
        ctx.fillRect(-bw / 2, -h, bw, h);
        ctx.fillStyle = "rgba(0,0,0,0.35)";
        ctx.fillRect(bw / 2 - 2, -h, 2, h);
        if (Math.random() > 0.4) {
          ctx.fillStyle = "rgba(216,180,100,0.5)";
          ctx.fillRect(-bw / 2 + 1, -h + 6, bw - 3, 1.5);
          ctx.fillRect(-bw / 2 + 1, -12, bw - 3, 1.5);
        }
        ctx.restore();
        x += bw + (Math.random() > 0.9 ? 6 : 0.5);
      }
    }
    // sink the whole band into shadow so it reads as distance, not wallpaper
    const dim = ctx.createLinearGradient(0, 0, 0, H);
    dim.addColorStop(0, "rgba(6,4,2,0.72)");
    dim.addColorStop(0.55, "rgba(6,4,2,0.45)");
    dim.addColorStop(1, "rgba(6,4,2,0.62)");
    ctx.fillStyle = dim;
    ctx.fillRect(0, 0, W, H);
    noise(ctx, W, H, 0.05);
    const t = srgbTex(c);
    t.wrapS = THREE.RepeatWrapping;
    return t;
  }

  // moonlit gothic window: pointed arch, dark tracery, cold blue glass
  function windowTexture() {
    const W = 256, H = 512;
    const [c, ctx] = canvas2d(W, H);
    ctx.fillStyle = "#0b0805";
    ctx.fillRect(0, 0, W, H);
    const arch = (inset) => {
      ctx.beginPath();
      ctx.moveTo(inset, H - inset);
      ctx.lineTo(inset, H * 0.36);
      ctx.quadraticCurveTo(inset, inset, W / 2, inset);
      ctx.quadraticCurveTo(W - inset, inset, W - inset, H * 0.36);
      ctx.lineTo(W - inset, H - inset);
      ctx.closePath();
    };
    // moonlight behind the glass
    arch(26);
    const sky = ctx.createRadialGradient(W * 0.62, H * 0.24, 10, W / 2, H * 0.45, H * 0.75);
    sky.addColorStop(0, "#cfdcf2");
    sky.addColorStop(0.14, "#7d97c4");
    sky.addColorStop(0.5, "#3c5378");
    sky.addColorStop(1, "#141e30");
    ctx.save();
    ctx.clip();
    ctx.fillStyle = sky;
    ctx.fillRect(0, 0, W, H);
    // the moon itself, and a wisp of cloud
    ctx.fillStyle = "rgba(235,242,252,0.95)";
    ctx.beginPath(); ctx.arc(W * 0.62, H * 0.22, 22, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = "rgba(20,30,48,0.55)";
    ctx.beginPath(); ctx.ellipse(W * 0.5, H * 0.3, 70, 10, 0.1, 0, Math.PI * 2); ctx.fill();
    // diamond panes
    ctx.strokeStyle = "rgba(8,6,4,0.85)";
    ctx.lineWidth = 2.5;
    for (let i = -8; i < 16; i++) {
      ctx.beginPath(); ctx.moveTo(i * 32, 0); ctx.lineTo(i * 32 + H, H); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(i * 32, 0); ctx.lineTo(i * 32 - H, H); ctx.stroke();
    }
    ctx.restore();
    // stone tracery: center mullion + arch ribs
    ctx.strokeStyle = "#241a10";
    ctx.lineWidth = 9;
    arch(26); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(W / 2, 26); ctx.lineTo(W / 2, H - 26); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(26, H * 0.42); ctx.lineTo(W - 26, H * 0.42); ctx.stroke();
    ctx.strokeStyle = "#3a2c1a";
    ctx.lineWidth = 3;
    arch(18); ctx.stroke();
    noise(ctx, W, H, 0.04);
    return srgbTex(c);
  }

  // oxblood rug with a double gilt border and a quiet medallion
  function rugTexture() {
    const S = 512;
    const [c, ctx] = canvas2d(S, S);
    const cx = S / 2, cy = S / 2;
    const base = ctx.createRadialGradient(cx, cy, 0, cx, cy, S / 2);
    base.addColorStop(0, "#4a1f22");
    base.addColorStop(0.7, "#3a181b");
    base.addColorStop(1, "#2a1013");
    ctx.fillStyle = base;
    ctx.beginPath(); ctx.arc(cx, cy, S / 2 - 2, 0, Math.PI * 2); ctx.fill();
    const ring = (r, w, col) => {
      ctx.strokeStyle = col; ctx.lineWidth = w;
      ctx.beginPath(); ctx.arc(cx, cy, r, 0, Math.PI * 2); ctx.stroke();
    };
    ring(S / 2 - 14, 4, "rgba(201,168,76,0.75)");
    ring(S / 2 - 26, 2, "rgba(201,168,76,0.45)");
    ring(S * 0.18, 3, "rgba(201,168,76,0.5)");
    ring(S * 0.13, 1.5, "rgba(201,168,76,0.35)");
    // laurel dashes between the border rings
    ctx.strokeStyle = "rgba(201,168,76,0.32)";
    ctx.lineWidth = 2;
    for (let a = 0; a < Math.PI * 2; a += Math.PI / 24) {
      ctx.beginPath();
      ctx.arc(cx, cy, S / 2 - 20, a, a + Math.PI / 60);
      ctx.stroke();
    }
    // center rose medallion, eight petals
    ctx.fillStyle = "rgba(201,168,76,0.28)";
    for (let a = 0; a < Math.PI * 2; a += Math.PI / 4) {
      ctx.beginPath();
      ctx.ellipse(cx + Math.cos(a) * S * 0.055, cy + Math.sin(a) * S * 0.055, S * 0.05, S * 0.02, a, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.fillStyle = "rgba(224,185,92,0.5)";
    ctx.beginPath(); ctx.arc(cx, cy, S * 0.018, 0, Math.PI * 2); ctx.fill();
    noise(ctx, S, S, 0.06, 2200);
    const t = srgbTex(c);
    t.anisotropy = 4;
    return t;
  }

  // a painted night sky for the dome: near-black with faint gilt stars
  function domeTexture() {
    const S = 512;
    const [c, ctx] = canvas2d(S, S);
    const g = ctx.createLinearGradient(0, 0, 0, S);
    g.addColorStop(0, "#100c16");
    g.addColorStop(1, "#0a0710");
    ctx.fillStyle = g;
    ctx.fillRect(0, 0, S, S);
    for (let i = 0; i < 160; i++) {
      const r = Math.random() < 0.9 ? 0.8 : 1.8;
      ctx.fillStyle = `rgba(224,190,110,${0.15 + Math.random() * 0.45})`;
      ctx.beginPath();
      ctx.arc(Math.random() * S, Math.random() * S, r, 0, Math.PI * 2);
      ctx.fill();
    }
    const t = srgbTex(c);
    t.wrapS = THREE.RepeatWrapping;
    return t;
  }

  /* ---------- per-book texture: cloth, pages, spine, cover ---------- */

  function wrapLines(ctx, text, maxW, maxLines) {
    const words = (text || "").split(/\s+/).filter(Boolean);
    const lines = [];
    let cur = "";
    for (const w of words) {
      const probe = cur ? `${cur} ${w}` : w;
      if (ctx.measureText(probe).width <= maxW || !cur) cur = probe;
      else { lines.push(cur); cur = w; }
      if (lines.length === maxLines) break;
    }
    if (lines.length < maxLines && cur) lines.push(cur);
    if (lines.length === maxLines && cur && lines[maxLines - 1] !== cur) {
      lines[maxLines - 1] = lines[maxLines - 1].replace(/.{2}$/, "…");
    }
    return lines;
  }

  function drawBookCanvas(ctx, b, cloth, coverImg) {
    const S = TEX_SIZE;
    const title = b.title || "untitled";
    const authors = authorsOf(b);

    // cloth strip — the body of the book
    const clothG = ctx.createLinearGradient(0, 0, 0, S);
    clothG.addColorStop(0, shade(cloth, 1.15));
    clothG.addColorStop(0.5, cloth);
    clothG.addColorStop(1, shade(cloth, 0.72));
    ctx.fillStyle = clothG;
    ctx.fillRect(0, 0, REG.cloth[1] + 2, S);

    // pages strip — cream block with fine ruling
    ctx.fillStyle = "#e6dcc3";
    ctx.fillRect(REG.pages[0] - 1, 0, REG.pages[1] - REG.pages[0] + 2, S);
    ctx.strokeStyle = "rgba(120,100,70,0.35)";
    ctx.lineWidth = 1;
    for (let y = 3; y < S; y += 3 + (y % 5)) {
      ctx.beginPath();
      ctx.moveTo(REG.pages[0], y);
      ctx.lineTo(REG.pages[1], y);
      ctx.stroke();
    }

    // spine — deep cloth, gilded bands, vertical title
    const sx = REG.spine[0], sw = REG.spine[1] - REG.spine[0];
    const spineG = ctx.createLinearGradient(sx, 0, sx + sw, 0);
    spineG.addColorStop(0, shade(cloth, 0.55));
    spineG.addColorStop(0.5, shade(cloth, 0.95));
    spineG.addColorStop(1, shade(cloth, 0.5));
    ctx.fillStyle = spineG;
    ctx.fillRect(sx, 0, sw, S);
    ctx.fillStyle = "rgba(227,179,65,0.85)";
    ctx.fillRect(sx + 6, 12, sw - 12, 3);
    ctx.fillRect(sx + 6, 22, sw - 12, 2);
    ctx.fillRect(sx + 6, S - 26, sw - 12, 2);
    ctx.fillRect(sx + 6, S - 17, sw - 12, 3);
    ctx.save();
    ctx.translate(sx + sw / 2, 34);
    ctx.rotate(Math.PI / 2);
    ctx.fillStyle = "#efdba2";
    ctx.font = "600 24px Georgia, 'Times New Roman', serif";
    ctx.textBaseline = "middle";
    let spineTitle = title;
    while (ctx.measureText(spineTitle).width > S - 74 && spineTitle.length > 3) spineTitle = spineTitle.slice(0, -2).trim() + "…";
    ctx.fillText(spineTitle, 0, 0);
    ctx.restore();

    // cover region: real cover if we have one, cloth-bound typography if not
    const cx0 = REG.cover[0], cw = REG.cover[1] - REG.cover[0];
    if (coverImg) {
      // cover-fit crop
      const ar = cw / S, iar = coverImg.width / coverImg.height;
      let sxi = 0, syi = 0, swi = coverImg.width, shi = coverImg.height;
      if (iar > ar) { swi = shi * ar; sxi = (coverImg.width - swi) / 2; }
      else { shi = swi / ar; syi = (coverImg.height - shi) / 2; }
      ctx.drawImage(coverImg, sxi, syi, swi, shi, cx0, 0, cw, S);
      // hint of binding shadow at the hinge
      const hinge = ctx.createLinearGradient(cx0, 0, cx0 + 14, 0);
      hinge.addColorStop(0, "rgba(0,0,0,0.45)");
      hinge.addColorStop(1, "rgba(0,0,0,0)");
      ctx.fillStyle = hinge;
      ctx.fillRect(cx0, 0, 14, S);
    } else {
      ctx.fillStyle = clothG;
      ctx.fillRect(cx0, 0, cw, S);
      ctx.strokeStyle = "rgba(227,179,65,0.75)";
      ctx.lineWidth = 2;
      ctx.strokeRect(cx0 + 10, 10, cw - 20, S - 20);
      ctx.strokeRect(cx0 + 15, 15, cw - 30, S - 30);
      ctx.fillStyle = "#efdba2";
      ctx.textAlign = "center";
      ctx.font = "600 20px Georgia, 'Times New Roman', serif";
      const lines = wrapLines(ctx, title, cw - 44, 4);
      lines.forEach((ln, i) => ctx.fillText(ln, cx0 + cw / 2, 72 + i * 26));
      ctx.fillStyle = "rgba(227,179,65,0.75)";
      ctx.fillRect(cx0 + cw / 2 - 22, 178, 44, 2);
      ctx.fillStyle = "#d9c894";
      ctx.font = "15px Georgia, 'Times New Roman', serif";
      wrapLines(ctx, authors, cw - 44, 2).forEach((ln, i) => ctx.fillText(ln, cx0 + cw / 2, 202 + i * 20));
      ctx.textAlign = "left";
    }
    noise(ctx, S, S, 0.03, 500);
  }

  // average color of a loaded cover — used to re-dye the cloth and spine
  function avgColor(img) {
    const [c, ctx] = canvas2d(4, 4);
    ctx.drawImage(img, 0, 0, 4, 4);
    const d = ctx.getImageData(0, 0, 4, 4).data;
    let r = 0, g = 0, b = 0;
    for (let i = 0; i < d.length; i += 4) { r += d[i]; g += d[i + 1]; b += d[i + 2]; }
    const n = d.length / 4;
    r = Math.round(r / n); g = Math.round(g / n); b = Math.round(b / n);
    // keep it deep enough to read as book cloth
    const scale = Math.min(1, 150 / Math.max(1, (r + g + b) / 3));
    const hx = (v) => Math.round(v * scale).toString(16).padStart(2, "0");
    return `#${hx(r)}${hx(g)}${hx(b)}`;
  }

  /* ---------- box geometry with per-region UVs ---------- */

  // face order in BoxGeometry uvs: +x, -x, +y, -y, +z, -z (4 corners each).
  // book local space: x = cover width, y = height, z = thickness;
  // +z is the front cover, -x is the spine.
  function bookGeometry() {
    const geo = new THREE.BoxGeometry(1, 1, 1);
    const uv = geo.attributes.uv;
    const S = TEX_SIZE;
    const remap = (face, [a, b]) => {
      const u0 = (a + 1.5) / S, u1 = (b - 1.5) / S;
      for (let i = face * 4; i < face * 4 + 4; i++) {
        uv.setX(i, u0 + uv.getX(i) * (u1 - u0));
      }
    };
    remap(0, REG.pages);  // +x fore-edge
    remap(1, REG.spine);  // -x spine
    remap(2, REG.pages);  // +y top
    remap(3, REG.cloth);  // -y bottom
    remap(4, REG.cover);  // +z front cover
    remap(5, REG.cloth);  // -z back cover
    uv.needsUpdate = true;
    return geo;
  }

  /* ==========================================================================
     the world
     ========================================================================== */

  let renderer = null;
  let scene, camera, raycaster;
  let running = false;
  let books = null;            // cached library items
  let mode = "genre";
  let loading = false;
  let enriching = false;
  let enrichDone = false;
  let firstBuild = true;
  let shadowDirty = true;
  let radius = 3.2;            // current arc radius (updated per layout)
  let arcHalf = Math.PI * 0.8; // half-angle actually covered by bookcases
  let dust = null;
  let dustSeed = null;
  let caseRoot = null;         // rebuilt on every layout
  let hovered = null;
  let presenting = null;
  let introPlayed = false;
  let perfChecked = false;
  let frameTimes = [];

  const bookNodes = new Map(); // edition id -> Book record
  const sharedGeo = bookGeometry();
  const tmpV = new THREE.Vector3();
  const tmpV2 = new THREE.Vector3();
  const tmpQ = new THREE.Quaternion();
  const tmpE = new THREE.Euler(0, 0, 0, "YXZ");

  /* ---------- first-person player ---------- */

  const player = {
    pos: new THREE.Vector3(0, 0, 2.2),  // feet, on the floor
    vel: new THREE.Vector3(),
    yaw: 0,            // 0 faces -z, toward the stacks
    pitch: 0,
    flyY: 0,           // extra height during the cinematic intro
    bobT: 0,
  };
  const keys = new Set();
  let pointerLocked = false;

  // move stick (touch): -1..1 on each axis, driven by the left thumb
  const stick = { id: null, ox: 0, oy: 0, x: 0, y: 0, moved: 0, t0: 0 };
  // look drag (touch): driven by the right thumb
  const look = { id: null, x: 0, y: 0, moved: 0, t0: 0 };

  // solid things you can't walk through: {x, z, r}
  let colliders = [];
  let bayEndColliders = [];

  function collidePlayer(p) {
    // circular push-outs: pedestal, candelabras, the flanks of the end bays
    for (const c of [...colliders, ...bayEndColliders]) {
      const dx = p.x - c.x, dz = p.z - c.z;
      const d = Math.hypot(dx, dz);
      if (d < c.r && d > 1e-4) {
        p.x = c.x + (dx / d) * c.r;
        p.z = c.z + (dz / d) * c.r;
      }
    }
    // the shelf ring ahead, the rotunda wall through the opening behind
    const r = Math.hypot(p.x, p.z);
    if (r > 1e-4) {
      const a = Math.atan2(p.x, -p.z);   // matches bay placement angles
      const maxR = Math.abs(a) < arcHalf + 0.10 ? radius - 0.55 : roomWallR - 0.6;
      if (r > maxR) { p.x *= maxR / r; p.z *= maxR / r; }
    }
  }

  const forwardOf = (yaw) => tmpV.set(-Math.sin(yaw), 0, -Math.cos(yaw));

  function applyCamera(dt) {
    // desired walk direction in the ground plane (frozen while a book is
    // held up in front of you, so it stays put over the sheet)
    let mx = 0, mz = 0;
    if (!presenting) {
      if (keys.has("KeyW") || keys.has("ArrowUp")) mz += 1;
      if (keys.has("KeyS") || keys.has("ArrowDown")) mz -= 1;
      if (keys.has("KeyA") || keys.has("ArrowLeft")) mx -= 1;
      if (keys.has("KeyD") || keys.has("ArrowRight")) mx += 1;
      mx += stick.x; mz -= stick.y;
    }
    const mag = Math.hypot(mx, mz);
    if (mag > 1) { mx /= mag; mz /= mag; }

    const sprint = keys.has("ShiftLeft") || keys.has("ShiftRight");
    const speed = sprint ? 3.1 : 1.6;

    const sin = Math.sin(player.yaw), cos = Math.cos(player.yaw);
    // forward = (-sin, -cos), right = (cos, -sin) in the xz plane
    const wishX = (-sin * mz + cos * mx) * speed;
    const wishZ = (-cos * mz - sin * mx) * speed;

    const k = 1 - Math.exp(-dt * 9);           // smooth start/stop
    player.vel.x += (wishX - player.vel.x) * k;
    player.vel.z += (wishZ - player.vel.z) * k;

    player.pos.x += player.vel.x * dt;
    player.pos.z += player.vel.z * dt;
    collidePlayer(player.pos);

    // gentle head-bob scaled by how fast you're actually moving
    const v = Math.hypot(player.vel.x, player.vel.z);
    player.bobT += dt * (4.6 + v * 2.2);
    const bobAmt = reducedMotion() ? 0 : Math.min(1, v / 1.6) * 0.026;
    const bobY = Math.sin(player.bobT * 2) * bobAmt;
    const bobR = Math.sin(player.bobT) * bobAmt * 0.4;

    camera.position.set(player.pos.x, EYE + player.flyY + bobY, player.pos.z);
    tmpE.set(player.pitch, player.yaw, bobR);
    camera.quaternion.setFromEuler(tmpE);
  }

  /* ---------- input: pointer lock + keys on desktop, thumbs on touch ---------- */

  function setHint(text) {
    if (!hintEl) return;
    hintEl.textContent = text;
    hintEl.classList.remove("gone");
    clearTimeout(setHint.timer);
    setHint.timer = setTimeout(() => hintEl.classList.add("gone"), 6000);
  }

  function bindControls(el) {
    // --- desktop: click to enter, mouse to look, wasd to walk ---
    el.addEventListener("click", (e) => {
      if (coarsePointer()) return;
      if (!pointerLocked) {
        el.requestPointerLock?.();
        return;
      }
      const rec = pick(null);   // whatever the crosshair rests on
      if (rec) present(rec);
    });

    document.addEventListener("pointerlockchange", () => {
      pointerLocked = document.pointerLockElement === el;
      worldEl.classList.toggle("walking", pointerLocked);
      if (pointerLocked) setHint("wasd to walk · shift to hurry · click a book · esc to step out");
      else {
        setHover(null);
        if (viewVisible()) setHint("click to step inside");
      }
    });

    document.addEventListener("mousemove", (e) => {
      if (!pointerLocked) return;
      player.yaw -= e.movementX * 0.0023;
      player.pitch = Math.max(-1.35, Math.min(1.35, player.pitch - e.movementY * 0.0021));
    });

    window.addEventListener("keydown", (e) => {
      if (!running || !viewVisible()) return;
      if (e.target && /^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName)) return;
      if (/^(KeyW|KeyA|KeyS|KeyD|ArrowUp|ArrowDown|ArrowLeft|ArrowRight|ShiftLeft|ShiftRight)$/.test(e.code)) {
        keys.add(e.code);
        if (e.code.startsWith("Arrow")) e.preventDefault();
      }
    });
    window.addEventListener("keyup", (e) => keys.delete(e.code));
    window.addEventListener("blur", () => keys.clear());

    // --- touch: left thumb walks, right thumb looks, tap to reach ---
    el.addEventListener("pointerdown", (e) => {
      if (e.pointerType !== "touch") return;
      const r = el.getBoundingClientRect();
      const x = e.clientX - r.left, y = e.clientY - r.top;
      if (x < r.width / 2 && stick.id === null) {
        stick.id = e.pointerId;
        stick.ox = e.clientX; stick.oy = e.clientY;
        stick.x = 0; stick.y = 0;
        stick.moved = 0; stick.t0 = performance.now();
        if (stickEl) {
          stickEl.style.left = `${x}px`;
          stickEl.style.top = `${y}px`;
          stickEl.classList.add("live");
        }
      } else if (look.id === null) {
        look.id = e.pointerId;
        look.x = e.clientX; look.y = e.clientY;
        look.moved = 0; look.t0 = performance.now();
      }
      try { el.setPointerCapture(e.pointerId); } catch { /* synthetic events have no capture */ }
      hintEl?.classList.add("gone");
    });

    el.addEventListener("pointermove", (e) => {
      if (e.pointerType !== "touch") return;
      if (e.pointerId === stick.id) {
        const R = 46;
        let dx = e.clientX - stick.ox, dy = e.clientY - stick.oy;
        const d = Math.hypot(dx, dy);
        stick.moved = Math.max(stick.moved, d);
        if (d > R) { dx *= R / d; dy *= R / d; }
        stick.x = dx / R; stick.y = dy / R;
        if (stickNub) stickNub.style.transform = `translate(${dx}px, ${dy}px)`;
      } else if (e.pointerId === look.id) {
        const dx = e.clientX - look.x, dy = e.clientY - look.y;
        look.moved += Math.abs(dx) + Math.abs(dy);
        look.x = e.clientX; look.y = e.clientY;
        player.yaw -= dx * 0.005;
        player.pitch = Math.max(-1.35, Math.min(1.35, player.pitch - dy * 0.004));
      }
    });

    const touchUp = (e) => {
      if (e.pointerId === stick.id) {
        const quick = performance.now() - stick.t0 < 350 && stick.moved < 12;
        stick.id = null; stick.x = 0; stick.y = 0;
        stickEl?.classList.remove("live");
        if (stickNub) stickNub.style.transform = "";
        if (quick) {
          // a motionless dab on the left half is a tap, not a walk
          const rec = pick(e);
          if (rec) present(rec);
        }
      } else if (e.pointerId === look.id) {
        const quick = performance.now() - look.t0 < 350 && look.moved < 12;
        look.id = null;
        if (quick) {
          const rec = pick(e);
          if (rec) present(rec);
        }
      }
    };
    el.addEventListener("pointerup", touchUp);
    el.addEventListener("pointercancel", touchUp);

    el.addEventListener("wheel", (e) => e.preventDefault(), { passive: false });
  }

  /* ---------- room ---------- */

  let roomRoot = null;
  let roomWallR = 0;
  let flames = [];        // {obj, seed, base} — candle sprites that flicker
  let chandLight = null;
  let roseGroup = null;
  let roseSpin = null;
  let sparkles = null;
  let sparkleSeed = null;

  const goldMat = () => new THREE.MeshStandardMaterial({ color: 0x8a6a2c, roughness: 0.35, metalness: 0.85 });

  function candleFlame(scale = 1) {
    const m = new THREE.Sprite(new THREE.SpriteMaterial({
      map: assets.dustTex, color: 0xffbe6a, transparent: true, opacity: 0.95,
      blending: THREE.AdditiveBlending, depthWrite: false,
    }));
    m.scale.setScalar(0.075 * scale);
    flames.push({ obj: m, seed: Math.random() * Math.PI * 2, base: 0.075 * scale });
    return m;
  }

  // a floor-standing candelabra — the Lumière nod by the windows
  function buildCandelabra() {
    const g = new THREE.Group();
    const gold = goldMat();
    const stem = new THREE.Mesh(new THREE.CylinderGeometry(0.02, 0.05, 1.25, 8), gold);
    stem.position.y = 0.625;
    g.add(stem);
    const foot = new THREE.Mesh(new THREE.CylinderGeometry(0.14, 0.17, 0.05, 12), gold);
    foot.position.y = 0.025;
    g.add(foot);
    const candleMat = new THREE.MeshStandardMaterial({ color: 0xe8dcbe, roughness: 0.6 });
    for (let i = -1; i <= 1; i++) {
      const arm = new THREE.Mesh(new THREE.CylinderGeometry(0.012, 0.012, 0.3, 6), gold);
      arm.rotation.z = Math.PI / 2;
      arm.position.set(i * 0.14, 1.28, 0);
      if (i !== 0) g.add(arm);
      const cup = new THREE.Mesh(new THREE.CylinderGeometry(0.035, 0.02, 0.03, 8), gold);
      cup.position.set(i * 0.28, 1.3 + (i === 0 ? 0.08 : 0), 0);
      g.add(cup);
      const candle = new THREE.Mesh(new THREE.CylinderGeometry(0.016, 0.016, 0.14, 8), candleMat);
      candle.position.set(i * 0.28, 1.39 + (i === 0 ? 0.08 : 0), 0);
      g.add(candle);
      const fl = candleFlame(0.9);
      fl.position.set(i * 0.28, 1.5 + (i === 0 ? 0.08 : 0), 0);
      g.add(fl);
    }
    return g;
  }

  // the candle chandelier over the middle of the hall
  function buildChandelier(y, oculusY) {
    const g = new THREE.Group();
    const gold = goldMat();
    const chain = new THREE.Mesh(new THREE.CylinderGeometry(0.012, 0.012, oculusY - y, 6), gold);
    chain.position.y = (oculusY - y) / 2;
    g.add(chain);
    const hub = new THREE.Mesh(new THREE.SphereGeometry(0.09, 12, 8), gold);
    g.add(hub);
    const candleMat = new THREE.MeshStandardMaterial({ color: 0xe8dcbe, roughness: 0.6 });
    const rings = [
      { r: 0.85, n: 12, dy: 0 },
      { r: 0.45, n: 6, dy: 0.22 },
    ];
    for (const ring of rings) {
      const torus = new THREE.Mesh(new THREE.TorusGeometry(ring.r, 0.02, 8, 40), gold);
      torus.rotation.x = Math.PI / 2;
      torus.position.y = ring.dy;
      g.add(torus);
      for (let i = 0; i < ring.n; i++) {
        const a = (i / ring.n) * Math.PI * 2;
        const px = Math.cos(a) * ring.r, pz = Math.sin(a) * ring.r;
        const candle = new THREE.Mesh(new THREE.CylinderGeometry(0.018, 0.018, 0.15, 6), candleMat);
        candle.position.set(px, ring.dy + 0.095, pz);
        g.add(candle);
        const fl = candleFlame();
        fl.position.set(px, ring.dy + 0.215, pz);
        g.add(fl);
      }
      // spokes to the hub
      for (let i = 0; i < 4; i++) {
        const a = (i / 4) * Math.PI * 2 + 0.4;
        const spoke = new THREE.Mesh(new THREE.CylinderGeometry(0.008, 0.008, ring.r, 5), gold);
        spoke.rotation.z = Math.PI / 2;
        spoke.rotation.y = -a;
        spoke.position.set(Math.cos(a) * ring.r / 2, ring.dy, Math.sin(a) * ring.r / 2);
        g.add(spoke);
      }
    }
    chandLight = new THREE.PointLight(0xffb168, 16, 15, 1.7);
    chandLight.position.y = 0.1;
    g.add(chandLight);
    g.position.y = y;
    return g;
  }

  // the enchanted rose under glass, mid-hall on a marble pedestal
  function buildRose() {
    const g = new THREE.Group();

    const marble = new THREE.MeshStandardMaterial({ color: 0x35303c, roughness: 0.35, metalness: 0.1 });
    const column = new THREE.Mesh(new THREE.CylinderGeometry(0.16, 0.2, 0.92, 16), marble);
    column.position.y = 0.46;
    column.castShadow = true;
    g.add(column);
    const plate = new THREE.Mesh(new THREE.CylinderGeometry(0.26, 0.26, 0.035, 20), goldMat());
    plate.position.y = 0.94;
    g.add(plate);

    const rose = new THREE.Group();
    const stem = new THREE.Mesh(
      new THREE.CylinderGeometry(0.006, 0.008, 0.26, 5),
      new THREE.MeshStandardMaterial({ color: 0x2c4428, roughness: 0.8 }),
    );
    stem.position.y = 0.13;
    rose.add(stem);
    const leaf = new THREE.Mesh(
      new THREE.SphereGeometry(0.03, 6, 4),
      new THREE.MeshStandardMaterial({ color: 0x2c4428, roughness: 0.8 }),
    );
    leaf.scale.set(1, 0.25, 0.5);
    leaf.position.set(0.03, 0.12, 0);
    rose.add(leaf);
    const bloomMat = new THREE.MeshStandardMaterial({
      color: 0x8e1626, roughness: 0.5, emissive: 0x400d16, emissiveIntensity: 0.55,
    });
    const bloom = new THREE.Mesh(new THREE.IcosahedronGeometry(0.036, 1), bloomMat);
    bloom.scale.set(1, 1.3, 1);
    bloom.position.y = 0.285;
    rose.add(bloom);
    // outer petals: flattened spheres skirting the bloom
    for (let i = 0; i < 5; i++) {
      const a = (i / 5) * Math.PI * 2;
      const petal = new THREE.Mesh(new THREE.SphereGeometry(0.02, 6, 4), bloomMat);
      petal.scale.set(1, 0.45, 0.7);
      petal.position.set(Math.cos(a) * 0.026, 0.262, Math.sin(a) * 0.026);
      petal.rotation.y = -a;
      petal.rotation.z = 0.5;
      rose.add(petal);
    }
    // the rose floats — an enchantment, after all
    rose.position.y = 1.02;
    g.add(rose);
    roseSpin = rose;

    // fallen petals on the plate
    const petalMat = new THREE.MeshStandardMaterial({ color: 0x7c1626, roughness: 0.6, side: THREE.DoubleSide });
    for (let i = 0; i < 3; i++) {
      const p = new THREE.Mesh(new THREE.CircleGeometry(0.018, 6), petalMat);
      p.rotation.x = -Math.PI / 2 + 0.15 * (i - 1);
      p.rotation.z = i * 2.1;
      p.position.set(Math.cos(i * 2.4) * 0.12, 0.962, Math.sin(i * 2.4) * 0.12);
      g.add(p);
    }

    // glass cloche: a faint shell plus an additive rim so it catches the candles
    const dome = new THREE.Mesh(
      new THREE.SphereGeometry(0.22, 20, 14, 0, Math.PI * 2, 0, Math.PI / 2),
      new THREE.MeshPhongMaterial({
        color: 0xbfd0e0, transparent: true, opacity: 0.13, shininess: 90, specular: 0x99aabb,
      }),
    );
    dome.scale.y = 1.9;
    dome.position.y = 0.958;
    g.add(dome);
    const rim = new THREE.Mesh(
      new THREE.SphereGeometry(0.222, 20, 14, 0, Math.PI * 2, 0, Math.PI / 2),
      new THREE.MeshBasicMaterial({
        color: 0x8fb0d8, transparent: true, opacity: 0.05,
        blending: THREE.AdditiveBlending, depthWrite: false, side: THREE.BackSide,
      }),
    );
    rim.scale.y = 1.9;
    rim.position.y = 0.958;
    g.add(rim);

    // rose-light spills out of the glass
    const roseLight = new THREE.PointLight(0xff5f78, 1.4, 3.5, 1.9);
    roseLight.position.y = 1.35;
    g.add(roseLight);

    // enchanted motes rising inside the cloche
    const N = 36;
    const pos = new Float32Array(N * 3);
    sparkleSeed = new Float32Array(N);
    for (let i = 0; i < N; i++) {
      const r = Math.random() * 0.16, a = Math.random() * Math.PI * 2;
      pos[i * 3] = Math.cos(a) * r;
      pos[i * 3 + 1] = 0.98 + Math.random() * 0.36;
      pos[i * 3 + 2] = Math.sin(a) * r;
      sparkleSeed[i] = Math.random() * Math.PI * 2;
    }
    const sg = new THREE.BufferGeometry();
    sg.setAttribute("position", new THREE.BufferAttribute(pos, 3));
    sparkles = new THREE.Points(sg, new THREE.PointsMaterial({
      map: assets.dustTex, color: 0xff9eae, size: 0.014, transparent: true, opacity: 0.85,
      blending: THREE.AdditiveBlending, depthWrite: false, sizeAttenuation: true,
    }));
    g.add(sparkles);

    return g;
  }

  // the shell (floor, walls, upper stacks, dome, windows, chandelier, rose)
  // is sized to the bookcase ring and rebuilt only when a regroup
  // meaningfully changes the arc radius
  function buildShell(wallR) {
    if (Math.abs(wallR - roomWallR) < 0.6 && roomRoot) return;
    roomWallR = wallR;
    if (roomRoot) {
      scene.remove(roomRoot);
      roomRoot.traverse((o) => { if (o.isMesh) o.geometry.dispose(); });
    }
    flames = [];
    roomRoot = new THREE.Group();
    colliders = [{ x: 0, z: 0, r: 0.75 }];   // the rose pedestal
    const ceilH = Math.max(5.4, wallR * 1.1);
    const galleryY = Math.min(3.2, ceilH * 0.58);

    const floor = new THREE.Mesh(
      new THREE.CircleGeometry(wallR + 0.5, 64),
      new THREE.MeshStandardMaterial({ map: assets.floorTex, roughness: 0.55, metalness: 0.04 }),
    );
    floor.rotation.x = -Math.PI / 2;
    floor.receiveShadow = true;
    roomRoot.add(floor);

    // the great rug under the rose
    const rug = new THREE.Mesh(
      new THREE.CircleGeometry(1.9, 48),
      new THREE.MeshStandardMaterial({ map: assets.rugTex, roughness: 0.92 }),
    );
    rug.rotation.x = -Math.PI / 2;
    rug.position.y = 0.006;
    rug.receiveShadow = true;
    roomRoot.add(rug);

    // walls: oxblood plaster over a walnut wainscot, gilt rails between
    const wainH = 1.15;
    const wain = new THREE.Mesh(
      new THREE.CylinderGeometry(wallR, wallR, wainH, 48, 1, true),
      new THREE.MeshStandardMaterial({ map: assets.wood, roughness: 0.8, side: THREE.BackSide }),
    );
    wain.position.y = wainH / 2;
    roomRoot.add(wain);

    const wall = new THREE.Mesh(
      new THREE.CylinderGeometry(wallR, wallR, ceilH - wainH, 48, 1, true),
      new THREE.MeshStandardMaterial({ color: 0x2a1a15, roughness: 0.95, side: THREE.BackSide }),
    );
    wall.position.y = wainH + (ceilH - wainH) / 2;
    roomRoot.add(wall);

    // the illusion of endless upper stacks wrapping the rotunda
    const stacksTex = assets.stacksTex.clone();
    stacksTex.repeat.set(Math.max(6, Math.round(wallR * 1.6)), 1);
    stacksTex.needsUpdate = true;
    const stackH = Math.max(1.8, ceilH - 2.3);
    const stacks = new THREE.Mesh(
      new THREE.CylinderGeometry(wallR - 0.04, wallR - 0.04, stackH, 48, 1, true),
      new THREE.MeshStandardMaterial({ map: stacksTex, roughness: 0.95, side: THREE.BackSide }),
    );
    stacks.position.y = 1.9 + stackH / 2;
    roomRoot.add(stacks);

    // gallery ledge + brass railing girdling the upper stacks
    const ledge = new THREE.Mesh(
      new THREE.CylinderGeometry(wallR - 0.02, wallR - 0.02, 0.09, 48, 1, true),
      new THREE.MeshStandardMaterial({ map: assets.wood, roughness: 0.8, side: THREE.DoubleSide }),
    );
    ledge.position.y = galleryY;
    roomRoot.add(ledge);
    const rail = new THREE.Mesh(new THREE.TorusGeometry(wallR - 0.12, 0.018, 6, 64), goldMat());
    rail.rotation.x = Math.PI / 2;
    rail.position.y = galleryY + 0.32;
    roomRoot.add(rail);
    const railLow = new THREE.Mesh(new THREE.TorusGeometry(wallR - 0.12, 0.01, 5, 64), goldMat());
    railLow.rotation.x = Math.PI / 2;
    railLow.position.y = galleryY + 0.14;
    roomRoot.add(railLow);
    // wainscot cap rail
    const cap = new THREE.Mesh(new THREE.TorusGeometry(wallR - 0.02, 0.014, 5, 64), goldMat());
    cap.rotation.x = Math.PI / 2;
    cap.position.y = wainH;
    roomRoot.add(cap);

    // starred dome with the oculus above
    const dome = new THREE.Mesh(
      new THREE.SphereGeometry(wallR, 40, 18, 0, Math.PI * 2, 0, Math.PI / 2),
      new THREE.MeshStandardMaterial({ map: assets.domeTex, roughness: 1, side: THREE.BackSide }),
    );
    dome.position.y = ceilH;
    dome.scale.y = 0.55;
    roomRoot.add(dome);

    const oculusY = ceilH + wallR * 0.5;
    const ring = new THREE.Mesh(
      new THREE.TorusGeometry(0.95, 0.08, 12, 48),
      new THREE.MeshBasicMaterial({ color: 0xdfe8f8 }),
    );
    ring.rotation.x = Math.PI / 2;
    ring.position.y = oculusY;
    roomRoot.add(ring);

    // moonlight shaft through the oculus
    const shaft = (r0, r1, op) => {
      const m = new THREE.Mesh(
        new THREE.CylinderGeometry(r0, r1, oculusY, 32, 1, true),
        new THREE.MeshBasicMaterial({
          color: 0xcdd9ee, transparent: true, opacity: op,
          blending: THREE.AdditiveBlending, depthWrite: false, side: THREE.DoubleSide,
        }),
      );
      m.position.y = oculusY / 2;
      roomRoot.add(m);
    };
    shaft(0.9, 1.55, 0.02);
    shaft(0.55, 1.0, 0.028);

    // moonlit gothic windows in the opening behind you
    const winGeo = new THREE.PlaneGeometry(1.15, 2.3);
    const winMat = new THREE.MeshBasicMaterial({ map: assets.windowTex });
    for (const off of [-0.42, 0, 0.42]) {
      const a = Math.PI + off;
      const px = Math.sin(a) * (wallR - 0.08), pz = -Math.cos(a) * (wallR - 0.08);
      const w = new THREE.Mesh(winGeo, winMat);
      w.position.set(px, 1.85, pz);
      w.lookAt(0, 1.85, 0);
      roomRoot.add(w);
      // cold spill on the floor under each window
      const spill = new THREE.Mesh(
        new THREE.PlaneGeometry(1.5, 2.4),
        new THREE.MeshBasicMaterial({
          map: assets.dustTex, color: 0x4d648c, transparent: true, opacity: 0.14,
          blending: THREE.AdditiveBlending, depthWrite: false,
        }),
      );
      spill.rotation.x = -Math.PI / 2;
      spill.position.set(px * 0.82, 0.012, pz * 0.82);
      roomRoot.add(spill);
    }

    // floor candelabras flanking the windows
    for (const off of [-0.24, 0.24]) {
      const a = Math.PI + off;
      const px = Math.sin(a) * (wallR - 1.0), pz = -Math.cos(a) * (wallR - 1.0);
      const cd = buildCandelabra();
      cd.position.set(px, 0, pz);
      cd.rotation.y = -a;
      roomRoot.add(cd);
      colliders.push({ x: px, z: pz, r: 0.42 });
    }

    // wall sconces: candle slivers with a soft halo, purely decorative
    const sconceMat = new THREE.MeshBasicMaterial({ color: 0xffd9a0 });
    const haloMat = new THREE.MeshBasicMaterial({
      map: assets.dustTex, color: 0xff9d55, transparent: true, opacity: 0.5,
      blending: THREE.AdditiveBlending, depthWrite: false,
    });
    const nS = 10;
    for (let i = 0; i < nS; i++) {
      const a = (i / nS) * Math.PI * 2 + Math.PI / nS;
      const px = Math.sin(a) * (wallR - 0.06), pz = -Math.cos(a) * (wallR - 0.06);
      const sconce = new THREE.Mesh(new THREE.PlaneGeometry(0.05, 0.42), sconceMat);
      sconce.position.set(px, 2.6, pz);
      sconce.lookAt(0, 2.6, 0);
      roomRoot.add(sconce);
      const halo = new THREE.Mesh(new THREE.PlaneGeometry(1.1, 1.1), haloMat);
      halo.position.set(px * 0.985, 2.6, pz * 0.985);
      halo.lookAt(0, 2.6, 0);
      roomRoot.add(halo);
    }

    roomRoot.add(buildChandelier(Math.min(3.5, ceilH - 1.6), oculusY));
    roseGroup = buildRose();
    roomRoot.add(roseGroup);

    scene.add(roomRoot);
    shadowDirty = true;
  }

  function buildRoom() {
    const wood = woodTexture();
    const floorTex = floorTexture();
    const dustTex = dustTexture();

    // lights: cool moonlight from above, warm candlelight in the room
    scene.add(new THREE.HemisphereLight(0x8091ad, 0x2a1c10, 0.7));

    const moon = new THREE.DirectionalLight(0xcfdaf2, 1.7);
    moon.position.set(1.2, 7.2, 0.6);
    moon.castShadow = true;
    moon.shadow.mapSize.set(1024, 1024);
    moon.shadow.camera.left = moon.shadow.camera.bottom = -8;
    moon.shadow.camera.right = moon.shadow.camera.top = 8;
    moon.shadow.camera.far = 12;
    moon.shadow.bias = -0.0005;
    scene.add(moon);

    const ember = new THREE.PointLight(0xff9d55, 5, 11, 1.9);
    ember.position.set(-2.6, 2.3, 2.2);
    scene.add(ember);

    // drifting dust in the moonlight
    const N = 420;
    const pos = new Float32Array(N * 3);
    dustSeed = new Float32Array(N);
    for (let i = 0; i < N; i++) {
      const r = Math.sqrt(Math.random()) * 2.0;
      const a = Math.random() * Math.PI * 2;
      pos[i * 3] = Math.cos(a) * r;
      pos[i * 3 + 1] = Math.random() * 6.8;
      pos[i * 3 + 2] = Math.sin(a) * r;
      dustSeed[i] = Math.random() * Math.PI * 2;
    }
    const dustGeo = new THREE.BufferGeometry();
    dustGeo.setAttribute("position", new THREE.BufferAttribute(pos, 3));
    dust = new THREE.Points(dustGeo, new THREE.PointsMaterial({
      map: dustTex, size: 0.022, transparent: true, opacity: 0.5,
      blending: THREE.AdditiveBlending, depthWrite: false, sizeAttenuation: true,
    }));
    scene.add(dust);

    return {
      wood, floorTex, dustTex,
      rugTex: rugTexture(),
      stacksTex: stacksTexture(),
      windowTex: windowTexture(),
      domeTex: domeTexture(),
    };
  }

  function tickAmbience(t, dt) {
    // dust drifting down the moonlight
    if (dust) {
      const pos = dust.geometry.attributes.position;
      for (let i = 0; i < pos.count; i++) {
        let y = pos.getY(i) - 0.0011;
        if (y < 0.04) y = 6.8;
        pos.setY(i, y);
        pos.setX(i, pos.getX(i) + Math.sin(t * 0.0004 + dustSeed[i]) * 0.0006);
      }
      pos.needsUpdate = true;
    }
    // candle flicker
    const ts = t * 0.001;
    for (const f of flames) {
      const n = Math.sin(ts * 9 + f.seed) * 0.5 + Math.sin(ts * 23 + f.seed * 2) * 0.5;
      f.obj.scale.setScalar(f.base * (1 + n * 0.16));
      f.obj.material.opacity = 0.8 + n * 0.15;
    }
    if (chandLight) chandLight.intensity = 16 * (1 + Math.sin(ts * 11) * 0.035 + Math.sin(ts * 27) * 0.025);
    // the rose turns slowly under its glass
    if (roseSpin && !reducedMotion()) {
      roseSpin.rotation.y += dt * 0.25;
      roseSpin.position.y = 1.02 + Math.sin(ts * 0.9) * 0.012;
    }
    // enchanted motes spiralling up inside the cloche
    if (sparkles) {
      const pos = sparkles.geometry.attributes.position;
      for (let i = 0; i < pos.count; i++) {
        let y = pos.getY(i) + 0.0009;
        if (y > 1.36) y = 0.98;
        pos.setY(i, y);
        const a = ts * 0.5 + sparkleSeed[i];
        const r = 0.05 + (sparkleSeed[i] % 1) * 0.12;
        pos.setX(i, Math.cos(a + i) * r);
        pos.setZ(i, Math.sin(a + i) * r);
      }
      pos.needsUpdate = true;
    }
  }

  /* ---------- layout: groups -> shelf rows -> bays on an arc ---------- */

  // pack books into rows that fit the bay run. genre/format modes give each
  // group its own shelf run (with a face-out staff pick and a plaque);
  // author mode is one continuous alphabetical run — no shelf-per-author —
  // with each row labelled by the surname range it spans
  function packRows(groups) {
    const rows = [];

    if (mode === "author") {
      let row = null;
      for (const g of groups) {
        for (const b of g.items) {
          const s = sizeOf(b);
          const runW = s.th + BOOK_GAP;
          if (!row || row.used + runW > BAY.run) {
            row = { used: 0, books: [], plaque: null };
            rows.push(row);
          }
          row.books.push({ b, s, isFace: false, x: row.used + runW / 2 - BOOK_GAP / 2 });
          row.used += runW;
        }
      }
      for (const r of rows) {
        const a = surnameOf(r.books[0].b), z = surnameOf(r.books[r.books.length - 1].b);
        r.plaque = { text: a === z ? a : `${a} — ${z}`, n: null };
      }
      return rows;
    }

    for (const g of groups) {
      const faceOut = g.items.length >= 3 ? g.items.find((b) => b.cover_url) : null;
      let firstRow = true;
      let row = null;
      const push = (b, isFace) => {
        const s = sizeOf(b);
        const runW = (isFace ? s.w : s.th) + BOOK_GAP;
        if (!row || row.used + runW > BAY.run) {
          row = { used: 0, books: [], plaque: firstRow ? { text: g.key, n: g.items.length } : null };
          firstRow = false;
          rows.push(row);
        }
        row.books.push({ b, s, isFace, x: row.used + runW / 2 - BOOK_GAP / 2 });
        row.used += runW;
      };
      if (faceOut) push(faceOut, true);
      for (const b of g.items) if (b !== faceOut) push(b, false);
    }
    return rows;
  }

  // world placement for every book + transforms for every bay
  function computeLayout(groups) {
    const rows = packRows(groups);
    const bayCount = Math.max(3, Math.ceil(rows.length / BAY.rows));
    // spread rows proportionally so every bay gets a share (never 2/2/0)
    const bayOfRow = rows.map((_, ri) => Math.floor((ri * bayCount) / rows.length));
    const rowInBayOf = [];
    {
      const counters = new Array(bayCount).fill(0);
      for (const bi of bayOfRow) rowInBayOf.push(counters[bi]++);
    }
    const step = BAY.w + BAY.gap;
    const maxArc = Math.PI * 1.62;                       // leave an opening for the windows
    radius = Math.max(2.6, (step * bayCount) / maxArc);
    const angStep = step / radius;
    arcHalf = (bayCount * angStep) / 2;
    const bays = [];
    for (let i = 0; i < bayCount; i++) {
      const a = (i - (bayCount - 1) / 2) * angStep;      // 0 = straight ahead (-z from the door)
      const pos = new THREE.Vector3(Math.sin(a) * radius, 0, -Math.cos(a) * radius);
      // face the centre of the rotunda: local +z (the bay front) -> origin
      const quat = new THREE.Quaternion().setFromAxisAngle(new THREE.Vector3(0, 1, 0), -a);
      bays.push({ pos, quat, a });
    }
    bayEndColliders = [bays[0], bays[bays.length - 1]].map((b) => ({ x: b.pos.x, z: b.pos.z, r: 0.95 }));

    const placements = new Map();   // book id -> {pos, quat, out}
    const plaques = [];             // {bay, rowInBay, text, n}
    const rot90 = new THREE.Quaternion().setFromAxisAngle(new THREE.Vector3(0, 1, 0), Math.PI / 2);

    rows.forEach((row, ri) => {
      const bay = bays[bayOfRow[ri]];
      const rowInBay = BAY.rows - 1 - rowInBayOf[ri];   // fill from eye level down
      const shelfTop = BAY.baseH + rowInBay * BAY.rowH + BAY.shelfT;
      const startX = -row.used / 2;   // center each run in its bay

      if (row.plaque) plaques.push({ bay, rowInBay, text: row.plaque.text, n: row.plaque.n });

      for (const it of row.books) {
        const { b, s, isFace } = it;
        const jz = jitter(b, "z") * 0.008;
        const local = new THREE.Vector3(
          startX + it.x,
          shelfTop + s.h / 2,
          isFace ? (BAY.depth / 2 - s.th / 2 - 0.05) : (BAY.depth / 2 - s.w / 2 - 0.012 + jz),
        );
        const localQ = isFace
          ? new THREE.Quaternion().setFromAxisAngle(new THREE.Vector3(0, 1, 0), jitter(b, "r") * 0.03)
          : rot90.clone().multiply(new THREE.Quaternion().setFromAxisAngle(new THREE.Vector3(0, 1, 0), jitter(b, "r") * 0.02));

        const pos = local.clone().applyQuaternion(bay.quat).add(bay.pos);
        const quat = bay.quat.clone().multiply(localQ);
        const out = new THREE.Vector3(0, 0, 1).applyQuaternion(bay.quat);
        placements.set(b.id, { pos, quat, out, size: s });
      }
    });

    return { bays, placements, plaques, groupsCount: groups.length };
  }

  /* ---------- bookcases (rebuilt per layout) ---------- */

  function removeCases() {
    if (!caseRoot) return;
    scene.remove(caseRoot);
    caseRoot.traverse((o) => {
      if (o.isMesh) {
        o.geometry.dispose();
        if (o.material.map && o.material.userData.own) o.material.map.dispose();
        if (o.material.userData.own) o.material.dispose();
      }
    });
    caseRoot = null;
  }

  function buildCases(layout, wood) {
    removeCases();
    caseRoot = new THREE.Group();

    const woodMat = new THREE.MeshStandardMaterial({ map: wood, roughness: 0.72, metalness: 0.05 });
    const woodDark = new THREE.MeshStandardMaterial({ color: 0x1e1209, roughness: 0.9 });
    const glowMat = new THREE.MeshBasicMaterial({
      color: 0xffc98a, transparent: true, opacity: 0.85,
      blending: THREE.AdditiveBlending, depthWrite: false,
    });
    const gold = goldMat();

    layout.bays.forEach((bay, bi) => {
      const g = new THREE.Group();
      g.position.copy(bay.pos);
      g.quaternion.copy(bay.quat);

      const box = (w, h, d, x, y, z, mat = woodMat) => {
        const m = new THREE.Mesh(new THREE.BoxGeometry(w, h, d), mat);
        m.position.set(x, y, z);
        m.castShadow = true;
        m.receiveShadow = true;
        g.add(m);
        return m;
      };

      // carcass: sides, back, plinth, crown
      box(BAY.side, BAY.h, BAY.depth, -(BAY.w - BAY.side) / 2, BAY.h / 2, 0);
      box(BAY.side, BAY.h, BAY.depth, (BAY.w - BAY.side) / 2, BAY.h / 2, 0);
      box(BAY.w, BAY.h, 0.02, 0, BAY.h / 2, -BAY.depth / 2 + 0.01, woodDark);
      box(BAY.w + 0.06, BAY.baseH, BAY.depth + 0.05, 0, BAY.baseH / 2, 0.012);
      box(BAY.w + 0.08, BAY.crownH, BAY.depth + 0.07, 0, BAY.h - BAY.crownH / 2, 0.02);

      // a carved pilaster with a gilt capital between neighbouring bays
      if (bi > 0) {
        const col = new THREE.Mesh(new THREE.CylinderGeometry(0.055, 0.065, BAY.h + 0.34, 10), woodMat);
        col.position.set(-(BAY.w + BAY.gap) / 2, (BAY.h + 0.34) / 2, BAY.depth / 2 - 0.02);
        col.castShadow = true;
        g.add(col);
        const capTop = new THREE.Mesh(new THREE.CylinderGeometry(0.075, 0.055, 0.05, 10), gold);
        capTop.position.set(-(BAY.w + BAY.gap) / 2, BAY.h + 0.32, BAY.depth / 2 - 0.02);
        g.add(capTop);
      }

      // shelf planks + a warm candle strip on the underside of each
      for (let r = 0; r <= BAY.rows; r++) {
        const y = BAY.baseH + r * BAY.rowH;
        if (r < BAY.rows) box(BAY.w - BAY.side, BAY.shelfT, BAY.depth - 0.02, 0, y + BAY.shelfT / 2, 0);
        if (r > 0) {
          const strip = new THREE.Mesh(new THREE.PlaneGeometry(BAY.w - 2 * BAY.side, 0.012), glowMat);
          strip.position.set(0, y - 0.014, BAY.depth / 2 - 0.035);
          strip.rotation.x = -Math.PI / 3;
          g.add(strip);
        }
      }
      caseRoot.add(g);
    });

    // a wooden ladder leaning against the second bay, as libraries demand
    if (layout.bays.length > 1) {
      const bay = layout.bays[1];
      const ladder = new THREE.Group();
      const railGeo = new THREE.BoxGeometry(0.035, 1.95, 0.03);
      for (const sx of [-0.17, 0.17]) {
        const rail = new THREE.Mesh(railGeo, woodDark);
        rail.position.set(sx, 0.975, 0);
        rail.castShadow = true;
        ladder.add(rail);
      }
      for (let i = 0; i < 6; i++) {
        const rung = new THREE.Mesh(new THREE.CylinderGeometry(0.014, 0.014, 0.34, 6), woodDark);
        rung.rotation.z = Math.PI / 2;
        rung.position.set(0, 0.28 + i * 0.28, 0);
        ladder.add(rung);
      }
      ladder.rotation.x = -0.2;      // leaning back onto the case
      const local = new THREE.Vector3(0.45, 0, BAY.depth / 2 + 0.36);
      ladder.position.copy(local.applyQuaternion(bay.quat).add(bay.pos));
      ladder.quaternion.copy(bay.quat);
      ladder.rotateX(-0.2);
      caseRoot.add(ladder);
    }

    // brass plaques on the shelf edge where each group begins
    for (const p of layout.plaques) {
      const tex = plaqueTexture(p.text, p.n);
      const mat = new THREE.MeshBasicMaterial({ map: tex });
      mat.userData.own = true;
      const m = new THREE.Mesh(new THREE.PlaneGeometry(0.3, 0.075), mat);
      const y = BAY.baseH + p.rowInBay * BAY.rowH + BAY.shelfT / 2 - 0.001;
      const local = new THREE.Vector3(-(BAY.run / 2) + 0.16, y - 0.048, BAY.depth / 2 + 0.004);
      m.position.copy(local.applyQuaternion(p.bay.quat).add(p.bay.pos));
      m.quaternion.copy(p.bay.quat);
      caseRoot.add(m);
    }

    scene.add(caseRoot);
    shadowDirty = true;
  }

  /* ---------- books ---------- */

  function makeBook(b) {
    const cloth = clothOf(b);
    const [canvas, ctx] = canvas2d(TEX_SIZE, TEX_SIZE);
    drawBookCanvas(ctx, b, cloth, null);
    const tex = new THREE.CanvasTexture(canvas);
    tex.colorSpace = THREE.SRGBColorSpace;
    tex.anisotropy = 4;
    const mat = new THREE.MeshStandardMaterial({ map: tex, roughness: 0.75, metalness: 0.02 });
    const mesh = new THREE.Mesh(sharedGeo, mat);
    mesh.castShadow = true;
    mesh.receiveShadow = true;
    const s = sizeOf(b);
    mesh.scale.set(s.w, s.h, s.th);

    const rec = { b, mesh, canvas, ctx, tex, cloth, home: null, out: null, hoverT: null, dead: false, floating: false };
    mesh.userData.rec = rec;

    // upgrade to the real cover once (and only once) it arrives
    const src = upgradeCover(b.cover_url);
    if (src) {
      const img = new Image();
      img.crossOrigin = "anonymous";
      img.onload = () => {
        if (rec.dead) return;
        try {
          const dyed = avgColor(img);
          ctx.clearRect(0, 0, TEX_SIZE, TEX_SIZE);
          drawBookCanvas(ctx, b, dyed, img);
          tex.needsUpdate = true;
          shadowDirty = true;
        } catch { /* tainted or broken image — keep the cloth binding */ }
      };
      img.src = src;
    }
    return rec;
  }

  function disposeBook(rec) {
    if (presenting === rec) presenting = null;
    rec.dead = true;
    scene.remove(rec.mesh);
    rec.tex.dispose();
    rec.mesh.material.dispose();
    bookNodes.delete(rec.b.id);
  }

  /* ---------- placing + the flight ---------- */

  function snapTo(rec, pl) {
    rec.home = pl;
    rec.out = pl.out;
    rec.mesh.position.copy(pl.pos);
    rec.mesh.quaternion.copy(pl.quat);
  }

  // send a book arcing through the rotunda to its new shelf
  function flyTo(rec, pl, delay) {
    rec.home = pl;
    rec.out = pl.out;
    const p0 = rec.mesh.position.clone();
    const q0 = rec.mesh.quaternion.clone();
    const p1 = pl.pos.clone();
    const q1 = pl.quat.clone();
    const dist = p0.distanceTo(p1);
    if (dist < 0.002) { snapTo(rec, pl); return; }
    gsap.killTweensOf(rec.mesh.position);   // a hover pull-out must not fight the flight

    // control point: pulled toward the middle of the hall and kept just
    // overhead, so from inside the room the flights sweep right past you
    // (and clear the rose cloche) instead of arcing out of view
    const mid = p0.clone().lerp(p1, 0.5);
    mid.x *= 0.4; mid.z *= 0.4;
    mid.y = Math.max(1.85, mid.y + 0.35 + Math.min(1.1, dist * 0.2));

    const spin = (jitter(rec.b, "spin")) * 1.4;
    const axis = new THREE.Vector3(0, 1, 0);
    const st = { t: 0 };
    gsap.to(st, {
      t: 1,
      duration: 1.05 + Math.min(0.9, dist * 0.12),
      delay,
      ease: "power2.inOut",
      onUpdate: () => {
        const t = st.t, u = 1 - t;
        tmpV.copy(p0).multiplyScalar(u * u)
          .add(tmpV2.copy(mid).multiplyScalar(2 * u * t))
          .add(tmpV2.copy(p1).multiplyScalar(t * t));
        rec.mesh.position.copy(tmpV);
        rec.mesh.quaternion.slerpQuaternions(q0, q1, t);
        tmpQ.setFromAxisAngle(axis, Math.sin(t * Math.PI) * spin);
        rec.mesh.quaternion.multiply(tmpQ);
        shadowDirty = true;
      },
    });
  }

  // brand-new books tumble down from the oculus
  function dropIn(rec, pl, delay) {
    rec.home = pl;
    rec.out = pl.out;
    const start = new THREE.Vector3(jitter(rec.b, "dx") * 0.8, 7, jitter(rec.b, "dz") * 0.8);
    rec.mesh.position.copy(start);
    rec.mesh.quaternion.copy(pl.quat);
    rec.mesh.material.transparent = true;
    rec.mesh.material.opacity = 0;
    gsap.to(rec.mesh.material, { opacity: 1, duration: 0.4, delay, onComplete: () => { rec.mesh.material.transparent = false; } });
    flyTo(rec, pl, delay);
  }

  function regroup(animate) {
    if (!books || !renderer) return;
    countEl.textContent = "";
    if (!books.length) {
      // an emptied hall (e.g. switching to an empty library) must clear
      // the previous library's books and cases, not just the count line
      setHover(null);
      for (const [, rec] of [...bookNodes]) disposeBook(rec);
      removeCases();
      shadowDirty = true;
      countEl.textContent = "no books yet — scan a barcode and the hall fills itself";
      return;
    }

    setHover(null);
    const groups = computeGroups(books, mode);
    const layout = computeLayout(groups);
    buildShell(radius + 2.4);
    buildCases(layout, assets.wood);
    collidePlayer(player.pos);   // a shrinking ring nudges the reader back in

    const seen = new Set();
    const anim = animate && !reducedMotion();
    let i = 0;
    for (const b of books) {
      seen.add(b.id);
      const pl = layout.placements.get(b.id);
      if (!pl) continue;
      let rec = bookNodes.get(b.id);
      if (!rec) {
        rec = makeBook(b);
        bookNodes.set(b.id, rec);
        scene.add(rec.mesh);
        if (anim && !firstBuild) dropIn(rec, pl, Math.min(0.6, i * 0.05));
        else snapTo(rec, pl);
      } else if (rec === presenting) {
        // a book held up in front of the reader keeps floating; it learns
        // its new shelf and flies there once the sheet closes
        rec.home = pl;
        rec.out = pl.out;
      } else if (anim) {
        // a long stagger turns the resort into a stream of books gliding
        // past the reader instead of one near-instant blur
        flyTo(rec, pl, Math.min(1.3, i * 0.045) + Math.abs(jitter(b, "d")) * 0.15);
      } else {
        snapTo(rec, pl);
      }
      i++;
    }
    for (const [id, rec] of [...bookNodes]) {
      if (!seen.has(id)) {
        gsap.to(rec.mesh.position, { y: 7.5, duration: 0.7, ease: "power2.in", onComplete: () => disposeBook(rec) });
      }
    }

    const n = books.length, s = layout.groupsCount;
    countEl.textContent = mode === "author"
      ? `${n} book${n === 1 ? "" : "s"} · a–z by author · walk the stacks`
      : `${n} book${n === 1 ? "" : "s"} · ${s} shel${s === 1 ? "f" : "ves"} · walk the stacks`;

    if (firstBuild) {
      firstBuild = false;
      playIntro();
    }
    shadowDirty = true;
  }

  /* ---------- cinematic intro ---------- */

  function playIntro() {
    // where you come to rest: just inside the doors, facing the stacks
    player.pos.set(0, 0, Math.max(1.7, radius * 0.62));
    player.vel.set(0, 0, 0);
    player.yaw = 0;
    player.pitch = 0;
    if (reducedMotion() || introPlayed) {
      introPlayed = true;
      return;
    }
    introPlayed = true;
    // drift down out of the dome, turning to face the shelves
    player.flyY = 3.1;
    player.yaw = -2.4;
    player.pitch = -0.6;
    gsap.to(player, { flyY: 0, duration: 3.2, ease: "power3.inOut" });
    gsap.to(player, { yaw: 0, pitch: 0, duration: 3.4, ease: "power2.inOut" });
  }

  /* ---------- hover + click ---------- */

  function pick(e) {
    if (e) {
      const r = renderer.domElement.getBoundingClientRect();
      const x = ((e.clientX - r.left) / r.width) * 2 - 1;
      const y = -((e.clientY - r.top) / r.height) * 2 + 1;
      raycaster.setFromCamera({ x, y }, camera);
    } else {
      raycaster.setFromCamera({ x: 0, y: 0 }, camera);   // the crosshair
    }
    raycaster.far = 6;
    const meshes = [];
    for (const rec of bookNodes.values()) meshes.push(rec.mesh);
    const hit = raycaster.intersectObjects(meshes, false)[0];
    return hit ? hit.object.userData.rec : null;
  }

  function setHover(rec) {
    if (hovered === rec) return;
    if (hovered && hovered.home && hovered !== presenting) {
      gsap.to(hovered.mesh.position, {
        x: hovered.home.pos.x, y: hovered.home.pos.y, z: hovered.home.pos.z,
        duration: 0.35, ease: "power2.out", onUpdate: () => { shadowDirty = true; },
      });
    }
    hovered = rec;
    if (focusEl) {
      if (rec) {
        focusEl.textContent = `${(rec.b.title || "untitled").toLowerCase()} — ${authorsOf(rec.b).toLowerCase()}`;
        focusEl.classList.add("on");
      } else {
        focusEl.classList.remove("on");
      }
    }
    crossEl?.classList.toggle("hot", !!rec);
    if (rec && rec.home && rec !== presenting && !reducedMotion()) {
      const p = rec.home.pos.clone().addScaledVector(rec.out, 0.07);
      gsap.to(rec.mesh.position, {
        x: p.x, y: p.y, z: p.z, duration: 0.3, ease: "power2.out",
        onUpdate: () => { shadowDirty = true; },
      });
    }
  }

  // where a held book floats: in front of the camera, lifted so it hangs
  // above the detail sheet instead of hiding behind it
  const tmpV3 = new THREE.Vector3();
  function floatPose(outP, outQ) {
    outP.set(0, 0, -1).applyQuaternion(camera.quaternion);
    tmpV3.set(0, 1, 0).applyQuaternion(camera.quaternion);
    outP.multiplyScalar(0.5).add(camera.position).addScaledVector(tmpV3, 0.16);
    outQ.copy(camera.quaternion);
  }

  function present(rec) {
    if (presenting) return;
    presenting = rec;
    gsap.killTweensOf(rec.mesh.position);
    const st = { t: 0 };
    const p0 = rec.mesh.position.clone();
    const q0 = rec.mesh.quaternion.clone();
    gsap.to(st, {
      t: 1, duration: reducedMotion() ? 0 : 0.55, ease: "power3.out",
      onUpdate: () => {
        floatPose(tmpV, tmpQ);
        rec.mesh.position.lerpVectors(p0, tmpV, st.t);
        rec.mesh.quaternion.slerpQuaternions(q0, tmpQ, st.t);
        shadowDirty = true;
      },
      onComplete: () => {
        rec.floating = true;            // it hangs here until the sheet closes
        document.exitPointerLock?.();   // hand the cursor back for the sheet
        openShelfBook(rec.b);
      },
    });
  }

  // the held book drifts gently in place while the sheet is up
  const tmpQ2 = new THREE.Quaternion();
  const upAxis = new THREE.Vector3(0, 1, 0);
  function tickFloating(t, dt) {
    if (!presenting || !presenting.floating) return;
    floatPose(tmpV, tmpQ);
    if (!reducedMotion()) {
      tmpV.y += Math.sin(t * 0.0016) * 0.008;
      tmpQ.multiply(tmpQ2.setFromAxisAngle(upAxis, Math.sin(t * 0.0006) * 0.16));
    }
    const k = 1 - Math.exp(-dt * 10);
    presenting.mesh.position.lerp(tmpV, k);
    presenting.mesh.quaternion.slerp(tmpQ, k);
    shadowDirty = true;
  }

  /* ---------- render loop ---------- */

  let lastT = 0;
  let hoverFrame = 0;

  function tick(t) {
    const dt = Math.min(0.05, lastT ? (t - lastT) / 1000 : 0.016);
    lastT = t;

    applyCamera(dt);
    tickAmbience(t, dt);
    tickFloating(t, dt);

    // the crosshair rests on a book: pull it out a whisker and name it
    if (pointerLocked && !presenting && (hoverFrame++ % 6 === 0)) {
      setHover(pick(null));
    }

    if (shadowDirty) {
      renderer.shadowMap.needsUpdate = true;
      shadowDirty = false;
    }
    renderer.render(scene, camera);

    // adaptive quality: if the first seconds run slow, drop resolution + shadows
    if (!perfChecked) {
      frameTimes.push(t);
      if (frameTimes.length === 90) {
        const avg = (frameTimes[89] - frameTimes[9]) / 80;
        if (avg > 30) {
          renderer.setPixelRatio(1);
          renderer.shadowMap.enabled = false;
          scene.traverse((o) => { if (o.isMesh) o.castShadow = o.receiveShadow = false; });
        }
        perfChecked = true;
        frameTimes = null;
      }
    }
  }

  function setRunning(on) {
    if (!renderer || on === running) return;
    running = on;
    if (!on) { keys.clear(); lastT = 0; }
    renderer.setAnimationLoop(on ? tick : null);
  }

  function resize() {
    if (!renderer) return;
    const w = worldEl.clientWidth, h = worldEl.clientHeight;
    if (!w || !h) return;
    renderer.setSize(w, h);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
  }

  /* ---------- boot ---------- */

  let assets = null;

  function initWorld() {
    const canvas = document.createElement("canvas");
    renderer = new THREE.WebGLRenderer({ canvas, antialias: true, powerPreference: "high-performance" });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.12;
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    renderer.shadowMap.autoUpdate = false;   // only re-render shadows when something moved
    worldEl.prepend(canvas);

    scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0d0906);
    scene.fog = new THREE.FogExp2(0x0d0906, 0.045);

    camera = new THREE.PerspectiveCamera(62, 1, 0.05, 40);
    raycaster = new THREE.Raycaster();

    assets = buildRoom();
    bindControls(renderer.domElement);

    setHint(coarsePointer()
      ? "left thumb to walk · right thumb to look · tap a book"
      : "click to step inside · wasd to walk · mouse to look");

    new ResizeObserver(resize).observe(worldEl);
    resize();

    // the held book flies home the moment the detail sheet closes
    const sheetEl = document.getElementById("sheet");
    new MutationObserver(() => {
      if (presenting && sheetEl.classList.contains("hidden")) {
        const rec = presenting;
        presenting = null;
        rec.floating = false;
        if (!rec.dead && rec.home) flyTo(rec, rec.home, 0.05);
      }
    }).observe(sheetEl, { attributes: true, attributeFilter: ["class"] });

    // pause when the tab or the view goes away
    document.addEventListener("visibilitychange", () => setRunning(!document.hidden && viewVisible()));
    new MutationObserver(() => {
      setRunning(!document.hidden && viewVisible());
      if (viewVisible()) resize();
    }).observe(viewEl, { attributes: true, attributeFilter: ["class"] });
  }

  /* ---------- data plumbing (same flow as the DOM fallback) ---------- */

  const sigOf = (list) =>
    list.map((b) => `${b.id}:${b.genre ?? ""}:${b.format ?? ""}`).sort().join("|");

  async function enrichGenres() {
    if (enriching || enrichDone || !books || !books.length) return;
    enriching = true;
    try {
      for (let round = 0; round < ENRICH_ROUNDS; round++) {
        const res = await api("/api/enrich/genres", {
          method: "POST",
          body: JSON.stringify({ limit: ENRICH_BATCH }),
        });
        const updated = res.updated || [];
        if (updated.length) {
          const genreById = new Map(updated.map((u) => [u.id, u.genre]));
          for (const b of books) if (genreById.has(b.id)) b.genre = genreById.get(b.id);
          if (mode === "genre" && viewVisible()) regroup(true);
        }
        if (!res.remaining) { enrichDone = true; break; }
      }
    } catch {
      enrichDone = true;
    }
    enriching = false;
  }

  let pendingEnter = false;

  async function enter() {
    setRunning(true);
    resize();
    if (loading) { pendingEnter = true; return; }  // re-run with the latest scope after
    const first = !books;
    if (first) countEl.textContent = "unlocking the library…";
    loading = true;
    let fresh;
    try {
      const data = await api(`/api/books?status=library${shelfLibraryScope()}`);
      fresh = data.items || [];
    } catch (err) {
      loading = false;
      if (first) countEl.textContent = `couldn't load the library — ${err.message}`;
      if (pendingEnter) { pendingEnter = false; enter(); }
      return;
    }
    loading = false;
    if (pendingEnter) { pendingEnter = false; enter(); return; }  // scope changed mid-fetch
    if (first || sigOf(fresh) !== sigOf(books)) {
      books = fresh;
      regroup(!first);
    } else {
      books = fresh;
    }
    enrichGenres();
  }

  function setMode(m) {
    if (m === mode) return;
    mode = m;
    regroup(true);
  }

  /* ---------- choose implementation + wire the segmented control ---------- */

  let impl;
  try {
    initWorld();
    impl = { enter, setMode };
    caseEl.classList.add("hidden");
    worldEl.classList.remove("hidden");
  } catch (err) {
    // no WebGL — fall back to the DOM bookcase
    console.warn("shelf3d: falling back to DOM shelves —", err);
    if (renderer) { try { renderer.dispose(); } catch { /* ignore */ } renderer = null; }
    worldEl.classList.add("hidden");
    caseEl.classList.remove("hidden");
    impl = window.ShelfDOM;
  }

  document.querySelectorAll("#shelf-mode .seg-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("#shelf-mode .seg-btn").forEach((b) => {
        const on = b === btn;
        b.classList.toggle("active", on);
        b.setAttribute("aria-selected", String(on));
      });
      impl.setMode(btn.dataset.mode);
    });
  });

  window.Shelf = { enter: () => impl.enter() };
})();
