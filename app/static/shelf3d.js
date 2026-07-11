/* shelf3d.js — the "shelves" tab, reimagined as a real 3D world.
   A circular library rotunda rendered with Three.js: wooden bookcases
   arranged in an arc around you, every book a physical object bound in
   cloth with its real cover, a skylight shaft with drifting dust, and
   GSAP-driven flights that send books arcing through the air when the
   grouping changes. Falls back to the DOM bookcase (ShelfDOM) when
   WebGL isn't available. Reuses app.js globals (api, authorsOf,
   openEditionSheet). */

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

  /* ---------- dom ---------- */

  const viewEl = document.getElementById("view-shelves");
  const countEl = document.getElementById("shelves-count");
  const worldEl = document.getElementById("world3d");
  const caseEl = document.getElementById("bookcase");
  const hintEl = document.getElementById("world3d-hint");

  const reducedMotion = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
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

  // vertical-grain wood for the cases
  function woodTexture(base = "#5c452e", dark = "#42311f", light = "#75593a") {
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
    const t = new THREE.CanvasTexture(c);
    t.wrapS = t.wrapT = THREE.RepeatWrapping;
    t.colorSpace = THREE.SRGBColorSpace;
    return t;
  }

  // concentric parquet rings for the rotunda floor
  function floorTexture() {
    const S = 1024;
    const [c, ctx] = canvas2d(S, S);
    ctx.fillStyle = "#231a12";
    ctx.fillRect(0, 0, S, S);
    const cx = S / 2, cy = S / 2;
    for (let r = 30; r < S * 0.75; r += 26) {
      ctx.strokeStyle = `rgba(${52 + Math.random() * 22},${38 + Math.random() * 15},${24 + Math.random() * 10},${0.3 + Math.random() * 0.25})`;
      ctx.lineWidth = 22;
      ctx.beginPath();
      ctx.arc(cx, cy, r, 0, Math.PI * 2);
      ctx.stroke();
      ctx.strokeStyle = "rgba(12,8,5,0.28)";
      ctx.lineWidth = 1.4;
      ctx.beginPath();
      ctx.arc(cx, cy, r + 12, 0, Math.PI * 2);
      ctx.stroke();
    }
    // radial seams
    ctx.strokeStyle = "rgba(12,8,5,0.22)";
    ctx.lineWidth = 1.6;
    for (let a = 0; a < Math.PI * 2; a += Math.PI / 14) {
      ctx.beginPath();
      ctx.moveTo(cx + Math.cos(a) * 40, cy + Math.sin(a) * 40);
      ctx.lineTo(cx + Math.cos(a) * S, cy + Math.sin(a) * S);
      ctx.stroke();
    }
    // warm pool of light in the middle, vignette at the rim
    const glow = ctx.createRadialGradient(cx, cy, 0, cx, cy, S * 0.55);
    glow.addColorStop(0, "rgba(255,190,110,0.10)");
    glow.addColorStop(0.45, "rgba(255,190,110,0.02)");
    glow.addColorStop(1, "rgba(0,0,0,0.55)");
    ctx.fillStyle = glow;
    ctx.fillRect(0, 0, S, S);
    noise(ctx, S, S, 0.04, 3000);
    const t = new THREE.CanvasTexture(c);
    t.colorSpace = THREE.SRGBColorSpace;
    t.anisotropy = 8;
    return t;
  }

  // soft round sprite for dust motes
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
    ctx.font = "600 26px ui-monospace, Menlo, Consolas, monospace";
    while (ctx.measureText(label).width > 230 && label.length > 4) label = label.slice(0, -2).trim() + "…";
    ctx.fillText(label, 128, 34);
    const t = new THREE.CanvasTexture(c);
    t.colorSpace = THREE.SRGBColorSpace;
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
    ctx.font = "600 24px ui-monospace, Menlo, Consolas, monospace";
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
      ctx.font = "600 20px ui-monospace, Menlo, Consolas, monospace";
      const lines = wrapLines(ctx, title, cw - 44, 4);
      lines.forEach((ln, i) => ctx.fillText(ln, cx0 + cw / 2, 72 + i * 26));
      ctx.fillStyle = "rgba(227,179,65,0.75)";
      ctx.fillRect(cx0 + cw / 2 - 22, 178, 44, 2);
      ctx.fillStyle = "#d9c894";
      ctx.font = "15px ui-monospace, Menlo, Consolas, monospace";
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

  /* ---------- orbit controls (tiny, damped, touch-friendly) ---------- */

  const ctrl = {
    theta: 0, phi: 1.24, dist: 2.4,
    tTheta: 0, tPhi: 1.24, tDist: 2.4,
    target: new THREE.Vector3(0, 1.05, 0),
    tTargetY: 1.05,
    minDist: 0.7, maxDist: 8,
    lastInput: 0,
    autoSpin: 0,
  };

  function applyCamera() {
    ctrl.theta += (ctrl.tTheta - ctrl.theta) * 0.09;
    ctrl.phi += (ctrl.tPhi - ctrl.phi) * 0.09;
    ctrl.dist += (ctrl.tDist - ctrl.dist) * 0.09;
    ctrl.target.y += (ctrl.tTargetY - ctrl.target.y) * 0.09;
    const sp = Math.sin(ctrl.phi);
    camera.position.set(
      ctrl.target.x + ctrl.dist * sp * Math.sin(ctrl.theta),
      ctrl.target.y + ctrl.dist * Math.cos(ctrl.phi),
      ctrl.target.z + ctrl.dist * sp * Math.cos(ctrl.theta),
    );
    camera.lookAt(ctrl.target);
  }

  let dragDist = 0;   // separates an orbit-drag release from a tap

  function bindControls(el) {
    const pointers = new Map();
    let pinchD = 0;

    const touch = () => { ctrl.lastInput = performance.now(); dismissHint(); };

    el.addEventListener("pointerdown", (e) => {
      dragDist = 0;
      pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
      if (pointers.size === 2) {
        const [a, b] = [...pointers.values()];
        pinchD = Math.hypot(a.x - b.x, a.y - b.y);
      }
      el.setPointerCapture(e.pointerId);
      touch();
    });

    el.addEventListener("pointermove", (e) => {
      if (!pointers.has(e.pointerId)) { queueHover(e); return; }
      const p = pointers.get(e.pointerId);
      const dx = e.clientX - p.x, dy = e.clientY - p.y;
      dragDist += Math.abs(dx) + Math.abs(dy);
      p.x = e.clientX; p.y = e.clientY;
      if (pointers.size === 1) {
        ctrl.tTheta -= dx * 0.005;
        ctrl.tPhi = Math.max(0.35, Math.min(1.53, ctrl.tPhi - dy * 0.004));
        touch();
      } else if (pointers.size === 2) {
        const [a, b] = [...pointers.values()];
        const d = Math.hypot(a.x - b.x, a.y - b.y);
        if (pinchD) {
          ctrl.tDist = Math.max(ctrl.minDist, Math.min(ctrl.maxDist, ctrl.tDist * (pinchD / d)));
          ctrl.tTargetY = Math.max(0.5, Math.min(2.4, ctrl.tTargetY + dy * 0.002));
        }
        pinchD = d;
        touch();
      }
    });

    const up = (e) => { pointers.delete(e.pointerId); pinchD = 0; };
    el.addEventListener("pointerup", up);
    el.addEventListener("pointercancel", up);

    el.addEventListener("wheel", (e) => {
      e.preventDefault();
      ctrl.tDist = Math.max(ctrl.minDist, Math.min(ctrl.maxDist, ctrl.tDist * (1 + e.deltaY * 0.0012)));
      touch();
    }, { passive: false });

    el.addEventListener("dblclick", () => {
      ctrl.tDist = ctrl.tDist > 2 ? 1.1 : radius * 0.78;
      touch();
    });
  }

  function dismissHint() {
    if (hintEl && !hintEl.classList.contains("gone")) hintEl.classList.add("gone");
  }

  /* ---------- room ---------- */

  let roomRoot = null;
  let roomWallR = 0;

  // the shell (floor, wall, dome, skylight) is sized to the bookcase ring
  // and rebuilt only when a regroup meaningfully changes the arc radius
  function buildShell(wallR) {
    if (Math.abs(wallR - roomWallR) < 0.6 && roomRoot) return;
    roomWallR = wallR;
    if (roomRoot) {
      scene.remove(roomRoot);
      roomRoot.traverse((o) => { if (o.isMesh) o.geometry.dispose(); });
    }
    roomRoot = new THREE.Group();
    const ceilH = Math.max(4.6, wallR * 1.05);

    const floor = new THREE.Mesh(
      new THREE.CircleGeometry(wallR + 0.5, 64),
      new THREE.MeshStandardMaterial({ map: assets.floorTex, roughness: 0.55, metalness: 0.04 }),
    );
    floor.rotation.x = -Math.PI / 2;
    floor.receiveShadow = true;
    roomRoot.add(floor);

    const wall = new THREE.Mesh(
      new THREE.CylinderGeometry(wallR, wallR, ceilH, 48, 1, true),
      new THREE.MeshStandardMaterial({ color: 0x18202e, roughness: 0.95, side: THREE.BackSide }),
    );
    wall.position.y = ceilH / 2;
    roomRoot.add(wall);

    const dome = new THREE.Mesh(
      new THREE.SphereGeometry(wallR, 40, 18, 0, Math.PI * 2, 0, Math.PI / 2),
      new THREE.MeshStandardMaterial({ color: 0x11161f, roughness: 1, side: THREE.BackSide }),
    );
    dome.position.y = ceilH;
    dome.scale.y = 0.55;
    roomRoot.add(dome);

    // oculus ring + the shaft of light through it
    const oculusY = ceilH + wallR * 0.5;
    const ring = new THREE.Mesh(
      new THREE.TorusGeometry(0.95, 0.08, 12, 48),
      new THREE.MeshBasicMaterial({ color: 0xffe6bb }),
    );
    ring.rotation.x = Math.PI / 2;
    ring.position.y = oculusY;
    roomRoot.add(ring);

    const shaft = (r0, r1, op) => {
      const m = new THREE.Mesh(
        new THREE.CylinderGeometry(r0, r1, oculusY, 32, 1, true),
        new THREE.MeshBasicMaterial({
          color: 0xffdda6, transparent: true, opacity: op,
          blending: THREE.AdditiveBlending, depthWrite: false, side: THREE.DoubleSide,
        }),
      );
      m.position.y = oculusY / 2;
      roomRoot.add(m);
    };
    shaft(0.9, 1.55, 0.022);
    shaft(0.55, 1.0, 0.03);

    // wall sconces: emissive amber slivers with a soft halo, purely decorative
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

    scene.add(roomRoot);
    shadowDirty = true;
  }

  function buildRoom() {
    const wood = woodTexture();
    const floorTex = floorTexture();

    // lights
    scene.add(new THREE.HemisphereLight(0xaab7cc, 0x2a1c10, 0.85));

    const sun = new THREE.DirectionalLight(0xffe2b0, 2.6);
    sun.position.set(1.2, 7.2, 0.6);
    sun.castShadow = true;
    sun.shadow.mapSize.set(1024, 1024);
    sun.shadow.camera.left = sun.shadow.camera.bottom = -8;
    sun.shadow.camera.right = sun.shadow.camera.top = 8;
    sun.shadow.camera.far = 12;
    sun.shadow.bias = -0.0005;
    scene.add(sun);

    const warmA = new THREE.PointLight(0xffb46b, 8, 12, 1.8);
    warmA.position.set(2.4, 2.7, -2.0);
    scene.add(warmA);
    const warmB = new THREE.PointLight(0xff9d55, 6, 12, 1.8);
    warmB.position.set(-2.6, 2.4, 2.2);
    scene.add(warmB);

    // drifting dust in the shaft
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
    const dustTex = dustTexture();
    dust = new THREE.Points(dustGeo, new THREE.PointsMaterial({
      map: dustTex, size: 0.022, transparent: true, opacity: 0.5,
      blending: THREE.AdditiveBlending, depthWrite: false, sizeAttenuation: true,
    }));
    scene.add(dust);

    return { wood, floorTex, dustTex };
  }

  function tickDust(t) {
    if (!dust) return;
    const pos = dust.geometry.attributes.position;
    for (let i = 0; i < pos.count; i++) {
      let y = pos.getY(i) - 0.0011;
      if (y < 0.04) y = 6.8;
      pos.setY(i, y);
      pos.setX(i, pos.getX(i) + Math.sin(t * 0.0004 + dustSeed[i]) * 0.0006);
    }
    pos.needsUpdate = true;
  }

  /* ---------- layout: groups -> shelf rows -> bays on an arc ---------- */

  // pack a group's books into rows that fit the bay run; the group's first
  // covered book is displayed face-out like a bookshop staff pick
  function packRows(groups) {
    const rows = [];
    for (const g of groups) {
      const faceOut = g.items.length >= 3 ? g.items.find((b) => b.cover_url) : null;
      let row = null;
      const push = (b, isFace) => {
        const s = sizeOf(b);
        const runW = (isFace ? s.w : s.th) + BOOK_GAP;
        if (!row || row.used + runW > BAY.run) {
          row = { group: g.key, first: !rows.some((r) => r.group === g.key), used: 0, books: [] };
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
    const perBay = Math.ceil(rows.length / bayCount);   // balance rows so no bay sits empty
    const step = BAY.w + BAY.gap;
    const maxArc = Math.PI * 1.62;                       // leave an opening behind the camera
    radius = Math.max(2.6, (step * bayCount) / maxArc);
    const angStep = step / radius;
    const bays = [];
    for (let i = 0; i < bayCount; i++) {
      const a = (i - (bayCount - 1) / 2) * angStep;      // 0 = straight ahead (-z from camera start)
      const pos = new THREE.Vector3(Math.sin(a) * radius, 0, -Math.cos(a) * radius);
      // face the centre of the rotunda: local +z (the bay front) -> origin
      const quat = new THREE.Quaternion().setFromAxisAngle(new THREE.Vector3(0, 1, 0), -a);
      bays.push({ pos, quat, a });
    }

    const placements = new Map();   // book id -> {pos, quat, out}
    const plaques = [];             // {bay, rowInBay, text, n}
    const countByGroup = new Map(groups.map((g) => [g.key, g.items.length]));
    const rot90 = new THREE.Quaternion().setFromAxisAngle(new THREE.Vector3(0, 1, 0), Math.PI / 2);

    rows.forEach((row, ri) => {
      const bay = bays[Math.floor(ri / perBay)];
      const rowInBay = BAY.rows - 1 - (ri % perBay);    // fill from eye level down
      const shelfTop = BAY.baseH + rowInBay * BAY.rowH + BAY.shelfT;
      const startX = -row.used / 2;   // center each run in its bay

      if (row.first) plaques.push({ bay, rowInBay, text: row.group, n: countByGroup.get(row.group) });

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

  function buildCases(layout, wood) {
    if (caseRoot) {
      scene.remove(caseRoot);
      caseRoot.traverse((o) => {
        if (o.isMesh) {
          o.geometry.dispose();
          if (o.material.map && o.material.userData.own) o.material.map.dispose();
          if (o.material.userData.own) o.material.dispose();
        }
      });
    }
    caseRoot = new THREE.Group();

    const woodMat = new THREE.MeshStandardMaterial({ map: wood, roughness: 0.72, metalness: 0.05 });
    const woodDark = new THREE.MeshStandardMaterial({ color: 0x241812, roughness: 0.9 });
    const glowMat = new THREE.MeshBasicMaterial({
      color: 0xffc98a, transparent: true, opacity: 0.85,
      blending: THREE.AdditiveBlending, depthWrite: false,
    });

    for (const bay of layout.bays) {
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

      // shelf planks + a warm light strip on the underside of each
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

    const rec = { b, mesh, canvas, ctx, tex, cloth, home: null, out: null, hoverT: null, dead: false };
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

    // control point: lifted and pulled toward the center of the room, so
    // flights sweep through the light shaft instead of clipping the cases
    const mid = p0.clone().lerp(p1, 0.5);
    mid.x *= 0.45; mid.z *= 0.45;
    mid.y += 0.5 + Math.min(1.6, dist * 0.32);

    const spin = (jitter(rec.b, "spin")) * 1.4;
    const axis = new THREE.Vector3(0, 1, 0);
    const st = { t: 0 };
    gsap.to(st, {
      t: 1,
      duration: 0.85 + Math.min(0.7, dist * 0.09),
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
      countEl.textContent = "no books yet — scan a barcode and the hall fills itself";
      return;
    }

    setHover(null);
    const groups = computeGroups(books, mode);
    const layout = computeLayout(groups);
    buildShell(radius + 2.4);
    buildCases(layout, assets.wood);
    ctrl.maxDist = radius + 1.6;

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
      } else if (anim) {
        flyTo(rec, pl, Math.min(0.55, i * 0.006 + Math.abs(jitter(b, "d")) * 0.12));
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
    countEl.textContent = `${n} book${n === 1 ? "" : "s"} · ${s} shel${s === 1 ? "f" : "ves"} · drag to wander`;

    if (firstBuild) {
      firstBuild = false;
      playIntro();
    }
    shadowDirty = true;
  }

  /* ---------- cinematic intro ---------- */

  function playIntro() {
    ctrl.tDist = Math.min(ctrl.maxDist, Math.max(2.2, radius * 0.72));
    ctrl.tPhi = 1.24;
    ctrl.tTheta = 0;
    if (reducedMotion() || introPlayed) {
      ctrl.dist = ctrl.tDist; ctrl.phi = ctrl.tPhi; ctrl.theta = ctrl.tTheta;
      introPlayed = true;
      return;
    }
    introPlayed = true;
    // start high inside the light shaft, looking down over the hall,
    // then swoop down and around to eye level
    ctrl.dist = 0.05; ctrl.phi = 0.12; ctrl.theta = -2.6;
    ctrl.target.y = 1.05;
    gsap.to(ctrl, { dist: ctrl.tDist, duration: 3.0, ease: "power3.inOut" });
    gsap.to(ctrl, { phi: ctrl.tPhi, duration: 3.0, ease: "power2.inOut" });
    gsap.to(ctrl, { theta: ctrl.tTheta, duration: 3.2, ease: "power2.out" });
    ctrl.lastInput = performance.now() + 2500; // hold off the auto-orbit briefly
  }

  /* ---------- hover + click ---------- */

  let hoverEvent = null;
  function queueHover(e) { hoverEvent = e; }

  function pick(e) {
    const r = renderer.domElement.getBoundingClientRect();
    const x = ((e.clientX - r.left) / r.width) * 2 - 1;
    const y = -((e.clientY - r.top) / r.height) * 2 + 1;
    raycaster.setFromCamera({ x, y }, camera);
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
    renderer.domElement.style.cursor = rec ? "pointer" : "";
    if (rec && rec.home && rec !== presenting && !reducedMotion()) {
      const p = rec.home.pos.clone().addScaledVector(rec.out, 0.07);
      gsap.to(rec.mesh.position, {
        x: p.x, y: p.y, z: p.z, duration: 0.3, ease: "power2.out",
        onUpdate: () => { shadowDirty = true; },
      });
    }
  }

  function present(rec) {
    if (presenting) return;
    presenting = rec;
    gsap.killTweensOf(rec.mesh.position);
    const dir = tmpV.set(0, 0, -1).applyQuaternion(camera.quaternion);
    const p = camera.position.clone().addScaledVector(dir, 0.55);
    const q = camera.quaternion.clone();
    const st = { t: 0 };
    const p0 = rec.mesh.position.clone();
    const q0 = rec.mesh.quaternion.clone();
    gsap.to(st, {
      t: 1, duration: reducedMotion() ? 0 : 0.55, ease: "power3.out",
      onUpdate: () => {
        rec.mesh.position.lerpVectors(p0, p, st.t);
        rec.mesh.quaternion.slerpQuaternions(q0, q, st.t);
        shadowDirty = true;
      },
      onComplete: () => {
        openEditionSheet(rec.b.id);
        // ease it home underneath the sheet
        gsap.delayedCall(0.7, () => {
          if (!rec.dead && rec.home) {
            flyTo(rec, rec.home, 0);
          }
          presenting = null;
        });
      },
    });
  }

  /* ---------- render loop ---------- */

  function tick(t) {
    if (hoverEvent) {
      const e = hoverEvent;
      hoverEvent = null;
      setHover(pick(e));
    }

    // gentle auto-orbit after a few seconds of stillness
    if (!reducedMotion() && performance.now() - ctrl.lastInput > 7000) {
      ctrl.autoSpin = Math.min(1, ctrl.autoSpin + 0.004);
    } else {
      ctrl.autoSpin = 0;
    }
    ctrl.tTheta += 0.0009 * ctrl.autoSpin;

    applyCamera();
    tickDust(t);

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
    scene.background = new THREE.Color(0x0a0e16);
    scene.fog = new THREE.FogExp2(0x0a0e16, 0.05);

    camera = new THREE.PerspectiveCamera(55, 1, 0.05, 40);
    raycaster = new THREE.Raycaster();

    assets = buildRoom();
    bindControls(renderer.domElement);
    renderer.domElement.addEventListener("click", (e) => {
      if (dragDist > 8) return;   // that was an orbit, not a tap
      const rec = pick(e);
      if (rec) present(rec);
    });

    new ResizeObserver(resize).observe(worldEl);
    resize();

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

  async function enter() {
    setRunning(true);
    resize();
    if (loading) return;
    const first = !books;
    if (first) countEl.textContent = "opening the hall…";
    loading = true;
    let fresh;
    try {
      const data = await api("/api/books?status=library");
      fresh = data.items || [];
    } catch (err) {
      loading = false;
      if (first) countEl.textContent = `couldn't load the library — ${err.message}`;
      return;
    }
    loading = false;
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
