/* shelf.js — the "shelves" tab: a 3D virtual bookcase for the library.
   Books are grouped by genre / format / author and GSAP Flip animates them
   fluidly between groupings. Loads after app.js and reuses its globals
   (api, esc, authorsOf, openEditionSheet). */

(() => {
  "use strict";

  if (window.gsap && window.Flip) gsap.registerPlugin(Flip);

  /* ---------- config ---------- */

  const FORMAT_ORDER = ["hardcover", "paperback", "mass market", "special edition", "ebook", "audiobook", "other", "unknown"];

  // muted book-cloth colors that sit comfortably on the navy background
  const PALETTE = [
    "#6e3b3f", // oxblood
    "#7a4a33", // brick
    "#2f5d43", // forest
    "#55603a", // moss
    "#8a6d3b", // ochre
    "#3b5372", // slate blue
    "#2f5b5e", // deep teal
    "#5d3b5e", // plum
    "#4b3a63", // aubergine
    "#71543e", // leather
  ];

  const ENRICH_ROUNDS = 6;   // max backfill calls per visit
  const ENRICH_BATCH = 12;

  /* ---------- state ---------- */

  let books = null;          // cached library items (null until first load)
  let mode = "genre";
  let loading = false;
  let enriching = false;
  let enrichDone = false;
  let firstRender = true;
  let activeFlip = null;
  let lastPerRow = 0;
  let resizeTimer = 0;

  const nodes = new Map();   // edition id -> persistent .book3d element

  const caseEl = document.getElementById("bookcase");
  const countEl = document.getElementById("shelves-count");
  const viewEl = document.getElementById("view-shelves");

  const reducedMotion = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const canFlip = () => !!(window.gsap && window.Flip) && !reducedMotion();
  const viewVisible = () => !viewEl.classList.contains("hidden");

  /* ---------- covers ---------- */

  // upgrade cover urls to shelf resolution: open library -M -> -L,
  // google books thumbnails -> zoom=1 without the page-curl overlay.
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
      return u.toString();
    } catch {
      return raw;
    }
  }

  // warm the browser cache so regroup animations never pop in covers
  function preloadCovers() {
    for (const b of books) {
      const src = upgradeCover(b.cover_url);
      if (src) new Image().src = src;
    }
  }

  /* ---------- cloth tint ---------- */

  function hashStr(s) {
    let h = 0;
    for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
    return Math.abs(h);
  }

  function clothOf(b) {
    return PALETTE[hashStr(`${b.title || ""}|${authorsOf(b)}`) % PALETTE.length];
  }

  // multiply a #rrggbb color's channels by f (darken < 1 < lighten)
  function shade(hex, f) {
    const n = parseInt(hex.slice(1), 16);
    const ch = (x) => Math.max(0, Math.min(255, Math.round(x * f)));
    return `rgb(${ch(n >> 16)},${ch((n >> 8) & 255)},${ch(n & 255)})`;
  }

  /* ---------- book nodes ---------- */

  // one persistent DOM node per edition, moved (never recreated) between
  // shelves so Flip can track identity across regroups
  function nodeFor(b) {
    let el = nodes.get(b.id);
    if (el) return el;

    const th = Math.max(10, Math.min(30, (b.page_count || 448) / 28)); // spine thickness, px
    const cloth = clothOf(b);
    const title = b.title || "untitled";
    const authors = authorsOf(b);
    const cover = upgradeCover(b.cover_url);

    el = document.createElement("div");
    el.className = "book3d";
    el.dataset.id = b.id;
    el.style.setProperty("--th", `${th.toFixed(1)}px`);
    el.style.setProperty("--cloth", cloth);
    el.style.setProperty("--cloth-deep", shade(cloth, 0.55));
    el.style.setProperty("--cloth-lite", shade(cloth, 1.35));
    el.setAttribute("role", "button");
    el.setAttribute("tabindex", "0");
    el.setAttribute("aria-label", authors ? `${title} — ${authors}` : title);

    el.innerHTML = `
      <div class="book3d-inner">
        <div class="bf-top"></div>
        <div class="bf-spine"><span>${esc(title)}</span></div>
        <div class="bf-front${cover ? "" : " cloth"}">
          ${cover ? `<img class="bf-cover" src="${esc(cover)}" alt="" loading="lazy" decoding="async">` : ""}
          <div class="bf-cloth">
            <div class="bf-cloth-title">${esc(title)}</div>
            <div class="bf-cloth-rule"></div>
            <div class="bf-cloth-author">${esc(authors)}</div>
          </div>
        </div>
      </div>`;

    const img = el.querySelector(".bf-cover");
    if (img) {
      // broken cover -> swap to the cloth-bound front, keep the node
      img.addEventListener("error", () => el.querySelector(".bf-front").classList.add("cloth"), { once: true });
    }

    const open = () => openEditionSheet(b.id);
    el.addEventListener("click", open);
    el.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
    });

    nodes.set(b.id, el);
    return el;
  }

  /* ---------- grouping ---------- */

  function surnameOf(item) {
    const first = authorsOf(item).split(",")[0].trim();
    if (!first) return "~"; // sorts after real names
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

  // -> [{key, items}] in display order, items sorted for the mode
  function computeGroups() {
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
        if (ua !== ub) return ua ? 1 : -1; // uncategorized shelved last
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

  /* ---------- layout ---------- */

  // how many books fit per shelf row at the current viewport width
  function booksPerRow() {
    const cs = getComputedStyle(caseEl);
    const inner = caseEl.clientWidth - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight);
    const probe = document.createElement("div");
    probe.className = "book3d";
    probe.style.cssText = "position:absolute;visibility:hidden;pointer-events:none;";
    caseEl.appendChild(probe);
    const bookW = probe.offsetWidth || 72;
    probe.remove();
    const gap = parseFloat(cs.getPropertyValue("--shelf-gap")) || 8;
    return Math.max(2, Math.floor((inner + gap) / (bookW + gap)));
  }

  // tear down the shelf containers and re-seat every book node in its
  // new slot; the nodes themselves persist (see nodeFor)
  function build() {
    lastPerRow = booksPerRow();
    const groups = computeGroups();
    caseEl.textContent = "";
    const frag = document.createDocumentFragment();

    for (const g of groups) {
      const groupEl = document.createElement("div");
      groupEl.className = "shelf-group";

      const label = document.createElement("div");
      label.className = "shelf-label";
      label.dataset.flipId = `lbl:${g.key}`; // lets Flip glide a label whose group moved
      label.innerHTML = `<span class="slashes">//</span> ${esc(g.key)} <span class="shelf-n">${g.items.length}</span>`;
      groupEl.appendChild(label);

      for (let i = 0; i < g.items.length; i += lastPerRow) {
        const shelf = document.createElement("div");
        shelf.className = "shelf";
        const row = document.createElement("div");
        row.className = "shelf-books";
        for (const b of g.items.slice(i, i + lastPerRow)) row.appendChild(nodeFor(b));
        shelf.appendChild(row);
        groupEl.appendChild(shelf);
      }
      frag.appendChild(groupEl);
    }
    caseEl.appendChild(frag);

    const n = books.length, s = groups.length;
    countEl.textContent = `${n} book${n === 1 ? "" : "s"} · ${s} shel${s === 1 ? "f" : "ves"}`;
  }

  /* ---------- animation ---------- */

  function cascade() {
    gsap.from("#bookcase .book3d", {
      y: -10, opacity: 0, duration: 0.4, ease: "power2.out",
      stagger: { amount: 0.4 },
      clearProps: "transform,opacity",
    });
  }

  // the money shot: capture layout, rebuild, and let Flip animate every
  // book (and label) from its old slot to its new one
  function regroup(animate) {
    if (!books) return;
    if (!books.length) {
      caseEl.innerHTML = `<div class="empty">no books in the library yet — scan a barcode and they'll appear here, standing on their shelf.</div>`;
      countEl.textContent = "";
      return;
    }

    if (activeFlip) { activeFlip.progress(1).kill(); activeFlip = null; }

    if (!animate || !canFlip()) {
      build();
      if (firstRender && canFlip()) cascade();
      firstRender = false;
      return;
    }

    const state = Flip.getState(".book3d, .shelf-label", { props: "opacity" });
    build();
    activeFlip = Flip.from(state, {
      duration: 0.65,
      ease: "power3.inOut",
      absolute: true,
      nested: true,
      stagger: 0.004,
      onEnter: (els) => gsap.fromTo(els,
        { opacity: 0, y: -14 },
        { opacity: 1, y: 0, duration: 0.35, ease: "power2.out", clearProps: "y" }),
      onLeave: (els) => gsap.to(els, { opacity: 0, duration: 0.25 }),
      onComplete: () => { activeFlip = null; },
    });
    firstRender = false;
  }

  /* ---------- genre enrichment ---------- */

  // background backfill: ask the server to classify a few books per round,
  // fold the answers into the cache, and let them visibly migrate shelves
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
      enrichDone = true; // best effort — don't hammer a failing endpoint
    }
    enriching = false;
  }

  /* ---------- entry / events ---------- */

  async function enter() {
    if (loading) return;
    if (books) {
      regroup(false); // instant re-seat in case the viewport changed while hidden
      enrichGenres(); // resume backfill if earlier rounds didn't finish
      return;
    }
    loading = true;
    caseEl.innerHTML = `<div class="empty">loading the shelves…</div>`;
    countEl.textContent = "";
    try {
      const data = await api("/api/books?status=library");
      books = data.items || [];
    } catch (err) {
      loading = false;
      caseEl.innerHTML = `<div class="empty">couldn't load the library — ${esc(err.message)}</div>`;
      return;
    }
    loading = false;
    preloadCovers();
    regroup(false); // first build cascades in
    enrichGenres();
  }

  document.querySelectorAll("#shelf-mode .seg-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      if (btn.dataset.mode === mode) return;
      mode = btn.dataset.mode;
      document.querySelectorAll("#shelf-mode .seg-btn").forEach((b) => {
        const on = b === btn;
        b.classList.toggle("active", on);
        b.setAttribute("aria-selected", String(on));
      });
      regroup(true);
    });
  });

  window.addEventListener("resize", () => {
    if (!books || !books.length || !viewVisible()) return;
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      if (booksPerRow() !== lastPerRow) regroup(true); // re-chunk rows with the same fluid motion
    }, 160);
  });

  window.Shelf = { enter };
})();
