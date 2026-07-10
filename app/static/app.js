/* book-bot frontend. Single-page app: scan / search / library / wishlist,
   with a bottom sheet for book details and add/manage actions. */

const FORMATS = ["hardcover", "paperback", "mass market", "special edition", "ebook", "audiobook", "other"];

const $ = (sel) => document.querySelector(sel);

let token = localStorage.getItem("bookbot_token") || "";
let currentView = "scan";
let listCache = { library: [], wishlist: [] };
let sheetOnClose = null;

/* ---------- api ---------- */

async function api(path, opts = {}) {
  const resp = await fetch(path, {
    ...opts,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(opts.headers || {}),
    },
  });
  if (resp.status === 401) {
    showLogin("session expired — log in again");
    throw new Error("unauthorized");
  }
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data.detail || `error ${resp.status}`);
  return data;
}

/* ---------- helpers ---------- */

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

function toast(msg, cls = "") {
  const el = $("#toast");
  el.textContent = msg;
  el.className = `toast ${cls}`;
  clearTimeout(el._timer);
  el._timer = setTimeout(() => el.classList.add("hidden"), 2600);
}

function coverImg(url, big = false) {
  if (url) return `<img class="cover" src="${esc(url)}" alt="" loading="lazy">`;
  return `<div class="cover-fallback">▤</div>`;
}
// swap broken cover images for the fallback glyph
document.addEventListener("error", (e) => {
  if (e.target.tagName === "IMG" && e.target.classList.contains("cover")) {
    const fb = document.createElement("div");
    fb.className = "cover-fallback";
    fb.textContent = "▤";
    e.target.replaceWith(fb);
  }
}, true);

function fmtDate(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return isNaN(d) ? "" : d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}

function authorsOf(x) {
  if (Array.isArray(x.authors)) return x.authors.join(", ");
  return x.authors || "";
}

function statusChip(status) {
  return status === "library"
    ? `<span class="chip ok">✓ library</span>`
    : `<span class="chip warn">✩ wishlist</span>`;
}

/* ---------- login ---------- */

function showLogin(err) {
  token = "";
  localStorage.removeItem("bookbot_token");
  Scanner.stop();
  $("#app").classList.add("hidden");
  $("#login-screen").classList.remove("hidden");
  if (err) {
    const box = $("#login-error");
    box.textContent = err;
    box.classList.remove("hidden");
  }
}

function showApp() {
  $("#login-screen").classList.add("hidden");
  $("#app").classList.remove("hidden");
  refreshStats();
  switchView(currentView);
}

$("#login-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const box = $("#login-error");
  box.classList.add("hidden");
  try {
    const data = await api("/api/login", {
      method: "POST",
      body: JSON.stringify({
        username: $("#login-username").value,
        password: $("#login-password").value,
      }),
    });
    token = data.token;
    localStorage.setItem("bookbot_token", token);
    $("#login-password").value = "";
    showApp();
  } catch (err) {
    box.textContent = err.message;
    box.classList.remove("hidden");
  }
});

$("#logout-btn").addEventListener("click", () => showLogin());

/* ---------- nav ---------- */

function switchView(name) {
  currentView = name;
  document.querySelectorAll(".view").forEach((v) => v.classList.add("hidden"));
  $(`#view-${name}`).classList.remove("hidden");
  document.querySelectorAll(".nav-btn").forEach((b) => b.classList.toggle("active", b.dataset.view === name));
  if (name !== "scan" && Scanner.isRunning()) stopScanner();
  if (name === "library") loadList("library");
  if (name === "wishlist") loadList("wishlist");
}

document.querySelectorAll(".nav-btn").forEach((btn) =>
  btn.addEventListener("click", () => switchView(btn.dataset.view))
);

async function refreshStats() {
  try {
    const s = await api("/api/stats");
    $("#stat-line").textContent = `${s.library} owned · ${s.wishlist} wished`;
  } catch { /* non-fatal */ }
}

/* ---------- scanning ---------- */

async function startScanner() {
  const status = $("#scan-status");
  status.textContent = "";
  try {
    $("#scanner-wrap").classList.remove("hidden");
    $("#scan-idle").classList.add("hidden");
    $("#scan-stop").classList.remove("hidden");
    await Scanner.start($("#scanner-video"), onBarcode);
    status.textContent = "looking for a barcode…";
  } catch (err) {
    stopScanner();
    status.textContent = `camera unavailable: ${err.message}. type the isbn below instead.`;
  }
}

function stopScanner() {
  Scanner.stop();
  $("#scanner-wrap").classList.add("hidden");
  $("#scan-stop").classList.add("hidden");
  $("#scan-idle").classList.remove("hidden");
  $("#scan-status").textContent = "";
}

$("#scan-start").addEventListener("click", startScanner);
$("#scan-stop").addEventListener("click", stopScanner);

async function onBarcode(text) {
  $("#scan-status").textContent = `read ${text} — looking it up…`;
  await lookupCode(text, { resumeScannerOnClose: true });
}

$("#manual-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const code = $("#manual-code").value.trim();
  if (code) await lookupCode(code, {});
});

async function lookupCode(code, { resumeScannerOnClose = false } = {}) {
  try {
    const data = await api(`/api/lookup?code=${encodeURIComponent(code)}`);
    if (!data.ok) {
      toast(data.reason, "err");
      if (resumeScannerOnClose) setTimeout(() => Scanner.resume(), 1600);
      return;
    }
    $("#scan-status").textContent = "";
    if (!data.found) {
      toast(`isbn ${data.isbn13} wasn't found in google books or open library — try a title search`, "err");
      if (resumeScannerOnClose) setTimeout(() => Scanner.resume(), 2200);
      return;
    }
    openBookSheet(data.metadata, data.ownership, resumeScannerOnClose ? () => Scanner.resume() : null);
  } catch (err) {
    toast(err.message, "err");
    if (resumeScannerOnClose) Scanner.resume();
  }
}

/* ---------- bottom sheet ---------- */

function openSheet(html, onClose) {
  sheetOnClose = onClose || null;
  $("#sheet").innerHTML = `<div class="sheet-grab" title="close"></div>${html}`;
  $("#sheet .sheet-grab").addEventListener("click", closeSheet);
  $("#sheet").classList.remove("hidden");
  $("#sheet-backdrop").classList.remove("hidden");
}

function closeSheet() {
  $("#sheet").classList.add("hidden");
  $("#sheet-backdrop").classList.add("hidden");
  if (sheetOnClose) { sheetOnClose(); sheetOnClose = null; }
}

$("#sheet-backdrop").addEventListener("click", closeSheet);

function ownershipBanner(ownership, meta) {
  const { exact, related } = ownership;
  const relatedList = related.length
    ? `<ul>${related.map((e) =>
        `<li>${esc(e.format || "unknown format")} — ${e.status === "library" ? "in library" : "on wishlist"}${e.isbn13 ? ` (${esc(e.isbn13)})` : ""}</li>`
      ).join("")}</ul>`
    : "";
  if (exact) {
    const cls = exact.status === "library" ? "ok" : "warn";
    const label = exact.status === "library"
      ? `✓ this exact edition is in the library${exact.format ? ` (${esc(exact.format)})` : ""}`
      : `✩ this exact edition is on the wishlist`;
    const more = related.length ? `<div>other editions you have:</div>${relatedList}` : "";
    return `<div class="own-banner ${cls}">${label}${more}</div>`;
  }
  if (related.length) {
    return `<div class="own-banner warn">≈ not this edition, but you have this book:${relatedList}</div>`;
  }
  return `<div class="own-banner none">not in the library or wishlist yet</div>`;
}

function formatSelect(id, selected) {
  const opts = [`<option value="">format — unknown</option>`]
    .concat(FORMATS.map((f) => `<option value="${f}" ${f === selected ? "selected" : ""}>${f}</option>`));
  return `<select id="${id}">${opts.join("")}</select>`;
}

function sheetHead(meta) {
  const bits = [
    authorsOf(meta) && esc(authorsOf(meta)),
  ].filter(Boolean).join("");
  const sub2 = [
    meta.publisher && esc(meta.publisher),
    meta.published_date && esc(meta.published_date),
    meta.isbn13 && `isbn ${esc(meta.isbn13)}`,
  ].filter(Boolean).join(" · ");
  return `
    <div class="sheet-head">
      ${coverImg(meta.cover_url, true)}
      <div>
        <div class="sheet-title">${esc(meta.title)}${meta.subtitle ? `: ${esc(meta.subtitle)}` : ""}</div>
        <div class="sheet-sub">${bits}</div>
        <div class="sheet-sub2">${sub2}</div>
      </div>
    </div>`;
}

function descBlock(meta) {
  if (!meta.description) return "";
  return `
    <div class="desc" id="sheet-desc">${esc(meta.description)}</div>
    <button class="link-btn desc-toggle" onclick="document.getElementById('sheet-desc').classList.toggle('open'); this.textContent = this.textContent === 'more' ? 'less' : 'more'">more</button>`;
}

/* add-mode sheet: a book found by scan/search that may or may not be owned */
function openBookSheet(meta, ownership, onClose) {
  const exact = ownership.exact;
  let actions;
  if (exact) {
    actions = `
      <div class="btnrow">
        <button class="btn secondary" id="sheet-flip">${exact.status === "library" ? "move to wishlist" : "move to library"}</button>
        <button class="btn danger" id="sheet-delete">remove</button>
      </div>`;
  } else {
    actions = `
      <label class="field"><span>edition format</span>${formatSelect("sheet-format", meta.format)}</label>
      <div class="btnrow">
        <button class="btn primary" id="sheet-add-library">+ library</button>
        <button class="btn secondary" id="sheet-add-wishlist">+ wishlist</button>
      </div>`;
  }
  openSheet(`
    ${sheetHead(meta)}
    ${ownershipBanner(ownership, meta)}
    <div class="sheet-actions">${actions}</div>
    ${descBlock(meta)}
  `, onClose);

  if (exact) {
    $("#sheet-flip").addEventListener("click", async () => {
      const next = exact.status === "library" ? "wishlist" : "library";
      await patchEdition(exact.id, { status: next }, `moved to ${next}`);
      closeSheet();
    });
    bindDelete($("#sheet-delete"), exact.id);
  } else {
    const add = (status) => async () => {
      try {
        const fmt = $("#sheet-format").value || null;
        const res = await api("/api/books", {
          method: "POST",
          body: JSON.stringify({ status, metadata: meta, format: fmt }),
        });
        toast(res.existed ? `already saved — status set to ${status}` : `added to ${status} ✓`, "ok");
        refreshStats();
        listCache = { library: [], wishlist: [] };
        closeSheet();
      } catch (err) { toast(err.message, "err"); }
    };
    $("#sheet-add-library").addEventListener("click", add("library"));
    $("#sheet-add-wishlist").addEventListener("click", add("wishlist"));
  }
}

/* manage-mode sheet: an edition already in the database */
async function openEditionSheet(editionId) {
  let data;
  try {
    data = await api(`/api/books/${editionId}`);
  } catch (err) { toast(err.message, "err"); return; }
  const ed = data.edition;
  const meta = { ...ed, authors: ed.authors };
  const related = data.related;
  const relatedHtml = related.length
    ? `<div class="own-banner warn">other editions of this book you track:<ul>${related.map((e) =>
        `<li>${esc(e.format || "unknown format")} — ${e.status === "library" ? "in library" : "on wishlist"}</li>`).join("")}</ul></div>`
    : "";
  openSheet(`
    ${sheetHead(meta)}
    <div class="own-banner ${ed.status === "library" ? "ok" : "warn"}">
      ${ed.status === "library" ? "✓ in library" : "✩ on wishlist"} since ${fmtDate(ed.status_changed_at)}${ed.copies > 1 ? ` · ${ed.copies} copies` : ""}
    </div>
    ${relatedHtml}
    <div class="sheet-actions">
      <label class="field"><span>edition format</span>${formatSelect("edit-format", ed.format)}</label>
      <label class="field"><span>notes</span><textarea id="edit-notes" rows="2" placeholder="signed copy, loaned to mom, …">${esc(ed.notes || "")}</textarea></label>
      <div class="btnrow">
        <button class="btn primary" id="edit-save">save</button>
        <button class="btn secondary" id="edit-flip">${ed.status === "library" ? "move to wishlist" : "move to library"}</button>
      </div>
      <div class="btnrow">
        <button class="btn danger" id="edit-delete">remove from ${ed.status}</button>
      </div>
    </div>
    ${descBlock(ed)}
  `);

  $("#edit-save").addEventListener("click", async () => {
    await patchEdition(ed.id, {
      format: $("#edit-format").value || null,
      notes: $("#edit-notes").value,
    }, "saved ✓");
    closeSheet();
  });
  $("#edit-flip").addEventListener("click", async () => {
    const next = ed.status === "library" ? "wishlist" : "library";
    await patchEdition(ed.id, { status: next }, `moved to ${next}`);
    closeSheet();
  });
  bindDelete($("#edit-delete"), ed.id);
}

function bindDelete(btn, editionId) {
  btn.addEventListener("click", async () => {
    if (!btn.dataset.armed) {
      btn.dataset.armed = "1";
      btn.textContent = "tap again to confirm removal";
      setTimeout(() => { btn.dataset.armed = ""; btn.textContent = "remove"; }, 3000);
      return;
    }
    try {
      await api(`/api/books/${editionId}`, { method: "DELETE" });
      toast("removed", "ok");
      refreshStats();
      listCache = { library: [], wishlist: [] };
      closeSheet();
      if (currentView === "library" || currentView === "wishlist") loadList(currentView);
    } catch (err) { toast(err.message, "err"); }
  });
}

async function patchEdition(id, fields, okMsg) {
  try {
    await api(`/api/books/${id}`, { method: "PATCH", body: JSON.stringify(fields) });
    toast(okMsg, "ok");
    refreshStats();
    listCache = { library: [], wishlist: [] };
    if (currentView === "library" || currentView === "wishlist") loadList(currentView);
  } catch (err) { toast(err.message, "err"); }
}

/* ---------- search ---------- */

$("#search-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const q = $("#search-input").value.trim();
  if (!q) return;
  const box = $("#search-results");
  box.innerHTML = `<div class="empty">searching…</div>`;
  try {
    const data = await api(`/api/search?q=${encodeURIComponent(q)}`);
    let html = "";
    if (data.local.length) {
      html += `<div class="result-group-label"><span class="slashes">//</span> your books</div>`;
      html += data.local.map((ed) => bookCardHtml(ed, "local")).join("");
    }
    html += `<div class="result-group-label"><span class="slashes">//</span> book catalog</div>`;
    html += data.external.length
      ? data.external.map((m, i) => bookCardHtml(m, "external", i)).join("")
      : `<div class="empty">no results</div>`;
    box.innerHTML = html;
    bindCards(box, data);
  } catch (err) {
    box.innerHTML = `<div class="empty">${esc(err.message)}</div>`;
  }
});

function bookCardHtml(item, kind, idx) {
  const chips = [];
  if (kind === "local") {
    chips.push(statusChip(item.status));
    if (item.format) chips.push(`<span class="chip">${esc(item.format)}</span>`);
  } else {
    if (item.owned_exact) chips.push(`<span class="chip ok">✓ you have this edition</span>`);
    else if ((item.owned_editions || []).length) {
      const owned = item.owned_editions.map((e) => `${e.format || "?"}${e.status === "wishlist" ? " ✩" : ""}`).join(", ");
      chips.push(`<span class="chip warn">≈ you have: ${esc(owned)}</span>`);
    }
    if (item.isbn13) chips.push(`<span class="chip">${esc(item.isbn13)}</span>`);
  }
  const sub = kind === "local"
    ? `added ${fmtDate(item.added_at)}`
    : [item.publisher, item.published_date].filter(Boolean).map(esc).join(" · ");
  return `
    <div class="book-card" data-kind="${kind}" data-id="${kind === "local" ? esc(item.id) : idx}">
      ${coverImg(item.cover_url)}
      <div class="meta">
        <div class="title">${esc(item.title)}</div>
        <div class="authors">${esc(authorsOf(item))}</div>
        <div class="sub">${sub}</div>
        <div class="chips">${chips.join("")}</div>
      </div>
    </div>`;
}

function bindCards(box, data) {
  box.querySelectorAll(".book-card").forEach((card) => {
    card.addEventListener("click", async () => {
      if (card.dataset.kind === "local") {
        openEditionSheet(card.dataset.id);
      } else {
        const meta = data.external[Number(card.dataset.id)];
        if (meta.isbn13) {
          // full lookup gives authoritative ownership + open library work link
          await lookupCode(meta.isbn13, {});
        } else {
          openBookSheet(meta, {
            exact: null,
            related: (meta.owned_editions || []).map((e) => ({ ...e })),
            work: null,
          });
        }
      }
    });
  });
}

/* ---------- library / wishlist ---------- */

async function loadList(status) {
  const box = $(`#${status === "library" ? "library" : "wishlist"}-list`);
  if (!listCache[status].length) {
    box.innerHTML = `<div class="empty">loading…</div>`;
    try {
      const data = await api(`/api/books?status=${status}`);
      listCache[status] = data.items;
    } catch (err) {
      box.innerHTML = `<div class="empty">${esc(err.message)}</div>`;
      return;
    }
  }
  renderList(status);
}

function renderList(status) {
  const box = $(`#${status === "library" ? "library" : "wishlist"}-list`);
  const filter = $(`#${status === "library" ? "library" : "wishlist"}-filter`).value.trim().toLowerCase();
  let items = listCache[status];
  if (filter) {
    items = items.filter((ed) =>
      (ed.title || "").toLowerCase().includes(filter) ||
      (ed.authors || "").toLowerCase().includes(filter) ||
      (ed.isbn13 || "").includes(filter));
  }
  if (!items.length) {
    box.innerHTML = `<div class="empty">${filter ? "nothing matches that filter" : status === "library" ? "no books yet — scan a barcode to start shelving" : "wishlist is empty — scan or search while shopping"}</div>`;
    return;
  }
  box.innerHTML = items.map((ed) => bookCardHtml(ed, "local")).join("");
  box.querySelectorAll(".book-card").forEach((card) =>
    card.addEventListener("click", () => openEditionSheet(card.dataset.id)));
}

$("#library-filter").addEventListener("input", () => renderList("library"));
$("#wishlist-filter").addEventListener("input", () => renderList("wishlist"));

/* ---------- boot ---------- */

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("sw.js").catch(() => {});
}

if (token) {
  api("/api/stats").then(showApp).catch(() => {
    if ($("#app").classList.contains("hidden")) showLogin();
  });
} else {
  showLogin();
}
