/* book-bot frontend. Single-page app: scan / search / library / wishlist /
   reading history, with a bottom sheet for book details, add/manage actions
   and per-user reading status. Ownership lives in a library (shareable with
   other users); reading history is always personal. */

const FORMATS = ["hardcover", "paperback", "mass market", "special edition", "ebook", "audiobook", "other"];
const READ_LABELS = { want_to_read: "✩ want to read", reading: "◉ reading", read: "✓ read" };
const HOLD_CHIPS = { library: "✓ library", digital: "⌁ digital", wishlist: "✩ wishlist" };
const HOLD_PHRASES = { library: "in library", digital: "owned digitally", wishlist: "on wishlist" };

const $ = (sel) => document.querySelector(sel);

let token = localStorage.getItem("bookbot_token") || "";
let me = null; // {user_id, username, libraries: [{id, name, role, members}]}
let activeLibraryId = localStorage.getItem("bookbot_library") || "";
let currentView = "scan";
let listCache = { library: [], wishlist: [] };
let readCache = null;
let readFilter = "";
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

function coverImg(url) {
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
  // date-only values (read dates) must not shift a day across timezones
  const d = /^\d{4}-\d{2}-\d{2}$/.test(iso) ? new Date(`${iso}T00:00:00`) : new Date(iso);
  return isNaN(d) ? "" : d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}

function authorsOf(x) {
  if (Array.isArray(x.authors)) return x.authors.join(", ");
  return x.authors || "";
}

function statusChip(status) {
  const cls = { library: "ok", digital: "dig", wishlist: "warn" }[status] || "";
  return `<span class="chip ${cls}">${HOLD_CHIPS[status] || esc(status)}</span>`;
}

function readChip(status) {
  if (!status) return "";
  const cls = status === "read" ? "ok" : "";
  return `<span class="chip ${cls}">${READ_LABELS[status]}</span>`;
}

function invalidateBooks() {
  listCache = { library: [], wishlist: [] };
  readCache = null;
}

/* ---------- me / libraries ---------- */

async function loadMe() {
  me = await api("/api/me");
  const ids = me.libraries.map((l) => l.id);
  if (!ids.includes(activeLibraryId)) activeLibraryId = ids[0] || "";
  localStorage.setItem("bookbot_library", activeLibraryId);
  renderLibraryButton();
  return me;
}

function activeLibrary() {
  return me?.libraries.find((l) => l.id === activeLibraryId) || me?.libraries[0] || null;
}

// where adds land: the library currently being browsed
function targetLibraryId() {
  return activeLibraryId || null;
}

/* ---------- hooks for the shelves views (shelf.js / shelf3d.js) ---------- */

// query-string suffix scoping shelf loads to the active library
function shelfLibraryScope() {
  return activeLibraryId ? `&library_id=${encodeURIComponent(activeLibraryId)}` : "";
}

// route a shelf book tap to the manage sheet
function openShelfBook(item) {
  openEditionSheet(item.id);
}

function renderLibraryButton() {
  const lib = activeLibrary();
  const extra = me && me.libraries.length > 1 ? " ▾" : "";
  const label = lib ? `▤ ${lib.name}${extra}` : "▤ …";
  $("#library-btn").textContent = label;
  $("#shelves-lib-btn").textContent = label;
}

function openLibrarySheet() {
  const lib = activeLibrary();
  if (!lib) return;
  const rows = me.libraries.map((l) => `
    <div class="lib-row ${l.id === lib.id ? "active" : ""}" data-lib="${esc(l.id)}">
      <div class="lib-name">▤ ${esc(l.name)}</div>
      <div class="lib-members">${
        l.members.map((m) => esc(m.username || "?")).join(", ") || "just you"}</div>
    </div>`).join("");
  const manage = `<label class="field"><span>rename "${esc(lib.name)}"</span>
        <input id="lib-rename" value="${esc(lib.name)}"></label>
      <label class="field"><span>share "${esc(lib.name)}" with (username)</span>
        <input id="lib-invite" autocapitalize="none" placeholder="their book-bot username"></label>
      <div class="btnrow">
        <button class="btn secondary" id="lib-rename-save">rename</button>
        <button class="btn primary" id="lib-invite-btn">add member</button>
      </div>
      <div class="btnrow">
        <button class="btn secondary" id="lib-create">+ start another library</button>
      </div>`;
  openSheet(`
    <div class="sheet-title">my libraries</div>
    <div class="lib-list">${rows}</div>
    <div class="sheet-actions">${manage}</div>`);

  document.querySelectorAll(".lib-row").forEach((row) =>
    row.addEventListener("click", () => {
      activeLibraryId = row.dataset.lib;
      localStorage.setItem("bookbot_library", activeLibraryId);
      invalidateBooks();
      renderLibraryButton();
      closeSheet();
      if (currentView === "library" || currentView === "wishlist") loadList(currentView);
      if (currentView === "shelves") Shelf.enter();
      refreshStats();
    }));
  $("#lib-rename-save")?.addEventListener("click", async () => {
    const name = $("#lib-rename").value.trim();
    if (!name || name === lib.name) return;
    try {
      await api(`/api/libraries/${lib.id}`, { method: "PATCH", body: JSON.stringify({ name }) });
      await loadMe();
      toast("renamed ✓", "ok");
      closeSheet();
    } catch (err) { toast(err.message, "err"); }
  });
  $("#lib-invite-btn")?.addEventListener("click", async () => {
    const username = $("#lib-invite").value.trim();
    if (!username) return;
    try {
      const res = await api(`/api/libraries/${lib.id}/members`, {
        method: "POST", body: JSON.stringify({ username }),
      });
      await loadMe();
      toast(`${res.added} can now see this library ✓`, "ok");
      closeSheet();
    } catch (err) { toast(err.message, "err"); }
  });
  $("#lib-create").addEventListener("click", async () => {
    const name = prompt("name for the new library:", "");
    if (!name || !name.trim()) return;
    try {
      const res = await api("/api/libraries", { method: "POST", body: JSON.stringify({ name: name.trim() }) });
      activeLibraryId = res.library.id;
      localStorage.setItem("bookbot_library", activeLibraryId);
      invalidateBooks();
      await loadMe();
      toast("library created ✓", "ok");
      closeSheet();
      if (currentView === "library" || currentView === "wishlist") loadList(currentView);
    } catch (err) { toast(err.message, "err"); }
  });
}

/* ---------- login ---------- */

function showLogin(err) {
  token = "";
  me = null;
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

let authMode = "login"; // 'login' | 'signup'

$("#auth-toggle").addEventListener("click", () => {
  authMode = authMode === "login" ? "signup" : "login";
  const signup = authMode === "signup";
  $("#signup-confirm-field").classList.toggle("hidden", !signup);
  $("#login-submit").textContent = signup ? "create account" : "log in";
  $("#auth-toggle").textContent = signup
    ? "already have an account? log in"
    : "new here? create an account";
  $("#login-password").autocomplete = signup ? "new-password" : "current-password";
  $("#login-error").classList.add("hidden");
});

$("#login-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const box = $("#login-error");
  box.classList.add("hidden");
  const username = $("#login-username").value;
  const password = $("#login-password").value;
  try {
    if (authMode === "signup") {
      if (password !== $("#signup-confirm").value) throw new Error("passwords don't match");
      $("#login-submit").disabled = true;
      $("#login-submit").textContent = "setting up your library…";
    }
    const data = await api(authMode === "signup" ? "/api/signup" : "/api/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });
    token = data.token;
    localStorage.setItem("bookbot_token", token);
    $("#login-password").value = "";
    $("#signup-confirm").value = "";
    invalidateBooks();
    await loadMe();
    showApp();
    if (authMode === "signup") {
      toast("welcome! scan a barcode to start shelving", "ok");
      authMode = "login";
    }
  } catch (err) {
    box.textContent = err.message;
    box.classList.remove("hidden");
  } finally {
    $("#login-submit").disabled = false;
    $("#login-submit").textContent = authMode === "signup" ? "create account" : "log in";
  }
});

$("#logout-btn").addEventListener("click", () => showLogin());

/* ---------- nav ---------- */

function switchView(name) {
  currentView = name;
  $("#app").classList.toggle("lib-wide", name === "shelves"); // shelves get room on desktop
  document.querySelectorAll(".view").forEach((v) => v.classList.add("hidden"));
  $(`#view-${name}`).classList.remove("hidden");
  document.querySelectorAll(".nav-btn").forEach((b) => b.classList.toggle("active", b.dataset.view === name));
  if (name !== "scan" && Scanner.isRunning()) stopScanner();
  if (name === "library") loadList("library");
  if (name === "wishlist") loadList("wishlist");
  if (name === "read") loadReads();
  if (name === "shelves") Shelf.enter();
}

document.querySelectorAll(".nav-btn").forEach((btn) =>
  btn.addEventListener("click", () => switchView(btn.dataset.view))
);

$("#library-btn").addEventListener("click", openLibrarySheet);
$("#shelves-lib-btn").addEventListener("click", openLibrarySheet);

async function refreshStats() {
  try {
    const s = await api("/api/stats");
    $("#stat-line").textContent = `${s.library + s.digital} owned · ${s.wishlist} wished · ${s.read} read`;
  } catch { /* non-fatal */ }
}

/* ---------- scanning ---------- */

async function startScanner(bulk = false) {
  const status = $("#scan-status");
  status.textContent = "";
  bulkMode = bulk;
  if (bulk) beep(null); // unlock audio inside the tap that started the session
  try {
    $("#scanner-wrap").classList.remove("hidden");
    $("#scan-idle").classList.add("hidden");
    $("#scan-stop").classList.remove("hidden");
    $("#scan-stop").textContent = bulk ? "done with this stack" : "stop camera";
    $(".scan-box").classList.toggle("bulk", bulk);
    await Scanner.start($("#scanner-video"), onBarcode);
    status.textContent = bulk ? "bulk add — scan the first book" : "looking for a barcode…";
  } catch (err) {
    stopScanner();
    status.textContent = `camera unavailable: ${err.message}. type the isbn or upc below instead.`;
  }
}

function stopScanner() {
  Scanner.stop();
  bulkMode = false;
  $(".scan-box").classList.remove("bulk");
  $("#bulk-card").classList.add("hidden");
  $("#scanner-wrap").classList.add("hidden");
  $("#scan-stop").classList.add("hidden");
  $("#scan-idle").classList.remove("hidden");
  $("#scan-status").textContent = "";
}

$("#scan-start").addEventListener("click", () => startScanner());
$("#bulk-start").addEventListener("click", () => startScanner(true));
$("#scan-stop").addEventListener("click", stopScanner);

async function onBarcode(text) {
  if (bulkMode) { await bulkScan(text); return; }
  $("#scan-status").textContent = `read ${text} — looking it up…`;
  await lookupCode(text, { resumeScannerOnClose: true });
}

$("#goto-search").addEventListener("click", () => {
  switchView("search");
  $("#search-input").focus();
});

// returns true when the sheet opened, so the unified search box knows
// whether to fall back to a plain search
async function lookupCode(code, { resumeScannerOnClose = false, quiet = false } = {}) {
  try {
    const data = await api(`/api/lookup?code=${encodeURIComponent(code)}`);
    if (!data.ok) {
      if (!quiet) toast(data.reason, "err");
      if (resumeScannerOnClose) setTimeout(() => Scanner.resume(), 1600);
      return false;
    }
    $("#scan-status").textContent = "";
    if (!data.found) {
      const what = data.isbn13 ? `isbn ${data.isbn13}` : `barcode ${code}`;
      if (!quiet) toast(`${what} wasn't found in google books or open library — try a title search`, "err");
      if (resumeScannerOnClose) setTimeout(() => Scanner.resume(), 2200);
      return false;
    }
    openBookSheet(data.metadata, data.ownership, resumeScannerOnClose ? () => Scanner.resume() : null);
    return true;
  } catch (err) {
    toast(err.message, "err");
    if (resumeScannerOnClose) Scanner.resume();
    return false;
  }
}

/* ---------- bulk add ----------
   Shelving a stack: scan, one tap for the binding, next book — no sheet.
   A book that doesn't go in stops the scanner with a low double tone while
   it is still in hand, and stays in the session list at its place in the
   stack until it's dealt with. The list survives a reload. */

const BULK_FORMATS = ["hardcover", "paperback", "mass market", "special edition"];
let bulkMode = false;
let bulkSession = [];
try { bulkSession = JSON.parse(localStorage.getItem("bookbot_bulk") || "[]"); } catch { /* start empty */ }
let audioCtx = null;

// ok=true: one short high blip. ok=false: two low tones. ok=null: silent
// (creates the context inside a user gesture, which iOS requires)
function beep(ok) {
  try {
    audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
    if (audioCtx.state === "suspended") audioCtx.resume();
    if (ok === null) return;
    const tone = (freq, at, len) => {
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.frequency.value = freq;
      gain.gain.value = 0.25;
      osc.connect(gain).connect(audioCtx.destination);
      osc.start(audioCtx.currentTime + at);
      osc.stop(audioCtx.currentTime + at + len);
    };
    if (ok) tone(880, 0, 0.09);
    else { tone(196, 0, 0.25); tone(196, 0.33, 0.25); }
  } catch { /* no audio — the card still says it */ }
  if (!ok && navigator.vibrate) navigator.vibrate([200, 100, 200]);
}

function bulkLog(entry) {
  const n = bulkSession.length ? bulkSession[bulkSession.length - 1].n + 1 : 1;
  bulkSession.push({ n, ...entry });
  bulkSave();
}

function bulkSave() {
  localStorage.setItem("bookbot_bulk", JSON.stringify(bulkSession));
  renderBulkSession();
}

// the nearest book that did go in, before (dir -1) or after (dir 1) a miss
function bulkNeighbor(idx, dir) {
  for (let i = idx + dir; i >= 0 && i < bulkSession.length; i += dir) {
    if (bulkSession[i].kind !== "miss") return bulkSession[i].title;
  }
  return "";
}

function bulkWhere(idx) {
  const before = bulkNeighbor(idx, -1);
  const after = bulkNeighbor(idx, 1);
  if (before && after) return `between “${before}” and “${after}”`;
  if (before) return `right after “${before}”`;
  if (after) return `right before “${after}”`;
  return "";
}

function renderBulkSession() {
  $("#bulk-session").classList.toggle("hidden", !bulkSession.length);
  const misses = bulkSession.filter((e) => e.kind === "miss").length;
  const added = bulkSession.length - misses;
  $("#bulk-count").innerHTML = `${added} added`
    + (misses ? ` · <span class="bulk-miss-count">${misses} set aside</span>` : "");
  $("#bulk-list").innerHTML = bulkSession.map((e, idx) => {
    if (e.kind === "miss") {
      return `
        <div class="bulk-row miss">
          <span class="n">✗</span>
          <span class="t">not added — barcode ${esc(e.code)}
            <small>${esc(bulkWhere(idx) || e.reason)}</small></span>
          <button type="button" class="link-btn" data-search="${e.n}">search</button>
          <button type="button" class="link-btn" data-done="${e.n}">done</button>
        </div>`;
    }
    return `
      <button type="button" class="bulk-row" data-book="${esc(e.bookId)}">
        <span class="n">${e.n}</span>
        <span class="t">${esc(e.title)}${e.kind === "copy" ? " <small>another copy</small>" : ""}</span>
        <span class="chip">${esc(e.format || "format unknown")}</span>
      </button>`;
  }).reverse().join("");
}

$("#bulk-list").addEventListener("click", async (ev) => {
  const el = ev.target.closest("[data-book], [data-search], [data-done]");
  if (!el) return;
  if (el.dataset.done) {
    bulkSession = bulkSession.filter((e) => String(e.n) !== el.dataset.done);
    bulkSave();
  } else if (el.dataset.search) {
    switchView("search");
    $("#search-input").focus();
  } else {
    // fix a format, add a copy or remove it — the normal book sheet
    Scanner.pause();
    await openEditionSheet(el.dataset.book);
    if ($("#sheet").classList.contains("hidden")) bulkResume();
    else sheetOnClose = bulkResume;
  }
});

$("#bulk-clear").addEventListener("click", () => {
  const btn = $("#bulk-clear");
  if (!btn.dataset.armed) {
    btn.dataset.armed = "1";
    btn.textContent = "tap again — clears this list only, the books stay";
    setTimeout(() => { btn.dataset.armed = ""; btn.textContent = "clear list"; }, 3000);
    return;
  }
  btn.dataset.armed = "";
  btn.textContent = "clear list";
  bulkSession = [];
  bulkSave();
});

function bulkCard(html) {
  const card = $("#bulk-card");
  card.innerHTML = html;
  card.classList.remove("hidden");
  card.scrollIntoView({ block: "nearest", behavior: "smooth" });
}

// only while a card isn't waiting on a tap, so a book is never skipped unseen
function bulkResume() {
  if (bulkMode && $("#bulk-card").classList.contains("hidden")) Scanner.resume();
}

function bulkNext(code) {
  $("#bulk-card").classList.add("hidden");
  $("#scan-status").textContent = "next book…";
  if (code) Scanner.ignore(code, 4000);
  Scanner.resume();
}

async function bulkScan(code) {
  $("#scan-status").textContent = `read ${code} — looking it up…`;
  let data;
  try {
    data = await api(`/api/lookup?code=${encodeURIComponent(code)}`);
  } catch (err) {
    if (token) bulkMiss(code, `lookup failed: ${err.message}`);
    return;
  }
  $("#scan-status").textContent = "";
  if (!data.ok) { bulkMiss(code, data.reason); return; }
  if (!data.found) { bulkMiss(code, "not in google books or open library"); return; }
  bulkOffer(code, data.metadata, data.ownership);
}

function bulkMiss(code, reason) {
  beep(false);
  const last = bulkNeighbor(bulkSession.length, -1);
  bulkCard(`
    <div class="bulk-miss-title">✗ this book was not added</div>
    <div class="bulk-miss-why">${esc(reason)}</div>
    <div class="bulk-miss-why">barcode ${esc(code)}${last ? ` · it comes right after “${esc(last)}”` : ""}</div>
    <div class="btnrow">
      <button class="btn secondary" id="bulk-retry">scan it again</button>
      <button class="btn primary" id="bulk-aside">set it aside, keep going</button>
    </div>`);
  $("#bulk-retry").addEventListener("click", () => bulkNext(""));
  $("#bulk-aside").addEventListener("click", () => {
    bulkLog({ kind: "miss", code, reason });
    bulkNext(code);
  });
}

function bulkOffer(code, meta, ownership) {
  const exact = ownership.exact;
  const onShelf = exact && exact.status === "library";
  // a 12-digit upc is the old mass-market paperback barcode
  const guess = meta.format || (code.replace(/\D/g, "").length === 12 ? "mass market" : "");
  let note = "";
  if (onShelf) {
    const detail = [exact.format, exact.copies > 1 ? `×${exact.copies}` : ""].filter(Boolean).join(" ");
    note = `already in the library${detail ? ` (${esc(detail)})` : ""}`;
  } else if (exact) {
    note = `${HOLD_PHRASES[exact.status]} — a tap moves it to the library`;
  } else if (ownership.related.length) {
    note = `you have this book as: ${ownership.related.map((e) => esc(e.format || "unknown format")).join(", ")}`;
  }
  const actions = onShelf
    ? `<div class="btnrow">
         <button class="btn primary" id="bulk-copy">+ another copy</button>
         <button class="btn secondary" id="bulk-skip">skip</button>
       </div>`
    : `<div class="bulk-formats">
         ${BULK_FORMATS.map((f) => `<button class="btn ${f === guess ? "primary" : "secondary"}" data-fmt="${f}">${f}</button>`).join("")}
       </div>
       <button class="link-btn" id="bulk-skip">skip this one</button>`;
  bulkCard(`
    ${sheetHead(meta)}
    ${note ? `<div class="own-banner ${onShelf ? "ok" : "warn"}">${note}</div>` : ""}
    ${actions}`);

  $("#bulk-skip").addEventListener("click", () => bulkNext(code));
  if (onShelf) {
    $("#bulk-copy").addEventListener("click", async () => {
      try {
        await api(`/api/books/${exact.id}`, {
          method: "PATCH", body: JSON.stringify({ copies: (exact.copies || 1) + 1 }),
        });
      } catch (err) { toast(err.message, "err"); return; }
      bulkDone(code, { kind: "copy", title: meta.title, format: exact.format, bookId: exact.id, code });
    });
    return;
  }
  $("#bulk-card").querySelectorAll("[data-fmt]").forEach((btn) =>
    btn.addEventListener("click", async () => {
      const fmt = btn.dataset.fmt;
      let book;
      try {
        book = (await api("/api/books", {
          method: "POST",
          body: JSON.stringify({ status: "library", metadata: meta, format: fmt, library_id: targetLibraryId() }),
        })).book;
        // the catalog may already hold another format for this isbn; the
        // person holding the book wins
        if (book.format !== fmt) {
          await api(`/api/books/${book.id}`, { method: "PATCH", body: JSON.stringify({ format: fmt }) });
        }
      } catch (err) { toast(err.message, "err"); return; }
      bulkDone(code, { kind: "added", title: meta.title, format: fmt, bookId: book.id, code });
    }));
}

function bulkDone(code, entry) {
  beep(true);
  bulkLog(entry);
  refreshStats();
  invalidateBooks();
  bulkNext(code);
}

renderBulkSession();

/* ---------- bottom sheet ---------- */

function openSheet(html, onClose) {
  sheetOnClose = onClose || null;
  $("#sheet").innerHTML = `<div class="sheet-grab" title="close"></div>${html}`;
  $("#sheet .sheet-grab").addEventListener("click", closeSheet);
  const descToggle = $("#sheet-desc-toggle");
  if (descToggle) descToggle.addEventListener("click", () => {
    $("#sheet-desc").classList.toggle("open");
    descToggle.textContent = descToggle.textContent === "more" ? "less" : "more";
  });
  $("#sheet").classList.remove("hidden");
  $("#sheet-backdrop").classList.remove("hidden");
}

function closeSheet() {
  $("#sheet").classList.add("hidden");
  $("#sheet-backdrop").classList.add("hidden");
  if (sheetOnClose) { sheetOnClose(); sheetOnClose = null; }
}

$("#sheet-backdrop").addEventListener("click", closeSheet);

function libraryName(id) {
  const lib = me?.libraries.find((l) => l.id === id);
  return lib ? lib.name : "";
}

function holdingLine(e) {
  const where = me && me.libraries.length > 1 && e.library_id ? ` — ${esc(libraryName(e.library_id))}` : "";
  const copies = e.copies > 1 ? ` ×${e.copies}` : "";
  return `${esc(e.format || "unknown format")}${copies} — ${HOLD_PHRASES[e.status] || esc(e.status)}${e.isbn13 ? ` (${esc(e.isbn13)})` : ""}${where}`;
}

function ownershipBanner(ownership) {
  const { exact, related, read_state: rs } = ownership;
  const relatedList = related.length
    ? `<ul>${related.map((e) => `<li>${holdingLine(e)}</li>`).join("")}</ul>`
    : "";
  const readLine = rs
    ? `<div class="read-line">${READ_LABELS[rs.status]}${rs.finished_at ? ` · finished ${fmtDate(rs.finished_at)}` : ""}${rs.rating ? ` · ${"★".repeat(rs.rating)}` : ""}</div>`
    : "";
  if (exact) {
    const cls = { library: "ok", digital: "dig", wishlist: "warn" }[exact.status];
    const copies = exact.copies > 1 ? ` ×${exact.copies}` : "";
    const label = exact.status === "library"
      ? `✓ this exact edition is in ${esc(libraryName(exact.library_id) || "the library")}${exact.format ? ` (${esc(exact.format)}${copies})` : copies}`
      : exact.status === "digital"
        ? `⌁ you own this edition digitally`
        : `✩ this exact edition is on the wishlist`;
    const more = related.length ? `<div>other editions you have:</div>${relatedList}` : "";
    return `<div class="own-banner ${cls}">${label}${more}${readLine}</div>`;
  }
  if (related.length) {
    return `<div class="own-banner warn">≈ not this edition, but you have this book:${relatedList}${readLine}</div>`;
  }
  return `<div class="own-banner none">not in the library or wishlist yet${readLine}</div>`;
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
      ${coverImg(meta.cover_url)}
      <div>
        <div class="sheet-title">${esc(meta.title)}${meta.subtitle ? `: ${esc(meta.subtitle)}` : ""}</div>
        <div class="sheet-sub">${bits}</div>
        <div class="sheet-sub2">${sub2}</div>
      </div>
    </div>`;
}

function descBlock(meta) {
  if (!meta.description) return "";
  // no inline onclick: the CSP forbids inline script; openSheet binds it
  return `
    <div class="desc" id="sheet-desc">${esc(meta.description)}</div>
    <button class="link-btn desc-toggle" id="sheet-desc-toggle">more</button>`;
}

/* ---------- reading status section (shared by all sheets) ---------- */

function renderReadSection(container, rs, payloadBase) {
  const status = rs?.status || "";
  const rating = rs?.rating || 0;
  container.innerHTML = `
    <div class="result-group-label"><span class="slashes">//</span> my reading</div>
    <div class="btnrow read-status-row">
      ${Object.entries(READ_LABELS).map(([v, label]) =>
        `<button class="btn chip-toggle ${status === v ? "active" : ""}" data-read-status="${v}">${label}</button>`).join("")}
    </div>
    <div class="read-details ${status ? "" : "hidden"}">
      <div class="stars">${[1, 2, 3, 4, 5].map((n) =>
        `<button class="star ${n <= rating ? "on" : ""}" data-star="${n}" title="${n} star${n > 1 ? "s" : ""}">★</button>`).join("")}</div>
      <div class="daterow">
        <label class="field"><span>started</span><input type="date" id="read-started" value="${esc(rs?.started_at || "")}"></label>
        <label class="field"><span>finished</span><input type="date" id="read-finished" value="${esc(rs?.finished_at || "")}"></label>
      </div>
      <label class="field"><span>my notes (private)</span>
        <textarea id="read-notes" rows="2" placeholder="thoughts, favorite quotes…">${esc(rs?.notes || "")}</textarea></label>
      <div class="btnrow">
        <button class="btn primary" id="read-save">save reading</button>
        ${rs ? `<button class="btn danger" id="read-clear">clear</button>` : ""}
      </div>
    </div>`;

  let picked = status;
  let stars = rating;
  container.querySelectorAll("[data-read-status]").forEach((btn) =>
    btn.addEventListener("click", () => {
      picked = btn.dataset.readStatus;
      container.querySelectorAll("[data-read-status]").forEach((b) =>
        b.classList.toggle("active", b === btn));
      container.querySelector(".read-details").classList.remove("hidden");
    }));
  container.querySelectorAll(".star").forEach((btn) =>
    btn.addEventListener("click", () => {
      const n = Number(btn.dataset.star);
      stars = stars === n ? 0 : n;
      container.querySelectorAll(".star").forEach((b) =>
        b.classList.toggle("on", Number(b.dataset.star) <= stars));
    }));
  container.querySelector("#read-save").addEventListener("click", async () => {
    if (!picked) { toast("pick want to read / reading / read first", "err"); return; }
    try {
      const res = await api("/api/reads", {
        method: "POST",
        body: JSON.stringify({
          ...payloadBase,
          status: picked,
          rating: stars || null,
          notes: container.querySelector("#read-notes").value || null,
          started_at: container.querySelector("#read-started").value || null,
          finished_at: container.querySelector("#read-finished").value || null,
        }),
      });
      readCache = null;
      listCache = { library: [], wishlist: [] };
      refreshStats();
      if (currentView === "read") loadReads();
      toast("reading saved ✓", "ok");
      renderReadSection(container, res.read_state, { work_id: res.read_state.work_id });
    } catch (err) { toast(err.message, "err"); }
  });
  const clear = container.querySelector("#read-clear");
  if (clear) {
    clear.addEventListener("click", async () => {
      try {
        await api(`/api/reads/${rs.work_id}`, { method: "DELETE" });
        readCache = null;
        refreshStats();
        if (currentView === "read") loadReads();
        toast("cleared from reading history", "ok");
        renderReadSection(container, null, payloadBase);
      } catch (err) { toast(err.message, "err"); }
    });
  }
}

function statusSwitcher(current) {
  return `<div class="btnrow status-row">${Object.entries(HOLD_CHIPS).map(([v, label]) =>
    `<button class="btn chip-toggle ${v === current ? "active" : ""}" data-set-status="${v}">${label}</button>`).join("")}</div>`;
}

function bindStatusSwitcher(bookId, current) {
  document.querySelectorAll("#sheet [data-set-status]").forEach((btn) =>
    btn.addEventListener("click", async () => {
      const next = btn.dataset.setStatus;
      if (next === current) return;
      await patchBook(bookId, { status: next }, `moved to ${HOLD_PHRASES[next]}`);
      closeSheet();
    }));
}

/* add-mode sheet: a book found by scan/search that may or may not be owned */
// wire the "own a different edition too?" controls: a format picker plus
// +library / +digital / +wishlist, posting with new_edition so the server
// files it as a sibling edition of the same work
function bindAddEdition(prefix, meta) {
  const add = (status) => async () => {
    const fmt = $(`#${prefix}-format`).value;
    if (!fmt) { toast("pick the new edition's format first", "err"); return; }
    try {
      const res = await api("/api/books", {
        method: "POST",
        body: JSON.stringify({ status, metadata: meta, format: fmt, new_edition: true, library_id: targetLibraryId() }),
      });
      toast(res.existed ? `already tracked — now ${HOLD_PHRASES[status]}` : `${fmt} edition added — ${HOLD_PHRASES[status]} ✓`, "ok");
      refreshStats();
      invalidateBooks();
      closeSheet();
    } catch (err) { toast(err.message, "err"); }
  };
  $(`#${prefix}-library`).addEventListener("click", add("library"));
  $(`#${prefix}-digital`).addEventListener("click", add("digital"));
  $(`#${prefix}-wishlist`).addEventListener("click", add("wishlist"));
}

function openBookSheet(meta, ownership, onClose) {
  const exact = ownership.exact;
  let actions;
  if (exact) {
    actions = `
      ${statusSwitcher(exact.status)}
      <div class="btnrow">
        <button class="btn secondary" id="sheet-copy">+ another copy</button>
        <button class="btn danger" id="sheet-delete">remove</button>
      </div>
      <label class="field"><span>own a different edition too? pick its format</span>
        ${formatSelect("sheet-new-format", "")}</label>
      <div class="btnrow">
        <button class="btn secondary" id="sheet-new-library">+ library</button>
        <button class="btn secondary" id="sheet-new-digital">+ digital</button>
        <button class="btn secondary" id="sheet-new-wishlist">+ wishlist</button>
      </div>`;
  } else {
    actions = `
      <label class="field"><span>edition format</span>${formatSelect("sheet-format", meta.format)}</label>
      <div class="btnrow">
        <button class="btn primary" id="sheet-add-library">+ library</button>
        <button class="btn secondary" id="sheet-add-digital">+ digital</button>
        <button class="btn secondary" id="sheet-add-wishlist">+ wishlist</button>
      </div>`;
  }
  openSheet(`
    ${sheetHead(meta)}
    ${ownershipBanner(ownership)}
    <div class="sheet-actions">${actions}</div>
    <div id="sheet-read"></div>
    ${descBlock(meta)}
  `, onClose);

  const readPayload = ownership.work ? { work_id: ownership.work.id }
    : ownership.read_state?.work_id ? { work_id: ownership.read_state.work_id }
    : { metadata: meta };
  renderReadSection($("#sheet-read"), ownership.read_state, readPayload);

  if (exact) {
    $("#sheet-copy").addEventListener("click", async () => {
      await patchBook(exact.id, { copies: (exact.copies || 1) + 1 }, `now ${(exact.copies || 1) + 1} copies ✓`);
      closeSheet();
    });
    bindStatusSwitcher(exact.id, exact.status);
    bindDelete($("#sheet-delete"), exact.id);
    bindAddEdition("sheet-new", meta);
  } else {
    const add = (status) => async () => {
      try {
        const fmt = $("#sheet-format").value || null;
        const res = await api("/api/books", {
          method: "POST",
          body: JSON.stringify({ status, metadata: meta, format: fmt, library_id: targetLibraryId() }),
        });
        toast(res.existed ? `already saved — now ${HOLD_PHRASES[status]}` : `added — ${HOLD_PHRASES[status]} ✓`, "ok");
        refreshStats();
        invalidateBooks();
        closeSheet();
      } catch (err) { toast(err.message, "err"); }
    };
    $("#sheet-add-library").addEventListener("click", add("library"));
    $("#sheet-add-digital").addEventListener("click", add("digital"));
    $("#sheet-add-wishlist").addEventListener("click", add("wishlist"));
  }
}

/* manage-mode sheet: a book already on one of my shelves */
async function openEditionSheet(bookId) {
  let data;
  try {
    data = await api(`/api/books/${bookId}`);
  } catch (err) { toast(err.message, "err"); return; }
  const book = data.book;
  const related = data.related;
  const relatedHtml = related.length
    ? `<div class="own-banner warn">other editions of this book you track:<ul>${related.map((e) =>
        `<li>${holdingLine(e)}</li>`).join("")}</ul></div>`
    : "";
  const where = me && me.libraries.length > 1 ? ` in ${esc(libraryName(book.library_id))}` : "";
  const bannerCls = { library: "ok", digital: "dig", wishlist: "warn" }[book.status];
  const bannerLabel = book.status === "library" ? `✓ in library${where}`
    : book.status === "digital" ? `⌁ owned digitally${where}` : `✩ on wishlist${where}`;
  openSheet(`
    ${sheetHead(book)}
    <div class="own-banner ${bannerCls}">
      ${bannerLabel} since ${fmtDate(book.status_changed_at)}
    </div>
    ${relatedHtml}
    <div class="sheet-actions">
      ${statusSwitcher(book.status)}
      <label class="field"><span>edition format</span>${formatSelect("edit-format", book.format)}</label>
      <label class="field"><span>category (the shelves group by this)</span>
        <input id="edit-genre" list="genre-suggestions" autocapitalize="none"
          placeholder="e.g. fantasy, mystery — blank = uncategorized" value="${esc(book.genre || "")}">
        <datalist id="genre-suggestions">
          ${["fantasy", "science fiction", "mystery", "thriller", "horror", "romance",
             "literary fiction", "historical fiction", "fairy tale", "classics", "poetry",
             "young adult", "children's", "graphic novel", "nonfiction", "biography",
             "memoir", "history", "science", "self-help"].map((g) => `<option value="${g}">`).join("")}
        </datalist></label>
      <label class="field"><span>copies of this exact edition</span>
        <div class="copies-row">
          <button class="btn secondary" id="copies-minus">−</button>
          <span id="copies-count">${book.copies || 1}</span>
          <button class="btn secondary" id="copies-plus">+</button>
        </div></label>
      <label class="field"><span>library notes (shared)</span>
        <textarea id="edit-notes" rows="2" placeholder="signed copy, loaned to mom, …">${esc(book.notes || "")}</textarea></label>
      <div class="btnrow">
        <button class="btn primary" id="edit-save">save</button>
        <button class="btn danger" id="edit-delete">remove</button>
      </div>
      <label class="field"><span>own a different edition too? pick its format</span>
        ${formatSelect("new-ed-format", "")}</label>
      <div class="btnrow">
        <button class="btn secondary" id="new-ed-library">+ library</button>
        <button class="btn secondary" id="new-ed-digital">+ digital</button>
        <button class="btn secondary" id="new-ed-wishlist">+ wishlist</button>
      </div>
    </div>
    <div id="sheet-read"></div>
    ${descBlock(book)}
  `);

  renderReadSection($("#sheet-read"), data.read_state, { work_id: book.work_id });

  let copies = book.copies || 1;
  const renderCopies = () => { $("#copies-count").textContent = copies; };
  $("#copies-minus").addEventListener("click", () => { copies = Math.max(1, copies - 1); renderCopies(); });
  $("#copies-plus").addEventListener("click", () => { copies += 1; renderCopies(); });

  $("#edit-save").addEventListener("click", async () => {
    await patchBook(book.id, {
      format: $("#edit-format").value || null,
      genre: $("#edit-genre").value.trim().toLowerCase(),
      notes: $("#edit-notes").value,
      copies,
    }, "saved ✓");
    closeSheet();
  });
  bindStatusSwitcher(book.id, book.status);
  bindDelete($("#edit-delete"), book.id);
  bindAddEdition("new-ed", {
    title: book.title,
    subtitle: book.subtitle,
    authors: (book.authors || "").split(",").map((s) => s.trim()).filter(Boolean),
    publisher: book.publisher,
    published_date: book.published_date,
    description: book.description,
    cover_url: book.cover_url,
    page_count: book.page_count,
    language: book.language,
    genre: book.genre,
  });
}

/* browse-and-add sheet for the desk ledgers in the 3D library: the whole
   wishlist / reading log as a list, with an inline search to add to it */
async function openDeskListSheet(kind) {
  const isWish = kind === "wishlist";
  openSheet(`
    <div class="result-group-label"><span class="slashes">//</span> ${isWish ? "the wishlist" : "the reading log"}</div>
    <form id="ledger-add-form" class="row-form">
      <input id="ledger-add-input" placeholder="${isWish ? "add a book — title, author, or isbn" : "log a book — title, author, or isbn"}" autocapitalize="none">
      <button class="btn primary" type="submit">search</button>
    </form>
    <div id="ledger-add-results"></div>
    <div class="result-group-label"><span class="slashes">//</span> on the list</div>
    <div id="ledger-items" class="book-list"><div class="empty">loading…</div></div>
  `);

  async function renderItems() {
    const itemsBox = $("#ledger-items");
    if (!itemsBox) return;   // the sheet moved on
    try {
      const items = isWish
        ? (await api(`/api/books?status=wishlist${shelfLibraryScope()}`)).items || []
        : (await api("/api/reads")).items || [];
      if (!items.length) {
        itemsBox.innerHTML = `<div class="empty">${isWish
          ? "nothing wished for yet — search above to add the first"
          : "no reading history yet — search above to log the first"}</div>`;
        return;
      }
      itemsBox.innerHTML = items.map((it, i) => `
        <div class="book-card" data-idx="${i}">
          ${coverImg(it.cover_url)}
          <div class="meta">
            <div class="title">${esc(it.title || "")}</div>
            <div class="authors">${esc(it.authors || "")}</div>
            <div class="chips">${isWish
              ? `<span class="chip warn">✩ wishlist</span>${it.format ? `<span class="chip">${esc(it.format)}</span>` : ""}`
              : `${readChip(it.status)}${it.rating ? `<span class="chip">${"★".repeat(it.rating)}</span>` : ""}`}</div>
          </div>
        </div>`).join("");
      itemsBox.querySelectorAll(".book-card").forEach((card) =>
        card.addEventListener("click", () => {
          const it = items[Number(card.dataset.idx)];
          if (isWish) openShelfBook(it); else openReadSheet(it);
        }));
    } catch (err) { itemsBox.innerHTML = `<div class="empty">${esc(err.message)}</div>`; }
  }
  renderItems();

  $("#ledger-add-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const q = $("#ledger-add-input").value.trim();
    if (!q) return;
    const box = $("#ledger-add-results");
    box.innerHTML = `<div class="empty">searching…</div>`;
    try {
      const data = await api(`/api/search?q=${encodeURIComponent(q)}`);
      const results = (data.external || []).slice(0, 6);
      if (!results.length) { box.innerHTML = `<div class="empty">no results — try another spelling</div>`; return; }
      box.innerHTML = results.map((m, i) => `
        <div class="book-card" data-i="${i}">
          ${coverImg(m.cover_url)}
          <div class="meta">
            <div class="title">${esc(m.title || "")}</div>
            <div class="authors">${esc(Array.isArray(m.authors) ? m.authors.join(", ") : m.authors || "")}</div>
            <div class="chips"><span class="chip ok">${isWish ? "＋ tap to wish for it" : "＋ tap to log it"}</span>
              ${m.published_date ? `<span class="chip">${esc(m.published_date)}</span>` : ""}</div>
          </div>
        </div>`).join("");
      box.querySelectorAll(".book-card").forEach((card) => card.addEventListener("click", async () => {
        const m = results[Number(card.dataset.i)];
        try {
          if (isWish) {
            await api("/api/books", {
              method: "POST",
              body: JSON.stringify({ status: "wishlist", metadata: m, format: m.format || null, library_id: targetLibraryId() }),
            });
            toast(`${m.title} — wished for ✓`, "ok");
          } else {
            await api("/api/reads", {
              method: "POST",
              body: JSON.stringify({ metadata: m, status: "want_to_read" }),
            });
            toast(`${m.title} — on the reading log ✓`, "ok");
          }
          box.innerHTML = "";
          $("#ledger-add-input").value = "";
          refreshStats();
          invalidateBooks();
          readCache = null;
          renderItems();
        } catch (err) { toast(err.message, "err"); }
      }));
    } catch (err) { box.innerHTML = `<div class="empty">${esc(err.message)}</div>`; }
  });
}

function bindDelete(btn, bookId) {
  btn.addEventListener("click", async () => {
    if (!btn.dataset.armed) {
      btn.dataset.armed = "1";
      btn.textContent = "tap again to confirm removal";
      setTimeout(() => { btn.dataset.armed = ""; btn.textContent = "remove"; }, 3000);
      return;
    }
    try {
      await api(`/api/books/${bookId}`, { method: "DELETE" });
      toast("removed (reading history kept)", "ok");
      refreshStats();
      invalidateBooks();
      closeSheet();
      if (currentView === "library" || currentView === "wishlist") loadList(currentView);
    } catch (err) { toast(err.message, "err"); }
  });
}

async function patchBook(id, fields, okMsg) {
  try {
    await api(`/api/books/${id}`, { method: "PATCH", body: JSON.stringify(fields) });
    toast(okMsg, "ok");
    refreshStats();
    invalidateBooks();
    if (currentView === "library" || currentView === "wishlist") loadList(currentView);
  } catch (err) { toast(err.message, "err"); }
}

/* ---------- search ---------- */

// the × in the search box: visible only while there's something to clear,
// wipes both the query and the results
const searchClearBtn = $("#search-clear");
$("#search-input").addEventListener("input", () =>
  searchClearBtn.classList.toggle("show", !!$("#search-input").value));
searchClearBtn.addEventListener("click", () => {
  $("#search-input").value = "";
  searchClearBtn.classList.remove("show");
  $("#search-results").innerHTML = "";
  $("#search-input").focus();
});

$("#search-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const q = $("#search-input").value.trim();
  if (!q) return;
  const box = $("#search-results");

  // one box for everything: a complete isbn/upc typed here behaves like the
  // old "type the code" box and opens the edition sheet directly. Anything
  // else — words, or a partial code — falls through to search. (Scanning
  // still validates the check digit; this is only the typed path.)
  const codeish = q.replace(/[\s-]/g, "");
  if (/^\d{9}[\dxX]$|^\d{12,13}$/.test(codeish)) {
    box.innerHTML = `<div class="empty">looking up ${esc(codeish)}…</div>`;
    if (await lookupCode(codeish, { quiet: true })) {
      box.innerHTML = "";
      return;
    }
  }

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
    if (item.copies > 1) chips.push(`<span class="chip">×${item.copies}</span>`);
    if (item.read_status) chips.push(readChip(item.read_status));
  } else {
    if (item.owned_exact) chips.push(`<span class="chip ok">✓ you have this edition</span>`);
    else if ((item.owned_editions || []).length) {
      const owned = item.owned_editions.map((e) => `${e.format || "?"}${e.status === "wishlist" ? " ✩" : ""}`).join(", ");
      chips.push(`<span class="chip warn">≈ you have: ${esc(owned)}</span>`);
    }
    if (item.read_status) chips.push(readChip(item.read_status));
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
            read_state: meta.read_state || null,
          });
        }
      }
    });
  });
}

/* ---------- library / wishlist ---------- */

let ownFilter = ""; // '' = all owned | 'library' (physical) | 'digital'

async function loadList(view) {
  const box = $(`#${view}-list`);
  if (!listCache[view].length) {
    box.innerHTML = `<div class="empty">loading…</div>`;
    try {
      // the library view spans both owned states; wishlist stays its own list
      const params = new URLSearchParams();
      if (view === "wishlist") params.set("status", "wishlist");
      if (activeLibraryId) params.set("library_id", activeLibraryId);
      const data = await api(`/api/books?${params}`);
      listCache[view] = view === "wishlist" ? data.items
        : data.items.filter((b) => b.status !== "wishlist");
    } catch (err) {
      box.innerHTML = `<div class="empty">${esc(err.message)}</div>`;
      return;
    }
  }
  renderList(view);
}

function renderList(view) {
  const box = $(`#${view}-list`);
  const filter = $(`#${view}-filter`).value.trim().toLowerCase();
  let items = listCache[view];
  if (view === "library" && ownFilter) {
    items = items.filter((ed) => ed.status === ownFilter);
  }
  if (filter) {
    items = items.filter((ed) =>
      (ed.title || "").toLowerCase().includes(filter) ||
      (ed.authors || "").toLowerCase().includes(filter) ||
      (ed.isbn13 || "").includes(filter));
  }
  if (!items.length) {
    box.innerHTML = `<div class="empty">${filter || ownFilter ? "nothing matches that filter"
      : view === "library" ? "no books yet — scan a barcode to start shelving"
      : "wishlist is empty — scan or search while shopping"}</div>`;
    return;
  }
  box.innerHTML = items.map((ed) => bookCardHtml(ed, "local")).join("");
  box.querySelectorAll(".book-card").forEach((card) =>
    card.addEventListener("click", () => openEditionSheet(card.dataset.id)));
}

$("#library-filter").addEventListener("input", () => renderList("library"));
$("#wishlist-filter").addEventListener("input", () => renderList("wishlist"));

document.querySelectorAll("#own-filters .chip-btn").forEach((btn) =>
  btn.addEventListener("click", () => {
    ownFilter = btn.dataset.filter;
    document.querySelectorAll("#own-filters .chip-btn").forEach((b) =>
      b.classList.toggle("active", b === btn));
    renderList("library");
  }));

/* ---------- reading history ---------- */

async function loadReads() {
  const box = $("#read-list");
  if (!readCache) {
    box.innerHTML = `<div class="empty">loading…</div>`;
    try {
      const data = await api("/api/reads");
      readCache = data.items;
    } catch (err) {
      box.innerHTML = `<div class="empty">${esc(err.message)}</div>`;
      return;
    }
  }
  renderReads();
}

function renderReads() {
  const box = $("#read-list");
  let items = readCache || [];
  if (readFilter === "trophies") {
    // books you've read but have no physical copy of — candidates for
    // the shelf, even the ones you own digitally
    items = items.filter((r) => r.status === "read" && !r.owned_physical);
  } else if (readFilter) {
    items = items.filter((r) => r.status === readFilter);
  }
  if (!items.length) {
    box.innerHTML = `<div class="empty">${readFilter === "trophies"
      ? "no trophies to hunt — everything you've read is on your shelves"
      : "no reading history yet — open any book and mark it read"}</div>`;
    return;
  }
  box.innerHTML = items.map((r, i) => {
    const chips = [readChip(r.status)];
    if (r.rating) chips.push(`<span class="chip">${"★".repeat(r.rating)}</span>`);
    chips.push(r.owned_physical
      ? `<span class="chip ok">✓ owned</span>`
      : r.owned_digital
        ? `<span class="chip dig">⌁ digital only</span>`
        : `<span class="chip warn">not owned</span>`);
    const dates = [r.started_at && `started ${fmtDate(r.started_at)}`, r.finished_at && `finished ${fmtDate(r.finished_at)}`]
      .filter(Boolean).join(" · ");
    return `
      <div class="book-card" data-idx="${i}">
        ${coverImg(r.cover_url)}
        <div class="meta">
          <div class="title">${esc(r.title)}</div>
          <div class="authors">${esc(r.authors || "")}</div>
          <div class="sub">${dates}</div>
          <div class="chips">${chips.join("")}</div>
        </div>
      </div>`;
  }).join("");
  box.querySelectorAll(".book-card").forEach((card) =>
    card.addEventListener("click", () => openReadSheet(items[Number(card.dataset.idx)])));
}

function openReadSheet(item) {
  const bannerCls = item.owned_physical ? "ok" : item.owned_digital ? "dig" : "warn";
  const ownedHtml = item.owned_editions.length
    ? `<div class="own-banner ${bannerCls}">you have:<ul>${item.owned_editions.map((e) =>
        `<li>${holdingLine(e)}</li>`).join("")}</ul></div>`
    : `<div class="own-banner none">you don't own a copy — search or scan to add one</div>`;
  // read but no physical copy = trophy candidate (unless already wanted)
  const wantTrophy = !item.owned_physical
    && !item.owned_editions.some((e) => e.status === "wishlist");
  const trophyBtn = wantTrophy
    ? `<div class="btnrow"><button class="btn secondary" id="read-trophy">🏆 want it on the shelf — + wishlist</button></div>`
    : "";
  openSheet(`
    ${sheetHead(item)}
    ${ownedHtml}
    ${trophyBtn}
    <div id="sheet-read"></div>
  `);
  if (wantTrophy) {
    $("#read-trophy").addEventListener("click", async () => {
      try {
        await api("/api/books", {
          method: "POST",
          body: JSON.stringify({
            status: "wishlist",
            metadata: {
              title: item.title,
              authors: (item.authors || "").split(",").map((a) => a.trim()).filter(Boolean),
              cover_url: item.cover_url,
            },
            library_id: targetLibraryId(),
          }),
        });
        toast("on the wishlist — happy hunting 🏆", "ok");
        refreshStats();
        invalidateBooks();
        closeSheet();
      } catch (err) { toast(err.message, "err"); }
    });
  }
  renderReadSection($("#sheet-read"), item, { work_id: item.work_id });
}

document.querySelectorAll("#read-filters .chip-btn").forEach((btn) =>
  btn.addEventListener("click", () => {
    readFilter = btn.dataset.filter;
    document.querySelectorAll("#read-filters .chip-btn").forEach((b) =>
      b.classList.toggle("active", b === btn));
    renderReads();
  }));

/* ---------- boot ---------- */

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("sw.js").catch(() => {});
}

if (token) {
  loadMe().then(showApp).catch(() => {
    if ($("#app").classList.contains("hidden")) showLogin();
  });
} else {
  showLogin();
}
