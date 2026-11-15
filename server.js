#!/usr/bin/env node
"use strict";
/**
 * HSC PrepHub – shared-database server (MongoDB Atlas version)
 *
 *  - Sob data (course, notice, student, payment ...) MongoDB-te save hoy.
 *  - Login session-o MongoDB-te thake, tai restart/redeploy-e logout hoy na.
 *  - Video/PDF/image MongoDB GridFS-e save hoy (Render-er disk-er upor nirbhor kore na).
 *
 *  Render Environment Variable: MONGODB_URI = mongodb+srv://...
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const mongoose = require("mongoose");
const { promisify } = require("util");
const pbkdf2 = promisify(crypto.pbkdf2);

const PORT = Number(process.env.PORT) || process.env.PORT || 3000;
const HOST = process.env.HOST || "0.0.0.0";
const MONGODB_URI = process.env.MONGODB_URI;
const DATA_DIR = path.resolve(process.env.DATA_DIR || path.join(__dirname, "data")); // shudhu purono db.json import-er jonno
const DB_FILE = path.join(DATA_DIR, "db.json");
const INDEX_FILE = path.join(__dirname, "public", "index.html");
const MAX_BODY = (Number(process.env.MAX_BODY_MB) || 500) * 1024 * 1024;
const SESSION_MS = 60 * 24 * 3600 * 1000; // 60 din
const TRUST_PROXY = process.env.TRUST_PROXY === "1";
const DEFAULT_ADMIN_HASH = "301afa612ee582e4e918ec287098f3342fd2bb7cd59a934814fa7e403a67beb0";
const SHARED = ["students", "payments", "messages", "quizResults"];

let ready = false;   // MongoDB connect hole true hoy

/* ---------- helpers ---------- */
const sha256 = (s) => crypto.createHash("sha256").update(s).digest("hex");
const isObj = (v) => v && typeof v === "object" && !Array.isArray(v);
const isHex64 = (v) => typeof v === "string" && /^[a-f0-9]{64}$/.test(v);
const arr = (v) => (Array.isArray(v) ? v : []);
const str = (v, max) => String(v == null ? "" : v).slice(0, max);
const uid = () => "x" + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
function safeEq(a, b) {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}
/* ---------- database ---------- */
function defaults() {
  return {
    rev: 1,
    adminUser: "BILLU",
    adminPassHash: DEFAULT_ADMIN_HASH,
    students: [],
    courses: [{
      id: "c1", title: "HSC ICT পূর্ণাঙ্গ কোর্স",
      desc: "বেসিক থেকে এডভান্সড কোডিং ও আইসিটি প্রস্তুতি", price: 1200,
      thumb: "https://images.unsplash.com/photo-1555066931-4365d14bab8c?w=600&auto=format&fit=crop&q=80",
      chapters: [{ id: "ch1", title: "অধ্যায় ১: তথ্য ও যোগাযোগ প্রযুক্তি", classes: [{
        id: "cl1", title: "লেকচার ১: বিশ্বগ্রাম ও ভার্চুয়াল রিয়েলিটি", videoType: "yt",
        videoSource: "https://www.youtube.com/watch?v=qz0aGYrrlhU",
        pdfs: ["https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"], isFree: true }] }]
    }],
    notices: [{ id: "n1", title: "স্বাগতম", body: "HSC PrepHub এ আপনাকে স্বাগতম।", date: new Date().toISOString() }],
    payments: [], messages: [], quizzes: [], quizResults: [],
    numbers: { bkash: "01700000000", nagad: "01800000000" }
  };
}
function normalizeState(d) {
  if (!isObj(d)) d = defaults();
  ["students", "courses", "notices", "payments", "messages", "quizzes", "quizResults"].forEach((k) => { if (!Array.isArray(d[k])) d[k] = []; });
  if (!isObj(d.numbers)) d.numbers = { bkash: "", nagad: "" };
  if (!d.adminUser) d.adminUser = "BILLU";
  if (!isHex64(d.adminPassHash) && !String(d.adminPassHash || "").startsWith("fb_")) d.adminPassHash = DEFAULT_ADMIN_HASH;
  if (!Number.isFinite(d.rev)) d.rev = 1;
  d.notices.forEach((n) => { if (!n.id) n.id = uid(); });
  SHARED.forEach((k) => d[k].forEach((x) => { if (!Number.isFinite(x._rev)) x._rev = 0; }));
  return d;
}
let state, sessions = {}, col, bucket;

// Sob MongoDB write ek-ek kore line-e hoy, jate order na bhange.
let chain = Promise.resolve();
function enqueue(fn) {
  chain = chain.then(fn).catch((e) => console.error("MongoDB write failed:", e.message));
  return chain;
}
function saveState() {
  const json = JSON.stringify(state);
  return enqueue(() => col.replaceOne({ _id: "db" }, { _id: "db", json, updated: new Date() }, { upsert: true }));
}

/* ---------- sessions (disk-e save, restart-e thake) ---------- */
let sessTimer = null;
function flushSessions() {
  if (sessTimer) { clearTimeout(sessTimer); sessTimer = null; }
  const json = JSON.stringify(sessions);
  return enqueue(() => col.replaceOne({ _id: "sessions" }, { _id: "sessions", json, updated: new Date() }, { upsert: true }));
}
function saveSessions() {           // chhoto change (expiry barano, revoke) – ektu pore likho
  if (sessTimer) return;
  sessTimer = setTimeout(flushSessions, 300);
}
async function newSession(role, studentId) {
  const token = crypto.randomBytes(32).toString("hex");
  sessions[sha256(token)] = { role, studentId: studentId || null, exp: Date.now() + SESSION_MS };
  await flushSessions();            // notun login sathe sathe DB-te
  return token;
}
for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, async () => { if (ready) await flushSessions(); process.exit(0); });
function getSession(req) {
  const m = /^Bearer ([a-f0-9]{64})$/.exec(req.headers.authorization || "");
  if (!m) return null;
  const key = sha256(m[1]);
  const s = sessions[key];
  if (!s) return null;
  if (s.exp < Date.now()) { delete sessions[key]; saveSessions(); return null; }
  if (s.role === "student" && !state.students.some((x) => x.id === s.studentId)) { delete sessions[key]; saveSessions(); return null; }
  if (s.exp - Date.now() < SESSION_MS / 2) { s.exp = Date.now() + SESSION_MS; saveSessions(); }
  return { key, role: s.role, studentId: s.studentId };
}
function revokeWhere(fn) {
  let changed = false;
  for (const k of Object.keys(sessions)) if (fn(sessions[k], k)) { delete sessions[k]; changed = true; }
  if (changed) saveSessions();
}
setInterval(() => revokeWhere((s) => s.exp < Date.now()), 3600 * 1000).unref();

/* ---------- brute-force limiter ---------- */
const fails = new Map();
const WINDOW = 15 * 60 * 1000;
const limited = (k, max = 8) => { const f = fails.get(k); if (!f) return false; if (Date.now() - f.t > WINDOW) { fails.delete(k); return false; } return f.n >= max; };
const addFail = (k) => { const f = fails.get(k); if (!f || Date.now() - f.t > WINDOW) fails.set(k, { n: 1, t: Date.now() }); else f.n++; };
setInterval(() => { for (const [k, f] of fails) if (Date.now() - f.t > WINDOW) fails.delete(k); }, WINDOW).unref();
function clientIp(req) {
  if (TRUST_PROXY) { const x = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim(); if (x) return x; }
  return req.socket.remoteAddress || "unknown";
}

/* ---------- passwords ---------- */
async function hashPin(pin) {
  const salt = crypto.randomBytes(16);
  const bits = await pbkdf2(pin, salt, 600000, 32, "sha256");
  return `pbkdf2$600000$${salt.toString("hex")}$${bits.toString("hex")}`;
}
async function verifyPin(email, stored, pin) {
  if (typeof stored === "string" && stored.startsWith("pbkdf2$")) {
    const [, iter, saltHex, expected] = stored.split("$");
    const n = Number(iter);
    if (!n || n > 2000000 || !/^[a-f0-9]+$/.test(saltHex || "")) return false;
    const bits = await pbkdf2(pin, Buffer.from(saltHex, "hex"), n, 32, "sha256");
    return safeEq(bits.toString("hex"), expected || "");
  }
  const legacy = String(stored || "").startsWith("legacy$") ? String(stored).slice(7) : String(stored || "");
  return safeEq(legacy, sha256(email + "::" + pin));
}
function verifyAdminPass(p) {
  const stored = state.adminPassHash || "";
  for (const cand of new Set([p, p.trim()])) {
    if (!cand) continue;
    if (safeEq(sha256(cand), stored)) return cand;
    if (stored.startsWith("fb_") && safeEq(stored, "fb_" + cand)) return cand;
  }
  return null;
}

/* ---------- media (base64 video -> file) ---------- */
const EXT = { "video/mp4": "mp4", "video/webm": "webm", "video/ogg": "ogv", "video/quicktime": "mov", "video/x-matroska": "mkv",
  "application/pdf": "pdf", "image/png": "png", "image/jpeg": "jpg", "image/gif": "gif", "image/webp": "webp" };
const MIME = { mp4: "video/mp4", webm: "video/webm", ogv: "video/ogg", mov: "video/quicktime", mkv: "video/x-matroska",
  pdf: "application/pdf", png: "image/png", jpg: "image/jpeg", gif: "image/gif", webp: "image/webp" };
async function externalize(v) {
  if (typeof v !== "string" || !v.startsWith("data:")) return v;
  const m = /^data:([\w.+-]+\/[\w.+-]+)(?:;[^,]*)?;base64,/i.exec(v);
  if (!m) return v;
  const type = m[1].toLowerCase();
  const ext = EXT[type] || (type.startsWith("video/") ? "mp4" : null);
  if (!ext) return v;
  const buf = Buffer.from(v.slice(m[0].length), "base64");
  const name = sha256(buf).slice(0, 32) + "." + ext;
  const exists = await bucket.find({ filename: name }).limit(1).hasNext();
  if (!exists) {
    await new Promise((ok, fail) => {
      const up = bucket.openUploadStream(name, { contentType: type });
      up.on("error", fail); up.on("finish", ok); up.end(buf);
    });
  }
  return "/media/" + name;
}
async function walkMedia(node) {
  if (typeof node === "string") return externalize(node);
  if (Array.isArray(node)) { const out = []; for (const x of node) out.push(await walkMedia(x)); return out; }
  if (isObj(node)) { for (const k of Object.keys(node)) node[k] = await walkMedia(node[k]); return node; }
  return node;
}

/* ---------- views ---------- */
function viewFor(role, me) {
  if (role === "admin") return state;
  const approved = new Set(me ? state.payments.filter((p) => p.studentId === me.id && p.status === "Approved").map((p) => p.courseId) : []);
  const courses = state.courses.map((c) => {
    if (Number(c.price) <= 0 || approved.has(c.id)) return c;
    const copy = JSON.parse(JSON.stringify(c)); // lock kora course-er paid class-er source pathano hoy na
    arr(copy.chapters).forEach((ch) => arr(ch.classes).forEach((cl) => { if (!cl.isFree) { cl.videoSource = ""; cl.pdfs = []; } }));
    return copy;
  });
  const mine = (k) => (me ? state[k].filter((x) => x.studentId === me.id) : []);
  return {
    adminUser: "", courses, notices: state.notices, quizzes: state.quizzes, numbers: state.numbers,
    students: me ? [pubStudent(me)] : [], payments: mine("payments"), messages: mine("messages"), quizResults: mine("quizResults")
  };
}
const pubStudent = (s) => ({ id: s.id, name: s.name, email: s.email, joined: s.joined });

/* ---------- admin state merge ---------- */
function mergeShared(serverList, clientList, baseRev, newRev, replace) {
  const byId = new Map(serverList.map((x) => [x.id, x]));
  const seen = new Set(), out = [];
  for (const it of clientList) {
    if (!isObj(it) || typeof it.id !== "string" || seen.has(it.id)) continue;
    seen.add(it.id);
    const prev = byId.get(it.id);
    it._rev = prev ? prev._rev : newRev;
    out.push(it);
  }
  // admin-er load-er por je notun item (student-er) ashche, seta rekhe dao
  if (!replace) for (const s of serverList) if (!seen.has(s.id) && s._rev > baseRev) out.push(s);
  return out;
}
async function applyAdminState(inc, baseRev, replace, sess) {
  const courses = await walkMedia(arr(inc.courses).filter(isObj)); // async kaj age, tarpor sob sync
  const newRev = state.rev + 1;
  state.courses = courses;
  state.notices = arr(inc.notices).filter(isObj).map((n) => { if (!n.id) n.id = uid(); return n; });
  state.quizzes = arr(inc.quizzes).filter(isObj);
  if (isObj(inc.numbers)) state.numbers = { bkash: str(inc.numbers.bkash, 40), nagad: str(inc.numbers.nagad, 40) };
  if (typeof inc.adminUser === "string" && inc.adminUser.trim()) state.adminUser = inc.adminUser.trim().slice(0, 100);

  const oldStudents = state.students;
  for (const k of SHARED) state[k] = mergeShared(state[k], arr(inc[k]), baseRev, newRev, replace);

  const nowById = new Map(state.students.map((s) => [s.id, s]));
  const gone = new Set();
  for (const o of oldStudents) { const n = nowById.get(o.id); if (!n || n.pinHash !== o.pinHash) gone.add(o.id); }
  if (gone.size) revokeWhere((s) => s.role === "student" && gone.has(s.studentId));

  if (isHex64(inc.adminPassHash) && inc.adminPassHash !== state.adminPassHash) {
    state.adminPassHash = inc.adminPassHash;
    revokeWhere((s, k) => s.role === "admin" && k !== sess.key); // password bodlale onno admin session bondho
  }
  state.rev = newRev;
  await saveState();
}

/* ---------- HTTP ---------- */
function json(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "Content-Length": Buffer.byteLength(body) });
  res.end(body);
}
function readJson(req) {
  return new Promise((resolve, reject) => {
    const fail = (status) => { const e = new Error("request error"); e.status = status; return e; };
    if (Number(req.headers["content-length"] || 0) > MAX_BODY) { req.resume(); return reject(fail(413)); }
    const chunks = []; let size = 0, big = false;
    req.on("data", (c) => { size += c.length; if (size > MAX_BODY) big = true; else chunks.push(c); });
    req.on("end", () => {
      if (big) return reject(fail(413));
      try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}")); } catch { reject(fail(400)); }
    });
    req.on("error", reject);
  });
}
function serveIndex(req, res) {
  fs.readFile(INDEX_FILE, (err, buf) => {
    if (err) return json(res, 500, { error: "index_missing" });
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache", "X-Content-Type-Options": "nosniff" });
    res.end(req.method === "HEAD" ? undefined : buf);
  });
}
async function serveMedia(req, res, p) {
  const name = p.slice(7);
  if (!/^[a-f0-9]{32}\.[a-z0-9]{2,5}$/.test(name)) return json(res, 404, { error: "not_found" });
  const f = await bucket.find({ filename: name }).limit(1).next();
  if (!f) return json(res, 404, { error: "not_found" });
  const size = f.length;
  const headers = { "Content-Type": MIME[name.split(".")[1]] || "application/octet-stream", "Accept-Ranges": "bytes",
    "Cache-Control": "public, max-age=31536000, immutable", "X-Content-Type-Options": "nosniff", "Content-Security-Policy": "sandbox" };
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || "");
  let start = 0, end = size - 1, status = 200;
  if (m && (m[1] || m[2])) {
    if (m[1]) { start = Number(m[1]); if (m[2]) end = Math.min(Number(m[2]), end); }
    else { start = Math.max(0, size - Number(m[2])); }
    if (start > end || start >= size) { res.writeHead(416, { "Content-Range": `bytes */${size}` }); return res.end(); }
    status = 206; headers["Content-Range"] = `bytes ${start}-${end}/${size}`;
  }
  headers["Content-Length"] = size === 0 ? 0 : end - start + 1;
  res.writeHead(status, headers);
  if (req.method === "HEAD" || size === 0) return res.end();
  const stream = bucket.openDownloadStream(f._id, { start, end: end + 1 }); // GridFS-e end exclusive
  stream.on("error", () => res.destroy());
  res.on("close", () => stream.destroy());
  stream.pipe(res);
}

async function route(req, res) {
  const url = new URL(req.url, "http://localhost");
  const p = url.pathname, M = req.method;

  if ((M === "GET" || M === "HEAD") && (p === "/" || p === "/index.html")) return serveIndex(req, res);
  if ((M === "GET" || M === "HEAD") && p.startsWith("/media/")) return serveMedia(req, res, p);
  if (M === "GET" && p === "/favicon.ico") { res.writeHead(204); return res.end(); }
  if (M === "GET" && p === "/api/health") return json(res, 200, { ok: true });

  if (M === "GET" && p === "/api/state") {
    const sess = getSession(req);
    if (req.headers.authorization && !sess) return json(res, 401, { error: "session" });
    let role = "public", me = null;
    if (sess) {
      if (sess.role === "admin") role = "admin";
      else { me = state.students.find((s) => s.id === sess.studentId); role = "student"; }
    }
    const since = url.searchParams.get("since");
    if (since !== null && Number(since) === state.rev) return json(res, 200, { unchanged: true, rev: state.rev, role });
    return json(res, 200, { role, rev: state.rev, me: me ? pubStudent(me) : null, db: viewFor(role, me) });
  }

  if (M === "POST" && p === "/api/admin/login") {
    const key = "admin|" + clientIp(req);
    if (limited(key)) return json(res, 429, { error: "rate_limited" });
    const b = await readJson(req);
    const userOk = String(b.username || "").trim().toLowerCase() === String(state.adminUser).toLowerCase();
    const cand = verifyAdminPass(str(b.password, 300));
    if (!userOk || cand === null) { addFail(key); return json(res, 401, { error: "invalid" }); }
    fails.delete(key);
    if (String(state.adminPassHash).startsWith("fb_")) { state.adminPassHash = sha256(cand); await saveState(); }
    return json(res, 200, { token: await newSession("admin"), role: "admin", rev: state.rev, db: viewFor("admin") });
  }

  if (M === "PUT" && p === "/api/admin/state") {
    const sess = getSession(req);
    if (!sess || sess.role !== "admin") return json(res, 401, { error: "session" });
    const b = await readJson(req);
    if (!isObj(b.db)) return json(res, 400, { error: "bad_request" });
    const baseRev = Number.isFinite(Number(b.baseRev)) ? Number(b.baseRev) : 0;
    await applyAdminState(b.db, baseRev, b.replace === true, sess);
    return json(res, 200, { role: "admin", rev: state.rev, db: viewFor("admin") });
  }

  if (M === "POST" && (p === "/api/admin/logout" || p === "/api/student/logout")) {
    const sess = getSession(req);
    if (sess) { delete sessions[sess.key]; saveSessions(); }
    return json(res, 200, { ok: true });
  }

  if (M === "POST" && p === "/api/student/join") {
    const ip = clientIp(req);
    const b = await readJson(req);
    const name = str(b.name, 100).trim();
    const email = str(b.email, 200).trim().toLowerCase();
    const pin = str(b.pin, 200);
    if (!name || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || pin.length < 4) return json(res, 400, { error: "invalid_input" });
    const key = "stu|" + ip + "|" + email;
    if (limited(key)) return json(res, 429, { error: "rate_limited" });

    let student = null;
    for (let attempt = 0; attempt < 2 && !student; attempt++) {
      const s = state.students.find((x) => x.email === email);
      if (s) {
        if (s.pinHash && !(await verifyPin(email, s.pinHash, pin))) { addFail(key); return json(res, 401, { error: "bad_pin" }); }
        const upgrade = !s.pinHash || !s.pinHash.startsWith("pbkdf2$") ? await hashPin(pin) : null;
        const still = state.students.find((x) => x.id === s.id);
        if (!still) continue;
        still.name = name;
        if (upgrade) still.pinHash = upgrade;
        student = still;
      } else {
        if (limited("new|" + ip, 20)) return json(res, 429, { error: "rate_limited" });
        const pinHash = await hashPin(pin);
        if (state.students.some((x) => x.email === email)) continue;
        addFail("new|" + ip);
        student = { id: uid(), name, email, pinHash, joined: new Date().toISOString(), _rev: state.rev + 1 };
        state.students.push(student);
      }
    }
    if (!student) return json(res, 503, { error: "busy" });
    fails.delete(key);
    state.rev += 1;
    await saveState();
    return json(res, 200, { token: await newSession("student", student.id), role: "student", rev: state.rev, me: pubStudent(student), db: viewFor("student", student) });
  }

  if (M === "POST" && p === "/api/student/sync") {
    const sess = getSession(req);
    if (!sess || sess.role !== "student") return json(res, 401, { error: "session" });
    const me = state.students.find((s) => s.id === sess.studentId);
    if (!me) return json(res, 401, { error: "session" });
    const b = await readJson(req);
    const newRev = state.rev + 1;
    const accepted = [], rejected = [];
    const okId = (x) => isObj(x) && typeof x.id === "string" && x.id.length > 0 && x.id.length <= 64;
    let changed = false;

    for (const x of arr(b.payments).slice(0, 20)) {
      if (!okId(x)) continue;
      if (state.payments.some((q) => q.id === x.id)) { accepted.push(x.id); continue; }
      const course = state.courses.find((c) => c.id === x.courseId);
      const tx = str(x.transaction, 100).trim().toUpperCase();
      if (x.studentId !== me.id || !course || !tx) { rejected.push(x.id); continue; }
      state.payments.push({ id: x.id, studentId: me.id, studentName: me.name, email: me.email, courseId: course.id, courseTitle: course.title,
        method: str(x.method, 30), transaction: tx, amount: course.price, status: "Pending", date: new Date().toISOString(), _rev: newRev });
      accepted.push(x.id); changed = true;
    }
    for (const x of arr(b.messages).slice(0, 20)) {
      if (!okId(x)) continue;
      if (state.messages.some((q) => q.id === x.id)) { accepted.push(x.id); continue; }
      const text = str(x.message, 2000).trim();
      if (x.studentId !== me.id || !text) { rejected.push(x.id); continue; }
      state.messages.push({ id: x.id, studentId: me.id, studentName: me.name, email: me.email, message: text, date: new Date().toISOString(), reply: "", _rev: newRev });
      accepted.push(x.id); changed = true;
    }
    for (const x of arr(b.quizResults).slice(0, 20)) {
      if (!okId(x)) continue;
      if (state.quizResults.some((q) => q.id === x.id)) { accepted.push(x.id); continue; }
      const quiz = state.quizzes.find((q) => q.id === x.quizId);
      const total = quiz ? arr(quiz.questions).length : 0;
      if (x.studentId !== me.id || !quiz || !total) { rejected.push(x.id); continue; }
      const score = Math.min(total, Math.max(0, Math.floor(Number(x.score) || 0)));
      const percent = Math.round((score / total) * 100);
      state.quizResults.push({ id: x.id, studentId: me.id, quizId: quiz.id, score, total, percent, passed: percent >= 60, date: new Date().toISOString(), _rev: newRev });
      accepted.push(x.id); changed = true;
    }
    if (changed) { state.rev = newRev; await saveState(); }
    return json(res, 200, { accepted, rejected, rev: state.rev });
  }

  return json(res, 404, { error: "not_found" });
}

const server = http.createServer(async (req, res) => {
  try {
    if (!ready) return json(res, 503, { error: "starting" });
    await route(req, res);
  }
  catch (e) {
    const status = e.status || 500;
    if (status === 500) console.error(e);
    if (!res.headersSent) json(res, status, { error: status === 413 ? "too_large" : status === 400 ? "bad_request" : "server_error" });
    else res.end();
  }
});
server.requestTimeout = 0;       // boro video upload-er jonno
server.headersTimeout = 60000;

async function init() {
  if (!MONGODB_URI) throw new Error("MONGODB_URI environment variable set kora nei (Render > Environment-e dite hobe)");
  mongoose.set("strictQuery", true);
  await mongoose.connect(MONGODB_URI, { serverSelectionTimeoutMS: 15000 });
  const db = mongoose.connection.db;
  col = db.collection("app");
  bucket = new mongoose.mongo.GridFSBucket(db, { bucketName: "media" });

  const doc = await col.findOne({ _id: "db" });
  if (doc) {
    state = normalizeState(JSON.parse(doc.json));
  } else {
    let seed = null;                       // prothom bar: purono db.json thakle seta import koro
    if (fs.existsSync(DB_FILE)) { try { seed = JSON.parse(fs.readFileSync(DB_FILE, "utf8")); console.log("db.json MongoDB-te import hocche"); } catch {} }
    state = normalizeState(seed || defaults());
    await saveState();
  }
  const sdoc = await col.findOne({ _id: "sessions" });
  try { sessions = sdoc ? JSON.parse(sdoc.json) : {}; } catch { sessions = {}; }
  ready = true;
  console.log("Database: MongoDB connected");
}
async function startWithRetry() {
  for (;;) {
    try { await init(); return; }
    catch (e) {
      console.error("MongoDB connect hoyni:", e.message, "- 10 second pore abar chesta korbo");
      try { await mongoose.disconnect(); } catch {}
      await new Promise((r) => setTimeout(r, 10000));
    }
  }
}

// Age port khulbe (Render port scan pass korbe), tarpor MongoDB connect hobe.
server.listen(PORT, HOST, () => {
  console.log(`HSC PrepHub running on port ${PORT}`);
  startWithRetry();
});
