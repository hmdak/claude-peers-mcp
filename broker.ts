#!/usr/bin/env bun
/**
 * claude-peers broker daemon
 *
 * A singleton HTTP server on localhost:7899 backed by SQLite.
 * Tracks all registered Claude Code peers and routes messages between them.
 *
 * Auto-launched by the MCP server if not already running.
 * Run directly: bun broker.ts
 */

import { Database } from "bun:sqlite";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  Peer,
  Message,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${process.env.HOME}/.claude-peers.db`;

// --- Shared-secret auth ---
// If CLAUDE_PEERS_TOKEN is set, every request must present a matching
// `Authorization: Bearer <token>` header. This guarantees that only peers
// configured with the same user's secret can join the broker — a Claude Code
// on the same network without the token is rejected with 401.
// If unset, auth is disabled (backward compatible with existing deployments).
const AUTH_TOKEN = process.env.CLAUDE_PEERS_TOKEN ?? "";

// Constant-time comparison to avoid timing side-channels on the token.
function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

// Returns true if the request is authorized (or auth is disabled).
function isAuthorized(req: Request): boolean {
  if (!AUTH_TOKEN) return true; // auth disabled
  const header = req.headers.get("authorization") ?? "";
  const m = header.match(/^Bearer\s+(.+)$/i);
  if (!m) return false;
  return timingSafeEqual(m[1], AUTH_TOKEN);
}

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    hostname TEXT NOT NULL DEFAULT 'localhost',
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
  )
`);

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

// --- Migration: `pushed` column ---
// The channel push loop reads via /peek-messages, which deliberately does not
// set `delivered` (so check_messages can still show the message). Dedup lived
// only in the server's in-memory pushedMessageIds, so an MCP reconnect replayed
// every still-undelivered message — tens of them for a busy peer. `pushed`
// records "already sent as a channel notification" durably, independently of
// the delivered/ack flag.
const msgCols = (db.query("PRAGMA table_info(messages)").all() as { name: string }[]).map((c) => c.name);
if (!msgCols.includes("pushed")) {
  db.run("ALTER TABLE messages ADD COLUMN pushed INTEGER NOT NULL DEFAULT 0");
  // Existing rows were already pushed to their recipients under the old
  // behaviour; marking them 1 avoids a one-off replay storm on first upgrade.
  // They stay visible to check_messages because `delivered` is untouched.
  const n = db.run("UPDATE messages SET pushed = 1 WHERE delivered = 0").changes;
  console.error(`[claude-peers broker] migration: added messages.pushed, backfilled ${n} undelivered row(s)`);
}

// Stale threshold: peers not seen for 60 seconds are considered dead
const STALE_THRESHOLD_MS = 60_000;

// Clean up stale peers based on last_seen timestamp (works for both local and remote peers)
function cleanStalePeers() {
  const cutoff = new Date(Date.now() - STALE_THRESHOLD_MS).toISOString();
  const stale = db.query("SELECT id FROM peers WHERE last_seen < ?").all(cutoff) as { id: string }[];
  for (const peer of stale) {
    db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
    db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [peer.id]);
  }
  if (stale.length > 0) {
    console.error(`[claude-peers broker] Cleaned ${stale.length} stale peer(s)`);
  }
}

// Startup grace: do NOT evict on boot.
//
// While the broker was down nobody could send a heartbeat, so every row looks
// stale no matter how alive the peer is. Evicting here wipes the whole peer
// list on every restart, and clients that only send { id } then come back as
// "unknown" placeholders — which is exactly the bug this file is fixing.
// Give everyone one full interval to check in instead.
const bootNow = new Date().toISOString();
const revived = db.run("UPDATE peers SET last_seen = ?", [bootNow]).changes;
if (revived > 0) {
  console.error(`[claude-peers broker] Startup grace: kept ${revived} peer(s); they have ${STALE_THRESHOLD_MS / 1000}s to check in`);
}

// Periodically clean stale peers (every 30s)
setInterval(cleanStalePeers, 30_000);

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, pid, hostname, cwd, git_root, tty, summary, registered_at, last_seen)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const selectAllPeers = db.prepare(`
  SELECT * FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT * FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT * FROM peers WHERE git_root = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
  VALUES (?, ?, ?, ?, 0)
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

const markDelivered = db.prepare(`
  UPDATE messages SET delivered = 1 WHERE id = ?
`);

// Not-yet-pushed messages, for the channel push loop (see /peek-messages).
const selectUnpushed = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 AND pushed = 0 ORDER BY sent_at ASC
`);

const markPushed = db.prepare(`
  UPDATE messages SET pushed = 1 WHERE id = ?
`);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

// --- Request handlers ---

// hostname must be in the SET list. Without it, a row created by the heartbeat
// ghost path (hostname "unknown") could never recover its real hostname, so the
// peer stayed "unknown" to everyone else for the rest of the session.
const updatePeerOnReregister = db.prepare(`
  UPDATE peers SET pid = ?, hostname = ?, cwd = ?, git_root = ?, tty = ?, last_seen = ? WHERE id = ?
`);

function handleRegister(body: RegisterRequest): RegisterResponse {
  const now = new Date().toISOString();
  const hostname = body.hostname ?? "localhost";

  // Stable ID lets a session keep the same peer ID across `--resume` (which
  // changes PID and Claude's own session ID). If the client provides one and
  // it already exists, we UPDATE that row in place — so other peers' saved
  // to_id stays valid and queued messages still reach it. Fall back to a random
  // ID for older clients that don't send stable_id.
  if (body.stable_id) {
    const id = body.stable_id;
    const existing = db.query("SELECT id FROM peers WHERE id = ?").get(id) as { id: string } | null;
    if (existing) {
      // Re-registration of the same logical session (e.g. after --resume):
      // update metadata, keep the ID, and DO NOT drop undelivered messages.
      updatePeerOnReregister.run(body.pid, hostname, body.cwd, body.git_root, body.tty, now, id);
    } else {
      insertPeer.run(id, body.pid, hostname, body.cwd, body.git_root, body.tty, body.summary, now, now);
    }
    return { id };
  }

  // Legacy path: random ID keyed by PID+hostname.
  const id = generateId();
  const existing = db.query("SELECT id FROM peers WHERE pid = ? AND hostname = ?").get(body.pid, hostname) as { id: string } | null;
  if (existing) {
    deletePeer.run(existing.id);
  }
  insertPeer.run(id, body.pid, hostname, body.cwd, body.git_root, body.tty, body.summary, now, now);
  return { id };
}

function handleHeartbeat(body: HeartbeatRequest): void {
  const now = new Date().toISOString();
  const result = updateLastSeen.run(now, body.id);
  // If no rows updated, the peer was evicted — re-register as a ghost peer
  // so it at least appears in list_peers and can receive messages.
  // Next proper /register call (if server supports ensureRegistered) will replace this.
  if (result.changes === 0) {
    // The peer was evicted by cleanStalePeers but is clearly still alive.
    // Restore it from the identity carried in the heartbeat. Older clients
    // send only { id }; for those we still have to fall back to placeholders,
    // and their rows stay "unknown" until they re-register or restart.
    if (body.hostname && body.cwd !== undefined) {
      console.error(`[claude-peers broker] Restoring evicted peer ${body.id} (${body.hostname})`);
      insertPeer.run(body.id, body.pid ?? 0, body.hostname, body.cwd,
                     body.git_root ?? null, body.tty ?? null, body.summary ?? "", now, now);
    } else {
      console.error(`[claude-peers broker] Heartbeat from unknown peer ${body.id} without identity, creating ghost entry`);
      insertPeer.run(body.id, 0, "unknown", "", null, null, "", now, now);
    }
  }
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  let peers: Peer[];

  switch (body.scope) {
    case "machine":
      peers = selectAllPeers.all() as Peer[];
      break;
    case "directory":
      peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      break;
    case "repo":
      if (body.git_root) {
        peers = selectPeersByGitRoot.all(body.git_root) as Peer[];
      } else {
        // No git root, fall back to directory
        peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      }
      break;
    default:
      peers = selectAllPeers.all() as Peer[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // Filter out stale peers (not seen within threshold)
  const cutoff = new Date(Date.now() - STALE_THRESHOLD_MS).toISOString();
  return peers.filter((p) => p.last_seen >= cutoff);
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  console.error(`[broker] send-message: from=${body.from_id} to=${body.to_id} text=${(body.text ?? "").slice(0, 40)}`);
  // Verify target exists
  const target = db.query("SELECT id FROM peers WHERE id = ?").get(body.to_id) as { id: string } | null;
  if (!target) {
    console.error(`[broker] send-message FAILED: Peer ${body.to_id} not found. All peers: ${JSON.stringify((selectAllPeers.all() as Peer[]).map(p => p.id))}`);
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  const messages = selectUndelivered.all(body.id) as Message[];

  // Mark them as delivered
  for (const msg of messages) {
    markDelivered.run(msg.id);
  }

  return { messages };
}

function handlePeekMessages(body: PollMessagesRequest): PollMessagesResponse {
  // Return undelivered messages WITHOUT marking them as delivered, so an
  // explicit check_messages can still surface them. `pushed` is set instead:
  // that makes channel-push dedup survive an MCP reconnect, which the server's
  // in-memory pushedMessageIds could not.
  const messages = selectUnpushed.all(body.id) as Message[];
  for (const msg of messages) {
    markPushed.run(msg.id);
  }
  return { messages };
}

function handleAckMessages(body: { ids: number[] }): void {
  for (const id of body.ids) {
    markDelivered.run(id);
  }
}

function handleUnregister(body: { id: string }): void {
  deletePeer.run(body.id);
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "0.0.0.0",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        return Response.json({ status: "ok", peers: (selectAllPeers.all() as Peer[]).length, auth: AUTH_TOKEN ? "required" : "disabled" });
      }
      return new Response("claude-peers broker", { status: 200 });
    }

    // All POST endpoints require a valid shared-secret token (if configured).
    if (!isAuthorized(req)) {
      return Response.json({ error: "unauthorized: invalid or missing peers token" }, { status: 401 });
    }

    try {
      const body = await req.json();

      switch (path) {
        case "/register":
          return Response.json(handleRegister(body as RegisterRequest));
        case "/heartbeat":
          handleHeartbeat(body as HeartbeatRequest);
          return Response.json({ ok: true });
        case "/set-summary":
          handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(handleSendMessage(body as SendMessageRequest));
        case "/poll-messages":
          return Response.json(handlePollMessages(body as PollMessagesRequest));
        case "/peek-messages":
          // Like poll-messages but does NOT mark as delivered
          return Response.json(handlePeekMessages(body as PollMessagesRequest));
        case "/ack-messages":
          // Mark specific message IDs as delivered
          handleAckMessages(body as { ids: number[] });
          return Response.json({ ok: true });
        case "/unregister":
          handleUnregister(body as { id: string });
          return Response.json({ ok: true });
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

console.error(`[claude-peers broker] listening on 0.0.0.0:${PORT} (db: ${DB_PATH}) auth=${AUTH_TOKEN ? "REQUIRED" : "DISABLED"}`);
