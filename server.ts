#!/usr/bin/env bun
/**
 * claude-peers MCP server
 *
 * Spawned by Claude Code as a stdio MCP server (one per instance).
 * Connects to the shared broker daemon for peer discovery and messaging.
 * Declares claude/channel capability to push inbound messages immediately.
 *
 * Usage:
 *   claude --dangerously-load-development-channels server:claude-peers
 *
 * With .mcp.json:
 *   { "claude-peers": { "command": "bun", "args": ["./server.ts"] } }
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import type {
  PeerId,
  Peer,
  RegisterResponse,
  PollMessagesResponse,
  Message,
} from "./shared/types.ts";
import {
  generateSummary,
  getGitBranch,
  getRecentFiles,
} from "./shared/summarize.ts";
import { createHash } from "node:crypto";

// --- Configuration ---

const BROKER_PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
// Allow connecting to a remote broker via CLAUDE_PEERS_BROKER_URL (e.g. http://evo-x2:7899)
const BROKER_URL = process.env.CLAUDE_PEERS_BROKER_URL ?? `http://127.0.0.1:${BROKER_PORT}`;
const POLL_INTERVAL_MS = 1000;
const HEARTBEAT_INTERVAL_MS = 15_000;
const BROKER_SCRIPT = new URL("./broker.ts", import.meta.url).pathname;

// Detect if broker is remote (not localhost)
const IS_REMOTE_BROKER = !BROKER_URL.includes("127.0.0.1") && !BROKER_URL.includes("localhost");

// Shared-secret token proving this peer belongs to the same user.
// Must match the broker's CLAUDE_PEERS_TOKEN. Distributed via env (.zshenv).
const AUTH_TOKEN = process.env.CLAUDE_PEERS_TOKEN ?? "";

// --- Broker communication ---

async function brokerFetch<T>(path: string, body: unknown): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (AUTH_TOKEN) headers["Authorization"] = `Bearer ${AUTH_TOKEN}`;
  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.text();
    if (res.status === 401) {
      throw new Error(
        `Broker rejected this peer (401): the CLAUDE_PEERS_TOKEN on this machine does not match the broker's. ` +
        `Ensure the same token is exported (e.g. in ~/.zshenv) on every worker and on the broker host.`,
      );
    }
    throw new Error(`Broker error (${path}): ${res.status} ${err}`);
  }
  return res.json() as Promise<T>;
}

async function isBrokerAlive(): Promise<boolean> {
  try {
    const res = await fetch(`${BROKER_URL}/health`, { signal: AbortSignal.timeout(2000) });
    return res.ok;
  } catch {
    return false;
  }
}

async function ensureBroker(): Promise<void> {
  if (await isBrokerAlive()) {
    log(`Broker already running at ${BROKER_URL}`);
    return;
  }

  if (IS_REMOTE_BROKER) {
    throw new Error(`Remote broker at ${BROKER_URL} is not reachable. Ensure the broker is running on the remote host.`);
  }

  log("Starting local broker daemon...");
  const proc = Bun.spawn(["bun", BROKER_SCRIPT], {
    stdio: ["ignore", "ignore", "inherit"],
    // Detach so the broker survives if this MCP server exits
    // On macOS/Linux, the broker will keep running
  });

  // Unref so this process can exit without waiting for the broker
  proc.unref();

  // Wait for it to come up
  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 200));
    if (await isBrokerAlive()) {
      log("Broker started");
      return;
    }
  }
  throw new Error("Failed to start broker daemon after 6 seconds");
}

// --- Utility ---

function log(msg: string) {
  // MCP stdio servers must only use stderr for logging (stdout is the MCP protocol)
  console.error(`[claude-peers] ${msg}`);
}

async function getGitRoot(cwd: string): Promise<string | null> {
  try {
    const proc = Bun.spawn(["git", "rev-parse", "--show-toplevel"], {
      cwd,
      stdout: "pipe",
      stderr: "ignore",
    });
    const text = await new Response(proc.stdout).text();
    const code = await proc.exited;
    if (code === 0) {
      return text.trim();
    }
  } catch {
    // not a git repo
  }
  return null;
}

function getTty(): string | null {
  try {
    // Try to get the parent's tty from the process tree
    const ppid = process.ppid;
    if (ppid) {
      const proc = Bun.spawnSync(["ps", "-o", "tty=", "-p", String(ppid)]);
      const tty = new TextDecoder().decode(proc.stdout).trim();
      if (tty && tty !== "?" && tty !== "??") {
        return tty;
      }
    }
  } catch {
    // ignore
  }
  return null;
}

// --- State ---

let myId: PeerId | null = null;
let myCwd = process.cwd();
let myGitRoot: string | null = null;
let myHostname: string = "localhost";
let myStableId: PeerId | null = null;

// Derive a stable peer ID from hostname + cwd + tty. This survives `--resume`
// (which changes the PID and Claude's own session ID) as long as the session is
// resumed in the same pane and directory — so the peer ID stays constant and
// other peers' saved to_id / queued messages keep reaching it.
// tty may be null (e.g. GUI launch); we still derive a deterministic ID, at the
// cost that two sessions in the same cwd without a tty would collide (rare).
function computeStableId(hostname: string, cwd: string, tty: string | null): PeerId {
  const material = `${hostname} ${cwd} ${tty ?? ""}`;
  // 8 lowercase-alnum chars, matching the broker's legacy ID shape.
  const hex = createHash("sha256").update(material).digest("hex");
  // Map hex to [a-z0-9] for visual consistency with generateId().
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[parseInt(hex.slice(i * 2, i * 2 + 2), 16) % chars.length];
  }
  return id;
}

// Track message IDs that we've already pushed via channel notification
// so we don't re-push the same message on every poll cycle
const pushedMessageIds = new Set<number>();

// Auto-register if not registered or evicted
// True when a peer row looks like the broker's heartbeat ghost placeholder
// rather than a real registration. Mirrors the values inserted by the broker's
// handleHeartbeat fallback: pid 0, hostname "unknown", empty cwd.
function isGhostRow(p: Peer): boolean {
  return p.pid === 0 || p.hostname === "unknown" || p.cwd === "";
}

async function ensureRegistered(): Promise<boolean> {
  if (myId) {
    // Check if still registered
    try {
      const peers = await brokerFetch<Peer[]>("/list-peers", {
        scope: "machine",
        cwd: myCwd,
        git_root: myGitRoot,
      });
      // A row created by the broker's heartbeat ghost path carries placeholder
      // metadata (pid 0 / hostname "unknown" / empty cwd). Treating that as
      // "registered" would skip /register forever, leaving us listed as
      // "unknown" to every other peer. Detect it and re-register to heal.
      const mine = peers.find((p) => p.id === myId);
      if (mine && !isGhostRow(mine)) return true;
      if (mine) log("Registered as a ghost entry (placeholder metadata) — re-registering to restore hostname");
    } catch {
      // Broker down — can't re-register
      return false;
    }
  }
  // Not registered or evicted — re-register (reuse the stable ID so the peer
  // ID stays constant across resume / stale-eviction).
  try {
    log("Auto-registering with broker...");
    const tty = getTty();
    const stableId = myStableId ?? computeStableId(myHostname, myCwd, tty);
    myStableId = stableId;
    const reg = await brokerFetch<RegisterResponse>("/register", {
      pid: process.pid,
      hostname: myHostname,
      cwd: myCwd,
      git_root: myGitRoot,
      tty,
      summary: "",
      stable_id: stableId,
    });
    myId = reg.id;
    log(`Auto-registered as peer ${myId}`);
    return true;
  } catch (e) {
    log(`Auto-registration failed: ${e instanceof Error ? e.message : String(e)}`);
    return false;
  }
}

async function getHostname(): Promise<string> {
  try {
    const proc = Bun.spawnSync(["hostname", "-s"]);
    const name = new TextDecoder().decode(proc.stdout).trim();
    return name || "localhost";
  } catch {
    return "localhost";
  }
}

// --- MCP Server ---

const mcp = new Server(
  { name: "claude-peers", version: "0.1.0" },
  {
    capabilities: {
      experimental: { "claude/channel": {} },
      tools: {},
    },
    instructions: `You are connected to the claude-peers network. Other Claude Code instances across all machines (local and remote workers) can see you and send you messages.

IMPORTANT: When you receive a <channel source="claude-peers" ...> message, RESPOND IMMEDIATELY. Do not wait until your current task is finished. Pause what you are doing, reply to the message using send_message, then resume your work. Treat incoming peer messages like a coworker tapping you on the shoulder — answer right away, even if you're in the middle of something.

CRITICAL — replying to the correct peer (avoid cross-talk):
- To reply, ALWAYS use the from_id of the SPECIFIC notification you are answering right now. Each notification carries the correct from_id in its meta and its "↩ Reply to THIS peer" footer — copy that exact ID.
- NEVER pick a from_id from earlier in the conversation, from your context/summary, or from another peer's message. If you are talking with more than one peer, they each have a different from_id, and reusing the wrong one sends your reply to the wrong session.
- If you are unsure which peer you are replying to (e.g. after a context compaction, or when several peers are active), DO NOT guess: call check_messages (or list_peers) first to re-confirm the current from_id, then reply.
- Peer IDs are short and easy to confuse. Match on the from_id string exactly, and use from_summary / from_cwd only to sanity-check that the ID belongs to the peer you mean.

Available tools:
- list_peers: Discover other Claude Code instances (scope: machine/directory/repo)
- send_message: Send a message to another instance by ID
- set_summary: Set a 1-2 sentence summary of what you're working on (visible to other peers)
- check_messages: Manually check for new messages
- whoami: Get your own peer ID and registration info (useful after session restart to tell other peers your current ID)

When you start, proactively call set_summary to describe what you're working on. This helps other instances understand your context.`,
  }
);

// --- Tool definitions ---

const TOOLS = [
  {
    name: "list_peers",
    description:
      "List other Claude Code instances running on this machine. Returns their ID, working directory, git repo, and summary.",
    inputSchema: {
      type: "object" as const,
      properties: {
        scope: {
          type: "string" as const,
          enum: ["machine", "directory", "repo"],
          description:
            'Scope of peer discovery. "machine" = all instances on this computer. "directory" = same working directory. "repo" = same git repository (including worktrees or subdirectories).',
        },
      },
      required: ["scope"],
    },
  },
  {
    name: "send_message",
    description:
      "Send a message to another Claude Code instance by peer ID. The message will be pushed into their session immediately via channel notification.",
    inputSchema: {
      type: "object" as const,
      properties: {
        to_id: {
          type: "string" as const,
          description: "The peer ID of the target Claude Code instance (from list_peers)",
        },
        message: {
          type: "string" as const,
          description: "The message to send",
        },
      },
      required: ["to_id", "message"],
    },
  },
  {
    name: "set_summary",
    description:
      "Set a brief summary (1-2 sentences) of what you are currently working on. This is visible to other Claude Code instances when they list peers.",
    inputSchema: {
      type: "object" as const,
      properties: {
        summary: {
          type: "string" as const,
          description: "A 1-2 sentence summary of your current work",
        },
      },
      required: ["summary"],
    },
  },
  {
    name: "check_messages",
    description:
      "Manually check for new messages from other Claude Code instances. Messages are normally pushed automatically via channel notifications, but you can use this as a fallback.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
  },
  {
    name: "whoami",
    description:
      "Return this instance's own peer ID and registration info (hostname, pid, cwd, tty). Useful when your session has restarted and other peers need to know your current ID to send messages.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
  },
];

// --- Tool handlers ---

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args } = req.params;

  switch (name) {
    case "list_peers": {
      const scope = (args as { scope: string }).scope as "machine" | "directory" | "repo";
      try {
        const peers = await brokerFetch<Peer[]>("/list-peers", {
          scope,
          cwd: myCwd,
          git_root: myGitRoot,
          exclude_id: myId,
        });

        if (peers.length === 0) {
          return {
            content: [
              {
                type: "text" as const,
                text: `No other Claude Code instances found (scope: ${scope}).`,
              },
            ],
          };
        }

        const lines = peers.map((p) => {
          const parts = [
            `ID: ${p.id}`,
            `Host: ${p.hostname ?? "localhost"}`,
            `PID: ${p.pid}`,
            `CWD: ${p.cwd}`,
          ];
          if (p.git_root) parts.push(`Repo: ${p.git_root}`);
          if (p.tty) parts.push(`TTY: ${p.tty}`);
          if (p.summary) parts.push(`Summary: ${p.summary}`);
          parts.push(`Last seen: ${p.last_seen}`);
          return parts.join("\n  ");
        });

        return {
          content: [
            {
              type: "text" as const,
              text: `Found ${peers.length} peer(s) (scope: ${scope}):\n\n${lines.join("\n\n")}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error listing peers: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "send_message": {
      const rawArgs = args as Record<string, string>;
      const to_id = rawArgs.to_id ?? rawArgs.to;
      const message = rawArgs.message ?? rawArgs.text;
      if (!myId) {
        const ok = await ensureRegistered();
        if (!ok || !myId) {
          return {
            content: [{ type: "text" as const, text: "Not registered with broker and auto-registration failed" }],
            isError: true,
          };
        }
      }
      // Guard against replying to the wrong / stale peer: verify to_id is a
      // live peer before sending. If not, return the current roster so the
      // caller can pick the right recipient instead of silently misfiring.
      if (!to_id) {
        return {
          content: [{ type: "text" as const, text: "send_message requires to_id (the from_id of the peer you are replying to)." }],
          isError: true,
        };
      }
      try {
        const roster = await brokerFetch<Peer[]>("/list-peers", {
          scope: "machine",
          cwd: myCwd,
          git_root: myGitRoot,
        });
        const target = roster.find((p) => p.id === to_id);
        if (!target) {
          const options = roster
            .filter((p) => p.id !== myId)
            .map((p) => `  - ${p.id} (${p.hostname ?? "?"}${p.summary ? `: ${p.summary.slice(0, 50)}` : ""})`)
            .join("\n");
          return {
            content: [{
              type: "text" as const,
              text:
                `Peer "${to_id}" is not currently connected (unknown or stale id). ` +
                `Message NOT sent — this usually means you used a from_id from earlier in the conversation. ` +
                `Reply using the from_id from the CURRENT notification. Live peers now:\n${options || "  (none)"}`,
            }],
            isError: true,
          };
        }
      } catch {
        // If the roster lookup fails, fall through and let the broker decide.
      }
      try {
        const result = await brokerFetch<{ ok: boolean; error?: string }>("/send-message", {
          from_id: myId,
          to_id,
          text: message,
        });
        if (!result.ok) {
          return {
            content: [{ type: "text" as const, text: `Failed to send: ${result.error}` }],
            isError: true,
          };
        }
        return {
          content: [{ type: "text" as const, text: `Message sent to peer ${to_id}` }],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error sending message: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "set_summary": {
      const { summary } = args as { summary: string };
      if (!myId) {
        const ok = await ensureRegistered();
        if (!ok || !myId) {
          return {
            content: [{ type: "text" as const, text: "Not registered with broker and auto-registration failed" }],
            isError: true,
          };
        }
      }
      try {
        await brokerFetch("/set-summary", { id: myId, summary });
        return {
          content: [{ type: "text" as const, text: `Summary updated: "${summary}"` }],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error setting summary: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "check_messages": {
      if (!myId) {
        const ok = await ensureRegistered();
        if (!ok || !myId) {
          return {
            content: [{ type: "text" as const, text: "Not registered with broker and auto-registration failed" }],
            isError: true,
          };
        }
      }
      try {
        // Use poll-messages which marks as delivered (ack on read)
        const result = await brokerFetch<PollMessagesResponse>("/poll-messages", { id: myId });

        // Also mark these as pushed so polling loop skips them
        for (const m of result.messages) {
          pushedMessageIds.add(m.id);
        }

        if (result.messages.length === 0) {
          return {
            content: [{ type: "text" as const, text: "No new messages." }],
          };
        }

        // Look up sender info for each message
        let peers: Peer[] = [];
        try {
          peers = await brokerFetch<Peer[]>("/list-peers", {
            scope: "machine",
            cwd: myCwd,
            git_root: myGitRoot,
          });
        } catch { /* non-critical */ }

        const lines = result.messages.map((m) => {
          const sender = peers.find((p) => p.id === m.from_id);
          const host = sender?.hostname ?? "unknown";
          const summary = sender?.summary ? ` (${sender.summary})` : "";
          return `From ${m.from_id}@${host}${summary} (${m.sent_at}):\n${m.text}`;
        });
        return {
          content: [
            {
              type: "text" as const,
              text: `${result.messages.length} new message(s):\n\n${lines.join("\n\n---\n\n")}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error checking messages: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "whoami": {
      if (!myId) {
        const ok = await ensureRegistered();
        if (!ok || !myId) {
          return {
            content: [{ type: "text" as const, text: "Not registered with broker and auto-registration failed" }],
            isError: true,
          };
        }
      }
      const info = {
        id: myId,
        hostname: myHostname,
        pid: process.pid,
        cwd: myCwd,
        git_root: myGitRoot,
        tty: getTty(),
      };
      const lines = [
        `ID: ${info.id}`,
        `Host: ${info.hostname}`,
        `PID: ${info.pid}`,
        `CWD: ${info.cwd}`,
      ];
      if (info.git_root) lines.push(`Repo: ${info.git_root}`);
      if (info.tty) lines.push(`TTY: ${info.tty}`);
      return {
        content: [{ type: "text" as const, text: lines.join("\n") }],
      };
    }

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
});

// --- Polling loop for inbound messages ---

async function pollAndPushMessages() {
  if (!myId) {
    await ensureRegistered();
    if (!myId) return;
  }

  try {
    // Use peek-messages (does NOT mark as delivered) so check_messages still works
    const result = await brokerFetch<PollMessagesResponse>("/peek-messages", { id: myId });

    for (const msg of result.messages) {
      // Skip messages we've already pushed
      if (pushedMessageIds.has(msg.id)) continue;

      // Look up the sender's info for context
      let fromSummary = "";
      let fromCwd = "";
      let fromHostname = "";
      try {
        const peers = await brokerFetch<Peer[]>("/list-peers", {
          scope: "machine",
          cwd: myCwd,
          git_root: myGitRoot,
        });
        const sender = peers.find((p) => p.id === msg.from_id);
        if (sender) {
          fromSummary = sender.summary;
          fromCwd = sender.cwd;
          fromHostname = sender.hostname ?? "localhost";
        }
      } catch {
        // Non-critical, proceed without sender info
      }

      // Build a reply footer so the receiver can't confuse who to answer.
      // It embeds the exact from_id of THIS message plus a copy-paste template.
      const whoBits = [
        fromHostname && `host=${fromHostname}`,
        fromCwd && `cwd=${fromCwd}`,
        fromSummary && `doing="${fromSummary.slice(0, 60)}"`,
      ].filter(Boolean).join(", ");
      const replyFooter =
        `\n\n────────\n` +
        `↩ Reply to THIS peer only — use this exact id:\n` +
        `   send_message(to_id="${msg.from_id}", text="…")\n` +
        (whoBits ? `   (${msg.from_id}: ${whoBits})\n` : "") +
        `Do NOT reply to any other from_id from earlier in the conversation.`;

      // Push as channel notification — this is what makes it immediate
      try {
        await mcp.notification({
          method: "notifications/claude/channel",
          params: {
            content: msg.text + replyFooter,
            meta: {
              from_id: msg.from_id,
              from_hostname: fromHostname,
              from_summary: fromSummary,
              from_cwd: fromCwd,
              sent_at: msg.sent_at,
            },
          },
        });
        log(`Pushed message from ${msg.from_id}@${fromHostname}: ${msg.text.slice(0, 80)}`);
      } catch (e) {
        log(`Channel push failed for msg ${msg.id}: ${e instanceof Error ? e.message : String(e)}`);
      }

      // Track as pushed regardless (don't retry endlessly)
      pushedMessageIds.add(msg.id);
    }
  } catch (e) {
    // Broker might be down temporarily, don't crash
    log(`Poll error: ${e instanceof Error ? e.message : String(e)}`);
  }
}

// --- Startup ---

async function main() {
  // 1. Ensure broker is running
  await ensureBroker();

  // 2. Gather context
  myCwd = process.cwd();
  myGitRoot = await getGitRoot(myCwd);
  myHostname = process.env.CLAUDE_PEERS_HOSTNAME ?? await getHostname();
  const tty = getTty();

  log(`Hostname: ${myHostname}`);
  log(`CWD: ${myCwd}`);
  log(`Git root: ${myGitRoot ?? "(none)"}`);
  log(`TTY: ${tty ?? "(unknown)"}`);

  // 3. Generate initial summary via gpt-5.4-nano (non-blocking, best-effort)
  let initialSummary = "";
  const summaryPromise = (async () => {
    try {
      const branch = await getGitBranch(myCwd);
      const recentFiles = await getRecentFiles(myCwd);
      const summary = await generateSummary({
        cwd: myCwd,
        git_root: myGitRoot,
        git_branch: branch,
        recent_files: recentFiles,
      });
      if (summary) {
        initialSummary = summary;
        log(`Auto-summary: ${summary}`);
      }
    } catch (e) {
      log(`Auto-summary failed (non-critical): ${e instanceof Error ? e.message : String(e)}`);
    }
  })();

  // Wait briefly for summary, but don't block startup
  await Promise.race([summaryPromise, new Promise((r) => setTimeout(r, 3000))]);

  // 4. Register with broker using a stable ID (survives --resume)
  myStableId = computeStableId(myHostname, myCwd, tty);
  log(`Stable ID: ${myStableId} (hostname+cwd+tty)`);
  const reg = await brokerFetch<RegisterResponse>("/register", {
    pid: process.pid,
    hostname: myHostname,
    cwd: myCwd,
    git_root: myGitRoot,
    tty,
    summary: initialSummary,
    stable_id: myStableId,
  });
  myId = reg.id;
  log(`Registered as peer ${myId}`);

  // If summary generation is still running, update it when done
  if (!initialSummary) {
    summaryPromise.then(async () => {
      if (initialSummary && myId) {
        try {
          await brokerFetch("/set-summary", { id: myId, summary: initialSummary });
          log(`Late auto-summary applied: ${initialSummary}`);
        } catch {
          // Non-critical
        }
      }
    });
  }

  // 5. Connect MCP over stdio
  await mcp.connect(new StdioServerTransport());
  log("MCP connected");

  // 6. Start polling for inbound messages
  const pollTimer = setInterval(pollAndPushMessages, POLL_INTERVAL_MS);

  // 7. Start heartbeat (with auto-re-register on stale eviction)
  const heartbeatTimer = setInterval(async () => {
    if (!myId) {
      await ensureRegistered();
      return;
    }
    try {
      // Carry our identity so that, if we were evicted while quiet, the broker
      // restores us as ourselves instead of inventing a placeholder row.
      await brokerFetch("/heartbeat", {
        id: myId,
        pid: process.pid,
        hostname: myHostname,
        cwd: myCwd,
        git_root: myGitRoot,
        tty: getTty(),
      });
      // A successful heartbeat is not proof that our row still holds real
      // metadata: if we were evicted and the heartbeat recreated us as a ghost,
      // the row now says hostname "unknown". ensureRegistered() detects that and
      // re-registers, so the placeholder heals on its own within one interval.
      // Tool handlers call it too, but an idle session invokes no tools — and it
      // is exactly the idle ones that get evicted, so this loop has to cover it.
      await ensureRegistered();
    } catch {
      // Heartbeat failed — check if evicted and re-register
      await ensureRegistered();
    }
  }, HEARTBEAT_INTERVAL_MS);

  // 8. Clean up on exit
  const cleanup = async () => {
    clearInterval(pollTimer);
    clearInterval(heartbeatTimer);
    if (myId) {
      try {
        await brokerFetch("/unregister", { id: myId });
        log("Unregistered from broker");
      } catch {
        // Best effort
      }
    }
    process.exit(0);
  };

  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);
}

main().catch((e) => {
  log(`Fatal: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
});
