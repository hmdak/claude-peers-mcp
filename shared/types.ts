// Unique ID for each Claude Code instance (generated on registration)
export type PeerId = string;

export interface Peer {
  id: PeerId;
  pid: number;
  hostname: string;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  summary: string;
  registered_at: string; // ISO timestamp
  last_seen: string; // ISO timestamp
}

// `pushed` (added by a broker migration) marks "already sent as a channel
// notification". It is separate from `delivered`, which means "acknowledged by
// poll-messages / check_messages". Optional: older brokers omit the column.
export interface Message {
  pushed?: number;
  id: number;
  from_id: PeerId;
  to_id: PeerId;
  text: string;
  sent_at: string; // ISO timestamp
  delivered: boolean;
}

// --- Broker API types ---

export interface RegisterRequest {
  pid: number;
  hostname: string;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  summary: string;
  // Optional stable ID derived from hostname+cwd+tty. When provided, the broker
  // reuses/updates the peer with this ID across `--resume` so the peer ID stays
  // constant and other peers' saved to_id / queued messages remain valid.
  stable_id?: PeerId;
}

export interface RegisterResponse {
  id: PeerId;
}

export interface HeartbeatRequest {
  id: PeerId;
  // Identity, so an evicted-but-alive peer can be restored as itself.
  // The broker deletes peers that go quiet for 60s (a sleeping laptop is
  // enough). Without these fields it has nothing to rebuild the row from and
  // has to invent a placeholder — that is where "unknown"/pid 0 rows came from.
  // Optional so an older client still heartbeats fine against a newer broker.
  pid?: number;
  hostname?: string;
  cwd?: string;
  git_root?: string | null;
  tty?: string | null;
  summary?: string;
}

export interface SetSummaryRequest {
  id: PeerId;
  summary: string;
}

export interface ListPeersRequest {
  scope: "machine" | "directory" | "repo";
  // The requesting peer's context (used for filtering)
  cwd: string;
  git_root: string | null;
  exclude_id?: PeerId;
}

export interface SendMessageRequest {
  from_id: PeerId;
  to_id: PeerId;
  text: string;
}

export interface PollMessagesRequest {
  id: PeerId;
}

export interface PollMessagesResponse {
  messages: Message[];
}
