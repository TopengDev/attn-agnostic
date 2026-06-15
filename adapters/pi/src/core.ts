/**
 * attn pi adapter — pure, host-agnostic CORE.
 *
 * This module holds the entire inbound/outbound contract of the pi adapter:
 *   - the daemon WebSocket lifecycle (connect on session_start, close on
 *     session_shutdown — never from the extension factory; see bug #2860),
 *   - inbound-frame parsing → injection into the live pi turn,
 *   - the local-mesh send operations (send(name) / send('all') / peers) and the
 *     send-routing precedence (local-peer → relay-bypassed; everything else → relay).
 *
 * It depends on NOTHING from the pi runtime. The two things that touch the
 * outside world — the message host (what injects a user message into the live
 * turn) and the WebSocket constructor — are INJECTED via {@link AttnClientOptions}.
 * That is what makes the whole contract unit-testable with a stub host + a mock
 * daemon WS server, which is the acceptance proof here (pi itself cannot run in
 * this build environment).
 *
 * SECURITY: inbound message content (from / message / filename / path) is
 * UNTRUSTED for relay messages — an external agent controls it. The adapter
 * NEVER interprets it as a command: it only ever surfaces it as a plain user
 * message string for the agent to reason about (truncated for the notice). It is
 * data, not instructions. Local-mesh frames (`local:true`) are same-host /
 * same-user and therefore trusted. See the daemon's loopback bind + Host guard.
 */

/** The minimal slice of pi's ExtensionAPI the adapter actually drives. */
export interface MessageHost {
  /**
   * Inject a message into the live pi session.
   * `deliverAs:'steer'` interleaves into the current turn (after in-flight tool
   * calls, before the next LLM call); `'followUp'` waits for idle.
   */
  sendUserMessage(text: string, opts?: { deliverAs?: DeliverAs }): unknown;
}

export type DeliverAs = 'steer' | 'followUp';

/**
 * The minimal WebSocket surface the core uses. Both the `ws` package's
 * WebSocket and the test's mock satisfy it (callers cast at the boundary so the
 * core never imports `ws`).
 */
export interface WSLike {
  readyState: number;
  send(data: string): void;
  close(): void;
  on(event: 'open', listener: () => void): void;
  on(event: 'message', listener: (data: unknown) => void): void;
  on(event: 'close', listener: () => void): void;
  on(event: 'error', listener: (err: unknown) => void): void;
}

/** Constructs a WebSocket for a URL. In pi: `(url) => new WebSocket(url)`. */
export type WSFactory = (url: string) => WSLike;

/** WebSocket.OPEN — inlined so the core stays free of any `ws` import. */
export const WS_OPEN = 1;

/**
 * The inbound frame the daemon pushes (the subset we read). Mirrors
 * `internal/httpapi/ws.go#surfaceToFrame`. Relay inbound has no `local`;
 * local-mesh inbound carries `local:true` + `trust:"local"`.
 */
export interface InboundFrame {
  type?: string;
  from?: string;
  message?: string;
  filename?: string;
  path?: string;
  size?: number;
  id?: string;
  ts?: number;
  trust?: string;
  agentName?: string;
  groupId?: string;
  groupName?: string;
  local?: boolean;
}

export interface LocalPeersResponse {
  sessions: string[];
  count: number;
}

/** Result of a routed outbound send. */
export interface SendResult {
  via: 'local' | 'broadcast' | 'relay';
  to?: string;
  id?: string;
  status?: string;
}

export interface AttnClientOptions {
  /** ATTN_SESSION — this session's registry name; also filtered out of /local-peers. */
  session: string | null;
  /** Harness label sent as `?harness=`. Default 'pi'. */
  harness?: string;
  /** Daemon HTTP base. Default 'http://127.0.0.1:9742'. */
  httpBase?: string;
  /** Daemon WS base. Default 'ws://127.0.0.1:9742'. */
  wsBase?: string;
  /** Injection target (pi.sendUserMessage). */
  host: MessageHost;
  /** WebSocket constructor (cast `new WebSocket(url)` to WSLike at the call site). */
  wsFactory: WSFactory;
  /** Fetch implementation. Default the global fetch (Node >=18). */
  fetchFn?: typeof fetch;
  /** Injection mode for inbound frames. Default 'steer'. */
  deliverAs?: DeliverAs;
  /** Reconnect backoff in ms. Default 5000; <=0 disables auto-reconnect. */
  reconnectMs?: number;
  /** Inbound message preview length before truncation. Default 300. */
  maxMessageLen?: number;
  /** Optional diagnostic logger. Default no-op. */
  log?: (msg: string) => void;
}

/**
 * The adapter's connection + routing engine. One instance per pi session.
 * Lifecycle: `new AttnMeshClient(opts)` in the extension factory (cheap, no I/O),
 * `start()` from `session_start`, `stop()` from `session_shutdown`.
 */
export class AttnMeshClient {
  readonly session: string | null;
  private readonly harness: string;
  private readonly httpBase: string;
  private readonly wsBase: string;
  private readonly host: MessageHost;
  private readonly wsFactory: WSFactory;
  private readonly fetchFn: typeof fetch;
  private readonly deliverAs: DeliverAs;
  private readonly reconnectMs: number;
  private readonly maxMessageLen: number;
  private readonly log: (msg: string) => void;

  private ws: WSLike | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;

  /** Sender of the most recent inbound message (session name or address) — used by attn_reply. */
  lastInboundFrom: string | null = null;
  /** Cache of local peer session names (minus self), refreshed by fetchLocalPeers(). */
  localPeers: string[] = [];

  constructor(opts: AttnClientOptions) {
    this.session = opts.session;
    this.harness = opts.harness ?? 'pi';
    this.httpBase = (opts.httpBase ?? 'http://127.0.0.1:9742').replace(/\/+$/, '');
    this.wsBase = (opts.wsBase ?? 'ws://127.0.0.1:9742').replace(/\/+$/, '');
    this.host = opts.host;
    this.wsFactory = opts.wsFactory;
    this.fetchFn = opts.fetchFn ?? (globalThis.fetch as typeof fetch);
    this.deliverAs = opts.deliverAs ?? 'steer';
    this.reconnectMs = opts.reconnectMs ?? 5000;
    this.maxMessageLen = opts.maxMessageLen ?? 300;
    this.log = opts.log ?? (() => {});
    if (!this.fetchFn) {
      throw new Error('AttnMeshClient: no fetch available — pass opts.fetchFn (Node >=18 has a global fetch)');
    }
  }

  /**
   * The exact WS URL the daemon expects: `ws://host/?session=<NAME>&harness=pi`.
   * The connection IS the registry entry (Layer A WS self-registration). An
   * anonymous connection (no session) still receives broadcasts but cannot
   * originate a local send.
   */
  connectUrl(): string {
    const params = new URLSearchParams();
    if (this.session) params.set('session', this.session);
    params.set('harness', this.harness);
    return `${this.wsBase}/?${params.toString()}`;
  }

  /** True when the WS is open and a local send can be originated. */
  isConnected(): boolean {
    return !!this.ws && this.ws.readyState === WS_OPEN;
  }

  /**
   * Open the daemon WS. Call from `session_start` (NOT the extension factory —
   * pi.sendUserMessage is silently dropped right after ctx.newSession(), bug
   * #2860). Idempotent: a second start() while already connected reconnects.
   */
  start(): void {
    this.stopped = false;
    if (!this.session) {
      this.log('[attn] WARNING: ATTN_SESSION not set — connecting anonymously (cannot originate local-mesh sends)');
    }
    this.connect();
  }

  /**
   * Close the WS and cancel any pending reconnect. Call from `session_shutdown`.
   * Idempotent and safe to call when never started.
   */
  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.closeSocket();
  }

  private closeSocket(): void {
    if (this.ws) {
      try {
        this.ws.close();
      } catch {
        /* already closing */
      }
      this.ws = null;
    }
  }

  private connect(): void {
    if (this.stopped) return;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.closeSocket();

    let socket: WSLike;
    try {
      socket = this.wsFactory(this.connectUrl());
    } catch (err) {
      this.log(`[attn] ws construct failed: ${errMsg(err)} — retrying`);
      this.scheduleReconnect();
      return;
    }
    this.ws = socket;

    socket.on('open', () => {
      this.log(`[attn] daemon ws connected (session=${this.session ?? '(anon)'})`);
    });
    socket.on('message', (data) => {
      // Defensive: a thrown handler must never kill the socket.
      try {
        this.handleFrame(data);
      } catch (err) {
        this.log(`[attn] frame handler error: ${errMsg(err)}`);
      }
    });
    socket.on('close', () => {
      this.ws = null;
      this.scheduleReconnect();
    });
    socket.on('error', (err) => {
      this.log(`[attn] ws error: ${errMsg(err)}`);
      this.ws = null;
      this.scheduleReconnect();
    });
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectMs <= 0 || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, this.reconnectMs);
  }

  // ── Inbound (daemon → live pi turn) ────────────────────────────────────────

  /**
   * Parse one daemon frame and inject it into the live turn. Parse failures and
   * unknown frame types are ignored (untrusted input must never throw out of
   * here). `local:true` renders with a 💻 Local: prefix to distinguish a
   * same-host mesh message from an external relay message.
   */
  handleFrame(raw: unknown): void {
    let msg: InboundFrame;
    try {
      msg = JSON.parse(decode(raw)) as InboundFrame;
    } catch {
      return; // not JSON — ignore
    }
    if (!msg || typeof msg !== 'object') return;

    // File message (daemon already downloaded + decrypted + saved to disk).
    if (msg.type === 'file' && msg.from && msg.path) {
      this.lastInboundFrom = msg.from;
      const name = msg.agentName || msg.from;
      const sizeKB = Math.round((msg.size || 0) / 1024);
      const prefix = msg.local ? '💻 Local: ' : '';
      this.inject(
        `[attn] 📎 ${prefix}File from ${name}: ${msg.filename ?? '(file)'} (${sizeKB} KB)\nSaved to: ${msg.path}`,
      );
      return;
    }

    if (msg.type === 'message' && msg.from && msg.message) {
      this.lastInboundFrom = msg.from;
      const name = msg.agentName || msg.from;

      // Reactions are surfaced as a one-line notice, not a full message block.
      if (msg.trust === 'reaction') {
        this.inject(`[attn] ${name} reacted ${msg.message} to a message`);
        return;
      }

      let prefix = '';
      if (msg.local) prefix = '💻 Local: ';
      else if (msg.trust === 'pending') prefix = '⚠️ Pending: ';
      else if (msg.groupId) prefix = `[${msg.groupName || msg.groupId}] `;

      const truncated =
        msg.message.length > this.maxMessageLen
          ? msg.message.slice(0, this.maxMessageLen) + '...'
          : msg.message;

      this.inject(
        `[attn] ${prefix}Message from ${name}:\n\n${truncated}\n\n---\nUse attn_reply or attn_send to respond.`,
      );
      return;
    }

    // {type:'local-ack'} and anything else: tracked silently / ignored.
  }

  private inject(text: string): void {
    try {
      this.host.sendUserMessage(text, { deliverAs: this.deliverAs });
    } catch (err) {
      this.log(`[attn] inject failed: ${errMsg(err)}`);
    }
  }

  // ── Outbound mesh (live pi → daemon) ────────────────────────────────────────

  /** GET /local-peers, refresh + return the cache (sessions minus self). */
  async fetchLocalPeers(): Promise<LocalPeersResponse> {
    const data = (await this.get('/local-peers')) as LocalPeersResponse;
    const sessions = Array.isArray(data?.sessions) ? data.sessions : [];
    this.localPeers = sessions.filter((s) => s !== this.session);
    return { sessions, count: typeof data?.count === 'number' ? data.count : sessions.length };
  }

  /**
   * Emit a local-mesh send frame over the WS. `to` is a peer session name or
   * `'all'` (broadcast minus self). This is relay-bypassed at the daemon. The
   * daemon replies with a `{type:'local-ack'}` (tracked silently inbound).
   */
  sendLocalFrame(to: string, message: string): { sent: boolean; reason?: string } {
    if (!this.isConnected()) return { sent: false, reason: 'ws not connected' };
    this.ws!.send(JSON.stringify({ type: 'local', to, message }));
    return { sent: true };
  }

  /** Broadcast to every local peer (minus self). Closes the prototype's send('all') parity gap. */
  broadcastLocal(message: string): { sent: boolean; reason?: string } {
    return this.sendLocalFrame('all', message);
  }

  /**
   * Route a send the way CC/pi does:
   *   - `to === 'all'`  → local broadcast (relay bypassed),
   *   - `to` is a known local peer name → local (relay bypassed),
   *   - otherwise (address / .attn name / unknown) → relay via POST /send.
   * A bare name that is NOT a live local session falls through to the relay,
   * matching the daemon's own routing precedence.
   */
  async send(to: string, message: string): Promise<SendResult> {
    if (to === 'all') {
      const r = this.broadcastLocal(message);
      if (!r.sent) throw new Error(`broadcast failed: ${r.reason}`);
      return { via: 'broadcast', to: 'all' };
    }

    // Refresh the peer cache to decide local vs relay. If the daemon is
    // unreachable for this read, fall through to a relay attempt.
    try {
      await this.fetchLocalPeers();
    } catch {
      /* daemon read failed — try relay below */
    }

    if (this.localPeers.includes(to)) {
      const r = this.sendLocalFrame(to, message);
      if (!r.sent) throw new Error(`local peer "${to}" found but ${r.reason}`);
      return { via: 'local', to };
    }

    const data = (await this.post('/send', { to, message })) as { id?: string; status?: string };
    return { via: 'relay', to, id: data?.id, status: data?.status };
  }

  // ── Thin daemon HTTP helpers (centralized so every call shares error shape) ──

  /** GET <httpBase><path> → parsed JSON. Throws on non-2xx. */
  async get(path: string): Promise<unknown> {
    const res = await this.fetchFn(`${this.httpBase}${path}`);
    if (!res.ok) throw new Error((await safeText(res)) || `HTTP ${res.status}`);
    return res.json();
  }

  /** POST <httpBase><path> with a JSON body → parsed JSON. Throws on non-2xx. */
  async post(path: string, body: unknown): Promise<unknown> {
    const res = await this.fetchFn(`${this.httpBase}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error((await safeText(res)) || `HTTP ${res.status}`);
    return res.json();
  }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/** Coerce a WS message payload (string | Buffer | …) to a UTF-8 string. */
function decode(raw: unknown): string {
  return typeof raw === 'string' ? raw : String(raw);
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

async function safeText(res: Response): Promise<string> {
  try {
    return await res.text();
  } catch {
    return '';
  }
}
