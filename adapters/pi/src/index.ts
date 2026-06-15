/**
 * attn pi adapter — pi extension entry point (Layer B for the pi harness).
 *
 * This is a THIN shim: it wires pi's ExtensionAPI into the host-agnostic
 * {@link AttnMeshClient} (see core.ts, which holds all the logic and is the
 * unit-tested unit) and exposes the attn tool surface. It targets OUR Go daemon
 * `attnd` on 127.0.0.1:9742 (M2 REST + WS, M3 Layer A local mesh).
 *
 * Lifecycle (honors pi bug #2860 — sendUserMessage is dropped right after
 * ctx.newSession()): the client is constructed in the factory (cheap, no I/O);
 * the WS opens on `session_start` and closes on `session_shutdown`. The daemon
 * is a SEPARATE Go process the user runs (`attnd` / `go run ./cmd/attnd`) — this
 * adapter does not spawn it; it connects and auto-reconnects until it is up.
 *
 * Pinned for pi: `earendil-works/pi` (MIT, Node >=18). Runtime dep: `ws`.
 */
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import { Type } from 'typebox';
import WebSocket from 'ws';

import { AttnMeshClient, type WSLike } from './core';

const HTTP_BASE = process.env.ATTN_DAEMON_HTTP || 'http://127.0.0.1:9742';
const WS_BASE = process.env.ATTN_DAEMON_WS || 'ws://127.0.0.1:9742';
const ATTN_SESSION = process.env.ATTN_SESSION || null;

export default function (pi: ExtensionAPI): void {
  // Construct in the factory (no network here — bug #2860). The WS is opened in
  // session_start below.
  const client = new AttnMeshClient({
    session: ATTN_SESSION,
    harness: 'pi',
    httpBase: HTTP_BASE,
    wsBase: WS_BASE,
    host: { sendUserMessage: (text, opts) => pi.sendUserMessage(text, opts) },
    // `ws` satisfies the WSLike subset; cast at this single boundary so the core
    // never imports `ws`.
    wsFactory: (url) => new WebSocket(url) as unknown as WSLike,
    fetchFn: fetch,
    deliverAs: 'steer',
  });

  pi.on('session_start', () => {
    client.start();
  });

  pi.on('session_shutdown', () => {
    client.stop();
  });

  // ── Tools ─────────────────────────────────────────────────────────────────

  // attn_send — send to another agent. Routing (handled in core.send):
  //   to:'all' → local broadcast · a live local peer name → local (relay
  //   bypassed) · address / .attn name / unknown → relay.
  pi.registerTool({
    name: 'attn_send',
    label: 'attn Send',
    description:
      'Send a message via attn. Use a local session name for a same-machine peer, "all" to broadcast to every local session, or an Ethereum address (0x...) / .attn name for an external agent (relayed, encrypted).',
    parameters: Type.Object({
      to: Type.String({ description: 'Local session name, "all", an 0x address, or a .attn name' }),
      message: Type.String({ description: 'Message text to send' }),
    }),
    async execute(_id, params: { to: string; message: string }) {
      try {
        const r = await client.send(params.to, params.message);
        const text =
          r.via === 'broadcast'
            ? 'Broadcast sent to all local sessions'
            : r.via === 'local'
              ? `Local message sent to ${r.to}`
              : `Message sent to ${r.to}${r.id ? ` (id: ${r.id})` : ''}`;
        return { content: [{ type: 'text' as const, text }], details: r };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { content: [{ type: 'text' as const, text: `Failed to send: ${msg}` }], details: { error: msg } };
      }
    },
  });

  // attn_broadcast — explicit convenience wrapper over send('all').
  pi.registerTool({
    name: 'attn_broadcast',
    label: 'attn Broadcast',
    description: 'Broadcast a message to every other attn session on this machine (local mesh, relay bypassed).',
    parameters: Type.Object({
      message: Type.String({ description: 'Message text to broadcast' }),
    }),
    async execute(_id, params: { message: string }) {
      try {
        await client.send('all', params.message);
        return { content: [{ type: 'text' as const, text: 'Broadcast sent to all local sessions' }], details: { via: 'broadcast' } };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { content: [{ type: 'text' as const, text: `Failed to broadcast: ${msg}` }], details: { error: msg } };
      }
    },
  });

  // attn_local_peers — list locally connected attn sessions (this machine).
  pi.registerTool({
    name: 'attn_local_peers',
    label: 'attn Local Peers',
    description: 'List attn sessions running locally on this machine (worker tabs, other agents). For external contacts use attn_peers.',
    parameters: Type.Object({}),
    async execute() {
      try {
        const result = await client.fetchLocalPeers();
        if (result.count === 0) {
          return { content: [{ type: 'text' as const, text: 'No local attn sessions connected.' }], details: result };
        }
        const lines = result.sessions.map(
          (s) => `  ${s === ATTN_SESSION ? '→ ' : '  '}${s}${s === ATTN_SESSION ? ' (this session)' : ''}`,
        );
        return {
          content: [{ type: 'text' as const, text: `Local attn sessions (${result.count}):\n${lines.join('\n')}` }],
          details: result,
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { content: [{ type: 'text' as const, text: `Failed to fetch local peers: ${msg}` }], details: { error: msg } };
      }
    },
  });

  // attn_peers — list contacts / known external agents (GET /peers).
  pi.registerTool({
    name: 'attn_peers',
    label: 'attn Peers',
    description: 'List your attn contacts and known external agents. For same-machine sessions use attn_local_peers.',
    parameters: Type.Object({}),
    async execute() {
      try {
        const result = (await client.get('/peers')) as {
          peers: Array<{ address: string; name: string | null; added_at: string }>;
        };
        if (!result.peers || result.peers.length === 0) {
          return { content: [{ type: 'text' as const, text: 'No contacts yet.' }], details: result };
        }
        const lines = result.peers.map((p) => `  ${p.name || p.address} (${p.address}) — added ${p.added_at}`);
        return {
          content: [{ type: 'text' as const, text: `Contacts (${result.peers.length}):\n${lines.join('\n')}` }],
          details: result,
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { content: [{ type: 'text' as const, text: `Failed to fetch peers: ${msg}` }], details: { error: msg } };
      }
    },
  });

  // attn_reply — reply to the most recent inbound message.
  pi.registerTool({
    name: 'attn_reply',
    label: 'attn Reply',
    description: 'Reply to the most recent inbound attn message (local or external).',
    parameters: Type.Object({
      message: Type.String({ description: 'Reply message text' }),
    }),
    async execute(_id, params: { message: string }) {
      const to = client.lastInboundFrom;
      if (!to) {
        return { content: [{ type: 'text' as const, text: 'No recent inbound message to reply to.' }], details: {} };
      }
      try {
        const r = await client.send(to, params.message);
        return { content: [{ type: 'text' as const, text: `Reply sent to ${to}` }], details: r };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { content: [{ type: 'text' as const, text: `Failed to reply: ${msg}` }], details: { error: msg } };
      }
    },
  });

  // attn_history — fetch message history with a peer (GET /history).
  pi.registerTool({
    name: 'attn_history',
    label: 'attn History',
    description: 'Fetch recent message history with a specific agent, address, or group.',
    parameters: Type.Object({
      with: Type.String({ description: 'Agent address, .attn name, or group ID' }),
      limit: Type.Optional(Type.Number({ description: 'Number of messages (default: 20)' })),
    }),
    async execute(_id, params: { with: string; limit?: number }) {
      try {
        const limit = params.limit ?? 20;
        const result = (await client.get(
          `/history?with=${encodeURIComponent(params.with)}&limit=${limit}`,
        )) as { messages: Array<{ direction: string; content: string; ts: string }> };
        if (!result.messages || result.messages.length === 0) {
          return { content: [{ type: 'text' as const, text: `No history with ${params.with}.` }], details: result };
        }
        const lines = result.messages.map((m) => {
          const dir = m.direction === 'inbound' ? '←' : '→';
          const preview = m.content.length > 150 ? m.content.slice(0, 150) + '...' : m.content;
          return `  ${dir} [${m.ts}] ${preview}`;
        });
        return {
          content: [{ type: 'text' as const, text: `History with ${params.with} (${result.messages.length}):\n${lines.join('\n')}` }],
          details: result,
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { content: [{ type: 'text' as const, text: `Failed to fetch history: ${msg}` }], details: { error: msg } };
      }
    },
  });

  // attn_status — daemon + relay + connection status.
  pi.registerTool({
    name: 'attn_status',
    label: 'attn Status',
    description: 'Check the attn daemon, relay connection, and this session\'s local-mesh WS link.',
    parameters: Type.Object({}),
    async execute() {
      try {
        const result = (await client.get('/status')) as {
          address: string;
          relayConnected: boolean;
          peers: number;
        };
        const text = [
          'attn daemon: running',
          `Address: ${result.address}`,
          `Relay: ${result.relayConnected ? 'connected' : 'disconnected'}`,
          `Contacts: ${result.peers}`,
          `Local mesh WS: ${client.isConnected() ? 'connected' : 'connecting…'}`,
        ].join('\n');
        return { content: [{ type: 'text' as const, text }], details: { ...result, wsConnected: client.isConnected() } };
      } catch {
        return {
          content: [
            {
              type: 'text' as const,
              text: 'attn daemon is not reachable on 127.0.0.1:9742. Start it (e.g. `attnd` or `go run ./cmd/attnd`); the adapter will auto-connect.',
            },
          ],
          details: { running: false, wsConnected: client.isConnected() },
        };
      }
    },
  });
}
