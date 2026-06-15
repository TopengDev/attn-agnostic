/**
 * Mock-daemon WS test for the attn pi adapter core.
 *
 * pi cannot run in this build environment, so the acceptance proof is here: a
 * real WebSocket server (the `ws` package) + a real HTTP server stand in for
 * `attnd`'s 127.0.0.1:9742 contract, the core connects to it over a real socket,
 * and we assert the two contract-critical behaviors the brief calls out:
 *   1. an inbound {type:'message',...} frame → host.sendUserMessage(text,{deliverAs:'steer'}),
 *   2. a mesh send('all') → the daemon receives {type:'local',to:'all',message}.
 * Plus: relay-vs-local routing precedence, self-filtering, file frames, the
 * connect URL (session+harness), reconnect, and untrusted/malformed-frame safety.
 *
 * Run: `npm test` (tsc --noEmit gate + tsc -p tsconfig.test.json + node --test).
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { WebSocketServer, WebSocket } from 'ws';

import { AttnMeshClient, type WSLike, type MessageHost } from '../src/core';

// ── Mock daemon (HTTP + WS on one port, mirroring attnd) ───────────────────────

interface MockDaemon {
  port: number;
  received: unknown[]; // client→server WS frames (parsed)
  relaySends: unknown[]; // bodies POSTed to /send
  connInfo(): { count: number; url: string | null };
  setPeers(p: string[]): void;
  push(frame: unknown): void; // server→client WS frame
  pushRaw(s: string): void; // server→client raw (non-JSON) payload
  dropSocket(): void; // close the server side to exercise reconnect
  close(): Promise<void>;
}

async function startMockDaemon(initialPeers: string[] = []): Promise<MockDaemon> {
  let peers = [...initialPeers];
  const received: unknown[] = [];
  const relaySends: unknown[] = [];
  let sock: WebSocket | null = null;
  let lastConnUrl: string | null = null;
  let connCount = 0;

  const http: Server = createServer((req, res) => {
    if (req.method === 'GET' && req.url && req.url.startsWith('/local-peers')) {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ sessions: peers, count: peers.length }));
      return;
    }
    if (req.method === 'POST' && req.url && req.url.startsWith('/send')) {
      let body = '';
      req.on('data', (c) => (body += c));
      req.on('end', () => {
        try {
          relaySends.push(JSON.parse(body));
        } catch {
          relaySends.push(body);
        }
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ id: 'relay-1', status: 'queued' }));
      });
      return;
    }
    res.statusCode = 404;
    res.end('not found');
  });

  const wss = new WebSocketServer({ server: http });
  wss.on('connection', (ws, req) => {
    connCount++;
    sock = ws;
    lastConnUrl = req.url ?? null;
    ws.on('message', (data) => {
      try {
        received.push(JSON.parse(data.toString()));
      } catch {
        /* ignore non-JSON from client */
      }
    });
  });

  await new Promise<void>((resolve) => http.listen(0, '127.0.0.1', () => resolve()));
  const port = (http.address() as AddressInfo).port;

  return {
    port,
    received,
    relaySends,
    connInfo: () => ({ count: connCount, url: lastConnUrl }),
    setPeers: (p) => {
      peers = [...p];
    },
    push: (frame) => sock?.send(JSON.stringify(frame)),
    pushRaw: (s) => sock?.send(s),
    dropSocket: () => sock?.close(),
    close: () =>
      new Promise<void>((resolve) => {
        wss.close(() => http.close(() => resolve()));
      }),
  };
}

// ── helpers ────────────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(cond: () => boolean, timeoutMs = 3000, intervalMs = 10): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor: condition not met within timeout');
    await sleep(intervalMs);
  }
}

interface RecordingHost extends MessageHost {
  calls: Array<{ text: string; opts?: { deliverAs?: string } }>;
}

function recordingHost(): RecordingHost {
  const calls: RecordingHost['calls'] = [];
  return {
    calls,
    sendUserMessage(text: string, opts?: { deliverAs?: 'steer' | 'followUp' }) {
      calls.push({ text, opts });
      return undefined;
    },
  };
}

function makeClient(d: MockDaemon, host: MessageHost, session = 'pi-test', reconnectMs = 5000): AttnMeshClient {
  return new AttnMeshClient({
    session,
    harness: 'pi',
    httpBase: `http://127.0.0.1:${d.port}`,
    wsBase: `ws://127.0.0.1:${d.port}`,
    host,
    wsFactory: (url) => new WebSocket(url) as unknown as WSLike,
    fetchFn: fetch,
    reconnectMs,
  });
}

// ── tests ──────────────────────────────────────────────────────────────────────

test('inbound local message frame → pi.sendUserMessage steered with 💻 Local prefix', async () => {
  const d = await startMockDaemon();
  const host = recordingHost();
  const client = makeClient(d, host);
  client.start();
  await waitFor(() => client.isConnected());

  d.push({ type: 'message', from: 'main', message: 'hello mesh', local: true, trust: 'local', id: '1', ts: 1 });
  await waitFor(() => host.calls.length > 0);

  const call = host.calls[0];
  assert.match(call.text, /💻 Local:/, 'local frame should render the 💻 Local: prefix');
  assert.match(call.text, /Message from main/);
  assert.match(call.text, /hello mesh/);
  assert.deepEqual(call.opts, { deliverAs: 'steer' }, 'must inject with deliverAs:steer');
  assert.equal(client.lastInboundFrom, 'main');

  client.stop();
  await d.close();
});

test('inbound relay (external) message frame has NO 💻 Local prefix', async () => {
  const d = await startMockDaemon();
  const host = recordingHost();
  const client = makeClient(d, host);
  client.start();
  await waitFor(() => client.isConnected());

  d.push({ type: 'message', from: 'alice.attn', message: 'external hi', id: '2', ts: 2 });
  await waitFor(() => host.calls.length > 0);

  const call = host.calls[0];
  assert.doesNotMatch(call.text, /💻 Local:/);
  assert.match(call.text, /external hi/);
  assert.deepEqual(call.opts, { deliverAs: 'steer' });

  client.stop();
  await d.close();
});

test('connect URL carries ?session= and &harness=pi (WS self-registration)', async () => {
  const d = await startMockDaemon();
  const client = makeClient(d, recordingHost());
  client.start();
  await waitFor(() => d.connInfo().count > 0);

  const { url } = d.connInfo();
  assert.ok(url, 'server should have seen a connection URL');
  assert.match(url!, /session=pi-test/);
  assert.match(url!, /harness=pi/);

  client.stop();
  await d.close();
});

test("send('all') emits {type:'local',to:'all',message}", async () => {
  const d = await startMockDaemon();
  const client = makeClient(d, recordingHost());
  client.start();
  await waitFor(() => client.isConnected());

  const r = await client.send('all', 'broadcast hi');
  assert.equal(r.via, 'broadcast');

  await waitFor(() => d.received.some((f) => (f as any)?.type === 'local' && (f as any)?.to === 'all'));
  const frame = d.received.find((f) => (f as any)?.type === 'local' && (f as any)?.to === 'all') as any;
  assert.equal(frame.message, 'broadcast hi');
  assert.equal(d.relaySends.length, 0, 'broadcast must bypass the relay');

  client.stop();
  await d.close();
});

test('send(local peer name) routes local (relay bypassed)', async () => {
  const d = await startMockDaemon(['peerX']);
  const client = makeClient(d, recordingHost());
  client.start();
  await waitFor(() => client.isConnected());

  const r = await client.send('peerX', 'hi local');
  assert.equal(r.via, 'local');
  assert.equal(r.to, 'peerX');

  await waitFor(() => d.received.some((f) => (f as any)?.type === 'local' && (f as any)?.to === 'peerX'));
  const frame = d.received.find((f) => (f as any)?.type === 'local' && (f as any)?.to === 'peerX') as any;
  assert.equal(frame.message, 'hi local');
  assert.equal(d.relaySends.length, 0, 'a live local peer must not hit the relay');

  client.stop();
  await d.close();
});

test('send(unknown / .attn name) falls through to the relay (POST /send)', async () => {
  const d = await startMockDaemon(['peerX']);
  const client = makeClient(d, recordingHost());
  client.start();
  await waitFor(() => client.isConnected());

  const r = await client.send('bob.attn', 'yo');
  assert.equal(r.via, 'relay');
  assert.equal(r.id, 'relay-1');
  assert.equal(r.status, 'queued');

  assert.equal(d.relaySends.length, 1);
  assert.deepEqual(d.relaySends[0], { to: 'bob.attn', message: 'yo' });
  assert.ok(
    !d.received.some((f) => (f as any)?.type === 'local'),
    'a non-local target must not emit a local frame',
  );

  client.stop();
  await d.close();
});

test('fetchLocalPeers filters out this session', async () => {
  const d = await startMockDaemon(['pi-test', 'peerX', 'peerY']);
  const client = makeClient(d, recordingHost());
  // No WS needed for an HTTP read, but start for realism.
  client.start();
  await waitFor(() => client.isConnected());

  const res = await client.fetchLocalPeers();
  assert.deepEqual(res.sessions, ['pi-test', 'peerX', 'peerY']);
  assert.equal(res.count, 3);
  assert.deepEqual(client.localPeers, ['peerX', 'peerY']);

  client.stop();
  await d.close();
});

test('inbound file frame → 📎 injection with Local prefix and saved path', async () => {
  const d = await startMockDaemon();
  const host = recordingHost();
  const client = makeClient(d, host);
  client.start();
  await waitFor(() => client.isConnected());

  d.push({ type: 'file', from: 'main', path: '/tmp/a.png', filename: 'a.png', size: 2048, local: true });
  await waitFor(() => host.calls.length > 0);

  const call = host.calls[0];
  assert.match(call.text, /📎/);
  assert.match(call.text, /💻 Local:/);
  assert.match(call.text, /a\.png/);
  assert.match(call.text, /Saved to: \/tmp\/a\.png/);

  client.stop();
  await d.close();
});

test('malformed / incomplete inbound frames are ignored (untrusted-input safety)', async () => {
  const d = await startMockDaemon();
  const host = recordingHost();
  const client = makeClient(d, host);
  client.start();
  await waitFor(() => client.isConnected());

  d.pushRaw('not json {{{'); // not JSON
  d.push({ type: 'message' }); // missing from/message
  d.push({ type: 'local-ack', to: 'main', delivered: true }); // ack — tracked silently
  d.push({ type: 'message', from: 'main', message: 'real one', id: '9', ts: 9 }); // the only valid one

  await waitFor(() => host.calls.length > 0);
  await sleep(80); // give any erroneous extra injections a chance to land
  assert.equal(host.calls.length, 1, 'only the one valid message frame should inject');
  assert.match(host.calls[0].text, /real one/);

  client.stop();
  await d.close();
});

test('self-echo frame (from === own session) is NOT injected (audit M3)', async () => {
  const d = await startMockDaemon();
  const host = recordingHost();
  const client = makeClient(d, host); // session defaults to 'pi-test'
  client.start();
  await waitFor(() => client.isConnected());

  // A frame claiming to come from THIS session must be dropped (injection-loop
  // guard); a frame from a real peer with the same body must still inject.
  d.push({ type: 'message', from: 'pi-test', message: 'my own echo', local: true, id: 'se1' });
  d.push({ type: 'message', from: 'main', message: 'peer message', local: true, id: 'se2' });

  await waitFor(() => host.calls.length > 0);
  await sleep(80); // let any erroneous extra injection land
  assert.equal(host.calls.length, 1, 'only the peer frame should inject (self-echo dropped)');
  assert.match(host.calls[0].text, /peer message/);
  assert.equal(client.lastInboundFrom, 'main', 'self-echo must not even update lastInboundFrom');

  client.stop();
  await d.close();
});

test('reaction frame → one-line notice carrying reactionMessageId (audit M6)', async () => {
  const d = await startMockDaemon();
  const host = recordingHost();
  const client = makeClient(d, host);
  client.start();
  await waitFor(() => client.isConnected());

  d.push({
    type: 'message',
    from: 'alice.attn',
    message: '👍',
    trust: 'reaction',
    reactionMessageId: 'm42',
    id: 'rx1',
  });
  await waitFor(() => host.calls.length > 0);

  const txt = host.calls[0].text;
  assert.match(txt, /reacted/);
  assert.match(txt, /👍/);
  assert.match(txt, /m42/, 'reactionMessageId must be threaded into the notice');
  assert.doesNotMatch(txt, /Message from/, 'a reaction must be a notice, not a full message block');

  client.stop();
  await d.close();
});

test('auto-reconnects after the socket drops', async () => {
  const d = await startMockDaemon();
  const client = makeClient(d, recordingHost(), 'pi-test', 120);
  client.start();
  await waitFor(() => d.connInfo().count === 1);

  d.dropSocket();
  await waitFor(() => d.connInfo().count >= 2, 4000);

  client.stop();
  await d.close();
});

test('stop() cancels reconnect (no further connections)', async () => {
  const d = await startMockDaemon();
  const client = makeClient(d, recordingHost(), 'pi-test', 120);
  client.start();
  await waitFor(() => client.isConnected());

  client.stop();
  const before = d.connInfo().count;
  await sleep(500);
  assert.equal(d.connInfo().count, before, 'no new connection should be made after stop()');

  await d.close();
});
