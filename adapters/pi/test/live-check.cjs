/**
 * LIVE cross-check: the REAL adapter core against the REAL Go daemon (attnd).
 *
 * pi cannot run here, so this proves the wire contract end-to-end without pi by
 * driving the actual AttnMeshClient (compiled to dist-test/src/core.js) against a
 * live `attnd` on 127.0.0.1:$LIVE_PORT, with a second raw-WS "driver" session
 * standing in for another local agent. It asserts:
 *   1. the adapter self-registers over WS (shows up in /local-peers),
 *   2. a driver→pi-test local send is routed (relay-bypassed) and the adapter
 *      parses it into the exact injection it would hand pi (from=driver, local,
 *      deliverAs:steer),
 *   3. the adapter's send('all') broadcast reaches the driver as a real daemon
 *      frame (local:true, trust:local, from=pi-test) and EXCLUDES the sender.
 *
 * The only thing not exercised is pi's in-process sendUserMessage injection into
 * a live model turn — that is source-proven (index.ts binds pi.sendUserMessage
 * 1:1; the mock test asserts the host call). Run by run-live-check.sh.
 */
const assert = require('node:assert/strict');
const { WebSocket } = require('ws');
const { AttnMeshClient } = require('../dist-test/src/core.js');

const PORT = process.env.LIVE_PORT || '29742';
const HTTP = `http://127.0.0.1:${PORT}`;
const WS = `ws://127.0.0.1:${PORT}`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(cond, timeoutMs = 6000, intervalMs = 25) {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor: timeout');
    await sleep(intervalMs);
  }
}

(async () => {
  const ok = (name) => console.log('  ✓', name);

  const calls = [];
  const host = { sendUserMessage: (text, opts) => calls.push({ text, opts }) };

  const client = new AttnMeshClient({
    session: 'pi-test',
    harness: 'pi',
    httpBase: HTTP,
    wsBase: WS,
    host,
    wsFactory: (url) => new WebSocket(url),
    fetchFn: fetch,
    reconnectMs: 1000,
  });
  client.start();
  await waitFor(() => client.isConnected(), 10000);
  ok('adapter (AttnMeshClient) connected to real attnd over WS');

  // Second session: a raw WS standing in for another local agent.
  const driverFrames = [];
  const driver = new WebSocket(`${WS}/?session=driver&harness=cli`);
  await new Promise((res, rej) => {
    driver.on('open', res);
    driver.on('error', rej);
  });
  driver.on('message', (d) => {
    try {
      driverFrames.push(JSON.parse(d.toString()));
    } catch {
      /* ignore */
    }
  });
  ok('second "driver" session connected (raw WS)');

  // Both sessions visible in the live registry.
  await waitFor(async () => true);
  const lp = await (await fetch(`${HTTP}/local-peers`)).json();
  assert.ok(
    lp.sessions.includes('pi-test') && lp.sessions.includes('driver'),
    `expected both sessions, got ${JSON.stringify(lp.sessions)}`,
  );
  ok(`/local-peers lists both live sessions: ${JSON.stringify(lp.sessions)}`);

  // driver -> pi-test local send (relay bypassed); adapter must parse + frame it.
  driver.send(JSON.stringify({ type: 'local', to: 'pi-test', message: 'live-mesh-hello' }));
  await waitFor(() => calls.length > 0, 6000);
  const c0 = calls[0];
  assert.match(c0.text, /live-mesh-hello/);
  assert.match(c0.text, /💻 Local:/); // 💻 Local:
  assert.match(c0.text, /Message from driver/);
  assert.deepEqual(c0.opts, { deliverAs: 'steer' });
  assert.equal(client.lastInboundFrom, 'driver');
  ok('driver→pi-test routed; adapter parsed it (from=driver, local, deliverAs:steer)');

  // pi-test broadcast -> driver receives the real daemon frame; sender excluded.
  const before = calls.length;
  const r = await client.send('all', 'bcast-from-pi');
  assert.equal(r.via, 'broadcast');
  await waitFor(() => driverFrames.some((f) => f.type === 'message' && f.message === 'bcast-from-pi'), 6000);
  const bf = driverFrames.find((f) => f.message === 'bcast-from-pi');
  assert.equal(bf.from, 'pi-test');
  assert.equal(bf.local, true);
  assert.equal(bf.trust, 'local');
  ok(`send('all') broadcast reached driver as a real daemon frame: ${JSON.stringify(bf)}`);

  await sleep(400);
  assert.equal(calls.length, before, 'sender must be excluded from its own broadcast');
  ok('broadcast excludes the sender (pi-test received no self-copy)');

  client.stop();
  driver.close();
  console.log('\nLIVE CHECK: ALL PASS ✅');
  process.exit(0);
})().catch((e) => {
  console.error('\nLIVE CHECK FAILED:', e && e.stack ? e.stack : e);
  process.exit(1);
});
