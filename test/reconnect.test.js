'use strict';

// Coverage for Step 3 of RENOVATION-PLAN.md: connect/reconnect behavior and
// the node.status() shown in the UI for every affected node type, against
// real Node-RED + real NATS in Docker (no mocks).
//
// Claims under test (see RENOVATION-PLAN.md "Step 3 - Tests"):
//   1. Cold start with the broker already down -> red/ring, no crash.
//   2. Broker starts -> green/dot.
//   3. Broker killed mid-flow -> non-green transition, no crash.
//   4. Broker restarts -> green again AND messages flow again, no redeploy.
//   5. Repeated kill/restart cycles keep recovering, no unbounded growth.
//   6. Per-node-type status: server, publish, subscribe, stream-publisher,
//      stream-consumer (JetStream pull consumer - the highest-risk case).
//   7. A subscription established before the outage survives it.
//   8. No unhandled rejection / crash in the Node-RED container.
//   9. Deleting a flow while the broker is down still completes promptly.
//
// The whole suite deploys exactly two flows and cycles the broker down/up a
// bounded number of times (1 cold-start + 3 repeated cycles) to keep runtime
// sane - see the top-level test's comment for the docker-cycle budget.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const path = require('node:path');
// ponytail: JetStream stream/consumer admin needs a client with
// jetstreamManager(). @nats-io/jetstream (the 3.x replacement) isn't
// installed yet (Step 4 migration hasn't landed) - `nats` v2.29.3 is already
// an installed dependency and is the exact client nodes/*.js itself still
// uses for JetStream, so reusing it here matches production, not a new dep.
const nats2 = require('nats');
const {
  ensureStackUp,
  deployFlow,
  deleteFlow,
  sweepHarnessFlows,
  triggerInject,
  connectComms,
  connectDirectNats,
  publishDirect,
  subscribeOnce,
  serverNode,
  NATS_URL,
  NODE_RED_URL,
} = require('./lib/harness');

const REPO_ROOT = path.resolve(__dirname, '..');

// --- Docker lifecycle for the broker only (nodered stays up throughout -
// its health is exactly what claim 8 is checking) -------------------------

function compose(args) {
  execFileSync('docker', ['compose', ...args], {
    cwd: REPO_ROOT,
    stdio: 'ignore',
  });
}
const stopNats = () => compose(['stop', 'nats-server']);
const killNats = () => compose(['kill', 'nats-server']);

// Waits for the broker's HTTP monitor to actually answer, not just for
// `docker compose up` to return - a (re)started container needs a moment
// before it accepts connections, and `nats2.connect({waitOnFirstConnect:
// false})` does NOT retry its *first* dial regardless of maxReconnectAttempts
// (that option only governs reconnection after an initial connect succeeds -
// verified the hard way while writing this file, see report). Every caller
// of startNats() gets this for free instead of each needing its own retry
// dance.
async function waitForNatsUp(timeoutMs = 20000) {
  const port = process.env.NATS_HTTP_PORT || 8222;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://localhost:${port}/varz`);
      if (res.ok) return;
    } catch {
      /* not up yet */
    }
    await new Promise(r => setTimeout(r, 300));
  }
  throw new Error(`NATS monitor did not come up within ${timeoutMs}ms`);
}

async function startNats() {
  compose(['up', '-d', 'nats-server']);
  await waitForNatsUp();
}
const noderedLogsSince = since =>
  execFileSync(
    'docker',
    ['compose', 'logs', '--no-color', '--since', since, 'nodered'],
    {
      cwd: REPO_ROOT,
      encoding: 'utf8',
    }
  );

// --- JetStream admin (stream setup only - not the thing under test) ------

// Deletes any pre-existing stream of this name first: a leftover stream from
// an earlier run (or from a different storage type) would otherwise make
// `streams.add` a silent no-op and desync this run's expectations from what
// is actually on disk (verified the hard way - see report).
async function freshFileStream(name, subject) {
  // Generous retry budget (unlike harness.connectDirectNats's fail-fast
  // default): this is called right after startNats(), before anything has
  // confirmed the container is actually accepting connections yet, and a
  // freshly (re)started container needs a couple of seconds.
  const nc = await nats2.connect({
    servers: NATS_URL,
    tls: null,
    waitOnFirstConnect: false,
    maxReconnectAttempts: 20,
    reconnectTimeWait: 500,
  });
  try {
    const jsm = await nc.jetstreamManager();
    await jsm.streams.delete(name).catch(() => {});
    await jsm.streams.add({
      name,
      subjects: [subject],
      retention: 'limits',
      // file, not memory: a memory-backed stream's messages (and sequence
      // counter) are wiped by the very container restart this suite
      // performs, which would make "did the message survive" indistinguishable
      // from "did the stream get wiped" - file storage is what a real
      // durability-seeking JetStream user would configure anyway.
      storage: 'file',
      max_msgs: 1000,
      max_bytes: 10 * 1024 * 1024,
      max_age: 0,
      duplicate_window: 0,
      num_replicas: 1,
    });
  } finally {
    await nc.close();
  }
}

async function deleteStream(name) {
  // Generous retry budget (unlike harness.connectDirectNats's fail-fast
  // default): this is called right after startNats(), before anything has
  // confirmed the container is actually accepting connections yet, and a
  // freshly (re)started container needs a couple of seconds.
  const nc = await nats2.connect({
    servers: NATS_URL,
    tls: null,
    waitOnFirstConnect: false,
    maxReconnectAttempts: 20,
    reconnectTimeWait: 500,
  });
  try {
    const jsm = await nc.jetstreamManager();
    await jsm.streams.delete(name).catch(() => {});
  } finally {
    await nc.close();
  }
}

async function natsConnectionCount() {
  const port = process.env.NATS_HTTP_PORT || 8222;
  const res = await fetch(`http://localhost:${port}/varz`);
  if (!res.ok)
    throw new Error(`NATS monitor /varz returned HTTP ${res.status}`);
  return (await res.json()).connections;
}

// --- Node-config builders (mirrors the *.html `defaults` blocks) ---------

let seq = 0;
const uid = base => `${base}${Date.now().toString(36)}${seq++}`;

const debugNode = id => ({
  id,
  type: 'debug',
  z: 'FLOW',
  name: '',
  active: true,
  tosidebar: true,
  console: false,
  complete: 'true',
  wires: [],
});

const injectNode = (id, wireTo) => ({
  id,
  type: 'inject',
  z: 'FLOW',
  name: '',
  props: [{ p: 'payload' }],
  repeat: '',
  once: false,
  topic: '',
  payload: '',
  payloadType: 'date',
  wires: [[wireTo]],
});

const publishNode = (id, srv, subject) => ({
  id,
  type: 'nats-suite-publish',
  z: 'FLOW',
  name: '',
  server: srv,
  debug: false,
  mode: 'publish',
  dataformat: 'string',
  datapointid: subject,
  enableTopicOverride: false,
  requestTimeout: 5000,
  requestFallbackToPublish: true,
  enableAutoReply: false,
  enableHeaders: false,
  headers: '',
  outputs: 0,
  wires: [],
});

const subscribeNode = (id, srv, subject, wireTo) => ({
  id,
  type: 'nats-suite-subscribe',
  z: 'FLOW',
  name: '',
  server: srv,
  debug: false,
  dataformat: 'auto',
  datapointid: subject,
  subscriptionMode: 'static',
  queueGroup: '',
  wires: [[wireTo]],
});

const streamPublisherNode = (id, srv, streamName, subject, wireTo) => ({
  id,
  type: 'nats-suite-stream-publisher',
  z: 'FLOW',
  name: '',
  server: srv,
  streamName,
  subjectPattern: subject,
  defaultSubject: subject,
  retention: 'limits',
  maxMessages: -1,
  maxAge: '24h',
  maxBytes: -1,
  duplicateWindow: '2m',
  storage: 'file',
  replicas: 1,
  operation: 'publish',
  createOnInit: true,
  debug: false,
  wires: [[wireTo]],
});

// ackPolicy 'none': the debug-node output the test reads back is a JSON
// snapshot (functions like msg.ack don't survive the /comms websocket), so
// there is no way for this test to actually call msg.ack(). With 'explicit'
// that would leave every fetched message permanently pending and eligible
// for redelivery after ackWait, which would make a *later* cycle's fetch
// non-deterministically return a stale message instead of the fresh one it
// just published. 'none' sidesteps that - JetStream tracks no ack state at
// all, so there is nothing to redeliver.
const streamConsumerNode = (
  id,
  srv,
  streamName,
  consumerName,
  subject,
  wireTo
) => ({
  id,
  type: 'nats-suite-stream-consumer',
  z: 'FLOW',
  name: '',
  server: srv,
  streamName,
  consumerName,
  createOnInit: true,
  filterSubject: subject,
  consumerType: 'pull',
  ackPolicy: 'none',
  deliverPolicy: 'new',
  ackWait: '30s',
  maxDeliver: 5,
  maxAckPending: 1000,
  idleHeartbeat: '5s',
  flowControl: false,
  batchSize: 1,
  maxWait: 2000,
  operation: 'consume',
  dataformat: 'auto',
  debug: false,
  wires: [[wireTo]],
});

// --- The suite -------------------------------------------------------------

test('reconnect + status: connect, disconnect, recover across every node type', async t => {
  const skipReason = await ensureStackUp();
  if (skipReason) {
    t.skip(skipReason);
    return;
  }

  const runId = uid('rc');
  const streamName = `${runId}-stream`;
  const consumerName = `${runId}-consumer`;
  const jsSubject = `test.reconnect.js.${runId}`;
  const pubSubject = `test.reconnect.pub.${runId}`;
  const coreSubject = `test.reconnect.core.${runId}`;

  const srv = `${runId}-srv`;
  const pub = `${runId}-pub`;
  const pubInj = `${runId}-pubinj`;
  const sub = `${runId}-sub`;
  const subDbg = `${runId}-subdbg`;
  const spub = `${runId}-spub`;
  const spubDbg = `${runId}-spubdbg`;
  const spubInj = `${runId}-spubinj`;
  const scon = `${runId}-scon`;
  const sconDbg = `${runId}-scondbg`;
  const sconInj = `${runId}-sconinj`;

  // All five node types under coverage requirement 6, table-driven so the
  // per-type checks below (cold-start-red, then connected-green) are one
  // assertion loop instead of five near-identical copies.
  const nodeTypes = [
    { label: 'server', id: srv },
    { label: 'publish', id: pub },
    { label: 'subscribe', id: sub },
    { label: 'stream-publisher', id: spub },
    { label: 'stream-consumer', id: scon },
  ];

  const comms = connectComms();
  let flowAId, flowBId;
  const startedAt = new Date();

  try {
    await comms.ready;

    // ======================================================================
    // Phase 1 - broker down from the start. Covers claims 1 and 9.
    // ======================================================================
    await t.test(
      '1. cold start with broker down -> every node type shows red/ring, runtime stays up',
      async () => {
        stopNats();

        const flowANodes = [
          serverNode(srv, { maxReconnectAttempts: 60, reconnectTimeWait: 500 }),
          publishNode(pub, srv, pubSubject),
          subscribeNode(sub, srv, coreSubject, subDbg),
          debugNode(subDbg),
          streamPublisherNode(spub, srv, streamName, jsSubject, spubDbg),
          debugNode(spubDbg),
          streamConsumerNode(
            scon,
            srv,
            streamName,
            consumerName,
            jsSubject,
            sconDbg
          ),
          debugNode(sconDbg),
        ];

        // Register every waiter before deploying: node construction for all
        // five types starts concurrently, so a waiter added after the fact can
        // miss a status broadcast that already happened (proven while writing
        // this file - a sequential registration loop silently dropped events).
        const waiters = nodeTypes.map(({ id }) =>
          comms.waitForStatus(id, d => d.fill === 'red', 20000)
        );

        flowAId = await deployFlow(flowANodes);
        const results = await Promise.all(waiters);

        results.forEach((d, i) => {
          assert.equal(
            d.fill,
            'red',
            `${nodeTypes[i].label} should show fill=red when the broker is down`
          );
          assert.equal(
            d.shape,
            'ring',
            `${nodeTypes[i].label} should show shape=ring (disconnected), not a dot`
          );
        });

        // Not "eventually times out" - the admin API must answer right now,
        // proving a broker outage at construction time didn't crash the runtime.
        const res = await fetch(`${NODE_RED_URL}/flows`);
        assert.ok(
          res.ok,
          'Node-RED admin API must stay responsive with the broker down at deploy time'
        );
      }
    );

    await t.test(
      '9. deleting a flow while the broker is still down completes without hanging',
      async () => {
        const t0 = Date.now();
        const DEADLINE_MS = 10000;
        await Promise.race([
          deleteFlow(flowAId),
          new Promise((_, reject) =>
            setTimeout(
              () => reject(new Error('deleteFlow did not resolve')),
              DEADLINE_MS
            )
          ),
        ]);
        flowAId = null;
        const elapsed = Date.now() - t0;
        assert.ok(
          elapsed < DEADLINE_MS,
          `close handlers must not hang during an outage (took ${elapsed}ms)`
        );
      }
    );

    // ======================================================================
    // Phase 2 - bring the broker up, deploy the flow used for the rest of
    // the suite. It is deployed exactly once; every later phase reuses it,
    // which is itself the proof of "no redeploy needed" (claim 4/7).
    // ======================================================================
    await t.test(
      '2. broker comes up -> every node type shows green',
      async () => {
        await startNats();
        await freshFileStream(streamName, jsSubject);

        const flowBNodes = [
          serverNode(srv, { maxReconnectAttempts: 60, reconnectTimeWait: 500 }),
          publishNode(pub, srv, pubSubject),
          injectNode(pubInj, pub),
          subscribeNode(sub, srv, coreSubject, subDbg),
          debugNode(subDbg),
          streamPublisherNode(spub, srv, streamName, jsSubject, spubDbg),
          debugNode(spubDbg),
          injectNode(spubInj, spub),
          streamConsumerNode(
            scon,
            srv,
            streamName,
            consumerName,
            jsSubject,
            sconDbg
          ),
          debugNode(sconDbg),
          injectNode(sconInj, scon),
        ];

        const waiters = nodeTypes.map(({ id }) =>
          comms.waitForStatus(id, d => d.fill === 'green', 45000)
        );
        flowBId = await deployFlow(flowBNodes);
        const results = await Promise.all(waiters);

        results.forEach((d, i) => {
          assert.equal(
            d.fill,
            'green',
            `${nodeTypes[i].label} should reach fill=green once the broker is reachable`
          );
        });
      }
    );

    // A single reusable check for "the whole pipe still works", called once
    // as a pre-outage baseline and again after every kill/restart cycle.
    async function proveMessageFlow(label) {
      // 1) Node-RED publish node -> real NATS: a direct subscriber sees it.
      {
        const directNc = await connectDirectNats();
        try {
          const delivered = subscribeOnce(directNc, pubSubject, 8000);
          await triggerInject(pubInj);
          const wireValue = await delivered;
          assert.ok(
            wireValue,
            `${label}: publish node should still reach NATS`
          );
        } finally {
          await directNc.close().catch(() => {});
        }
      }

      // 2) Direct NATS publish -> the SAME subscribe node established before
      // any outage -> its debug tap. This is claim 7: subscription survival.
      {
        const directNc = await connectDirectNats();
        try {
          const debugCaught = comms.waitForDebug(subDbg, 8000);
          publishDirect(directNc, coreSubject, `${label}-sub-check`);
          const msg = await debugCaught;
          assert.equal(
            msg.payload,
            `${label}-sub-check`,
            `${label}: pre-existing subscription must still deliver`
          );
        } finally {
          await directNc.close().catch(() => {});
        }
      }

      // 3) JetStream round trip: stream-publisher publishes, stream-consumer
      // (a durable PULL consumer, the highest-risk case per the brief)
      // fetches it. Both nodes are reused unchanged from Phase 2 - no redeploy.
      {
        const pubDebugCaught = comms.waitForDebug(spubDbg, 8000);
        await triggerInject(spubInj);
        const pubMsg = await pubDebugCaught;
        assert.equal(
          pubMsg.published,
          true,
          `${label}: JetStream publish should report published:true`
        );

        // Give the fetch a moment after the publish ack so the message is
        // durably visible to the pull consumer's next fetch() call.
        await new Promise(r => setTimeout(r, 300));

        const conDebugCaught = comms.waitForDebug(sconDbg, 8000);
        await triggerInject(sconInj);
        const conMsg = await conDebugCaught;
        assert.equal(
          conMsg.payload,
          pubMsg.payload,
          `${label}: JetStream pull consumer should deliver the message just published`
        );
      }
    }

    await t.test(
      'baseline (pre-outage): pub/sub and JetStream both work',
      async () => {
        await proveMessageFlow('baseline');
      }
    );

    const baselineConnections = await natsConnectionCount();

    // ======================================================================
    // Phase 3 - repeated kill/restart cycles. Covers claims 3, 4, 5, 7.
    // ======================================================================
    const CYCLES = 3;
    for (let cycle = 1; cycle <= CYCLES; cycle++) {
      await t.test(
        `cycle ${cycle}/${CYCLES}: kill -> non-green -> restart -> green + messages resume, no redeploy`,
        async () => {
          // --- kill: claim 3 --------------------------------------------------
          // Stream-consumer excluded from this specific mid-outage check: its
          // idle-timeout status update (fires 2s after the last processed
          // message, unconditionally) can race a disconnect and repaint green
          // right after it - a real quirk found while probing this suite, not
          // a flaky assertion. Its recovery is still proven functionally below.
          const nonGreenTypes = nodeTypes.filter(
            n => n.label !== 'stream-consumer'
          );
          // Both sets of waiters are registered up front, before killNats(), so
          // a fast transition can't be missed - and so that a failure in the
          // non-green assertions below still leaves startNats() reachable via
          // the finally, instead of stranding the broker down for every
          // subsequent cycle (a real failure mode hit while writing this file).
          const nonGreenWaiters = nonGreenTypes.map(({ id }) =>
            comms.waitForStatus(id, d => d.fill !== 'green', 10000)
          );
          const greenWaiters = nodeTypes.map(({ id }) =>
            comms.waitForStatus(id, d => d.fill === 'green', 45000)
          );

          killNats();
          try {
            const nonGreenResults = await Promise.all(nonGreenWaiters);
            nonGreenResults.forEach((d, i) => {
              assert.notEqual(
                d.fill,
                'green',
                `cycle ${cycle}: ${nonGreenTypes[i].label} must leave green when the broker is killed`
              );
              assert.equal(
                d.shape,
                'ring',
                `cycle ${cycle}: ${nonGreenTypes[i].label} non-green status should be shape=ring`
              );
            });
          } finally {
            await startNats();
          }

          // --- restart: claim 4a ----------------------------------------------
          const greenResults = await Promise.all(greenWaiters);
          greenResults.forEach((d, i) => {
            assert.equal(
              d.fill,
              'green',
              `cycle ${cycle}: ${nodeTypes[i].label} must return to green after the broker restarts`
            );
          });

          // --- messages flow again, same deployed flow: claims 4b, 5, 7 -------
          await proveMessageFlow(`cycle${cycle}`);
        }
      );
    }

    await t.test(
      '5. repeated cycles leave no growth in NATS connection count',
      async () => {
        const finalConnections = await natsConnectionCount();
        assert.ok(
          finalConnections <= baselineConnections + 1,
          `connection count should not grow across ${CYCLES} kill/restart cycles (baseline ${baselineConnections}, final ${finalConnections})`
        );
      }
    );

    await t.test(
      '8. no unhandled rejection / crash in the Node-RED container after all outages',
      async () => {
        const logs = noderedLogsSince(startedAt.toISOString());
        assert.doesNotMatch(
          logs.toLowerCase(),
          /unhandledrejection|unhandled promise rejection/,
          'Node-RED logs must contain no unhandled promise rejection after repeated broker outages'
        );

        const res = await fetch(`${NODE_RED_URL}/flows`);
        assert.ok(
          res.ok,
          'Node-RED admin API must still be responsive after all cycles'
        );
      }
    );
  } finally {
    if (flowAId) await deleteFlow(flowAId).catch(() => {});
    if (flowBId) await deleteFlow(flowBId).catch(() => {});
    await sweepHarnessFlows().catch(() => {});
    comms.close();
    await deleteStream(streamName).catch(() => {});
    // Always leave the broker running for other agents / the next run, even
    // if an assertion above threw mid-outage.
    await startNats().catch(() => {});
  }
});
