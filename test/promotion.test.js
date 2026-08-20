'use strict';

// Step 5: object-get/object-put/service were promoted from nodes-dev/ (unregistered,
// unshippable) to real registered nodes in nodes/. Proves the promotion actually
// works against the real Node-RED container: the three new types construct
// (an unregistered type never emits any status - Node-RED silently skips
// building it), the shared config node's connection-user register/unregister
// stays balanced across a redeploy (no orphaned NATS connection left behind),
// and the two examples that reference these types (04, 05) import and connect
// instead of failing with "unknown type" as they did before this step.

const fs = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  NATS_CONTAINER_URL,
  ensureStackUp,
  deployFlow,
  deleteFlow,
  connectComms,
  serverNode,
} = require('./lib/harness');

async function natsConnectionCount() {
  const port = process.env.NATS_HTTP_PORT || 8222;
  const res = await fetch(`http://localhost:${port}/varz`);
  if (!res.ok) throw new Error(`NATS monitor /varz returned HTTP ${res.status}`);
  return (await res.json()).connections;
}

// Bounded poll: the config node's own close handler tears the connection down
// synchronously on redeploy (no 30s idle-cleanup wait - that timer only
// applies while the config node itself survives with zero consumers), but
// the HTTP roundtrip and socket close still take a moment.
async function waitForConnectionCount(predicate, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    last = await natsConnectionCount();
    if (predicate(last)) return last;
    await new Promise(r => setTimeout(r, 200));
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for NATS connection count; last seen ${last}`);
}

let seq = 0;
const uid = base => `${base}${Date.now().toString(36)}${seq++}`;

test('promoted types: object-get, object-put, service construct and register/unregister balances across redeploy', async t => {
  const skipReason = await ensureStackUp();
  if (skipReason) {
    t.skip(skipReason);
    return;
  }

  const srv = uid('srv');
  const og = uid('og');
  const op = uid('op');
  const svc = uid('svc');

  const nodes = [
    serverNode(srv),
    {
      id: og,
      type: 'nats-suite-object-get',
      z: 'FLOW',
      name: '',
      server: srv,
      bucket: 'promotion-test-bucket',
      objectName: '',
      nameFrom: 'msg',
      outputFormat: 'buffer',
      operation: 'get',
      debug: false,
      wires: [[]],
    },
    {
      id: op,
      type: 'nats-suite-object-put',
      z: 'FLOW',
      name: '',
      server: srv,
      bucket: 'promotion-test-bucket',
      objectName: '',
      nameFrom: 'msg',
      dataFrom: 'payload',
      filePath: '',
      operation: 'put',
      debug: false,
      wires: [[]],
    },
    {
      id: svc,
      type: 'nats-suite-service',
      z: 'FLOW',
      name: '',
      server: srv,
      mode: 'discover',
      discoveryFilter: '*',
      debug: false,
      wires: [[]],
    },
  ];

  const comms = connectComms();
  let flowId;

  try {
    await comms.ready;

    const baseline = await natsConnectionCount();

    // Any status broadcast at all is proof the node constructed - an
    // unregistered type is silently skipped by the runtime and never emits
    // one. The status/# replay on subscribe means these resolve immediately
    // once deployFlow's response lands, not just on a future change.
    const anyStatus = () => true;
    const ogStatus = comms.waitForStatus(og, anyStatus, 15000);
    const opStatus = comms.waitForStatus(op, anyStatus, 15000);
    const svcStatus = comms.waitForStatus(svc, anyStatus, 15000);
    const srvConnected = comms.waitForStatus(srv, d => d.fill === 'green', 15000);

    flowId = await deployFlow(nodes);

    const [ogResult, opResult, svcResult] = await Promise.all([ogStatus, opStatus, svcStatus, srvConnected]);
    assert.ok(ogResult, 'nats-suite-object-get must construct and emit a status (proves the type resolved)');
    assert.ok(opResult, 'nats-suite-object-put must construct and emit a status (proves the type resolved)');
    assert.ok(svcResult, 'nats-suite-service must construct and emit a status (proves the type resolved)');

    // One shared connection for the three consumers + the config node's own
    // acquisition - registerConnectionUser is called by all three on
    // construction, unregisterConnectionUser by all three on close.
    const withFlow = await waitForConnectionCount(n => n >= baseline + 1, 10000);
    assert.ok(withFlow >= baseline + 1, `expected a new connection, baseline=${baseline} withFlow=${withFlow}`);

    await deleteFlow(flowId);
    flowId = null;

    // If any of the three had leaked their registerConnectionUser call
    // (still using the removed removeConnectionUser, or any other imbalance),
    // the config node's own close handler would either throw before reaching
    // connection teardown or never see connectionRefCount hit zero, and this
    // connection would still be open.
    await waitForConnectionCount(n => n <= baseline, 10000);

    // Redeploy the identical flow to prove this isn't a one-shot fluke -
    // the same balance holds on a second cycle, not just the first.
    const ogStatus2 = comms.waitForStatus(og, anyStatus, 15000);
    flowId = await deployFlow(nodes);
    await ogStatus2;
    await deleteFlow(flowId);
    flowId = null;
    await waitForConnectionCount(n => n <= baseline, 10000);
  } finally {
    if (flowId) await deleteFlow(flowId).catch(() => {});
    comms.close();
  }
});

async function importExample(t, filename, extraNodeIds) {
  const skipReason = await ensureStackUp();
  if (skipReason) {
    t.skip(skipReason);
    return;
  }

  const all = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'examples', filename), 'utf8'));
  const nodes = all
    .filter(n => n.type !== 'tab')
    .map(n => (n.type === 'nats-suite-server' ? { ...n, server: NATS_CONTAINER_URL } : n));
  const serverNodeId = all.find(n => n.type === 'nats-suite-server').id;

  const comms = connectComms();
  let flowId;
  try {
    await comms.ready;
    const anyStatus = () => true;
    const waits = extraNodeIds.map(id => comms.waitForStatus(id, anyStatus, 15000));
    const srvConnected = comms.waitForStatus(serverNodeId, d => d.fill === 'green', 15000);

    flowId = await deployFlow(nodes);

    // Every real nats-suite node in the example must construct (no "unknown
    // type") and the shared server must actually connect to the real broker.
    await Promise.all([...waits, srvConnected]);
  } finally {
    if (flowId) await deleteFlow(flowId).catch(() => {});
    comms.close();
  }
}

test('example 04-object-store.json imports and connects (object-get/object-put no longer unknown types)', async t => {
  await importExample(t, '04-object-store.json', ['object-put', 'object-delete', 'object-get', 'object-list']);
});

test('example 05-service.json imports and connects (service no longer an unknown type)', async t => {
  await importExample(
    t,
    '05-service.json',
    ['service-echo', 'service-discover', 'service-stats', 'service-ping', 'service-health', 'service-nats-stats']
  );
});
