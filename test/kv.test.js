'use strict';

// Node-layer tests for nats-suite-kv-get / nats-suite-kv-put against the real
// dockerized Node-RED + NATS stack, proving the @nats-io/kv (Kvm) migration
// actually works: put -> get round-trip, history, and watch.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Kvm } = require('@nats-io/kv');
const {
  NATS_CONTAINER_URL,
  ensureStackUp,
  deployFlow,
  deleteFlow,
  connectComms,
  connectDirectNats,
  triggerInject,
} = require('./lib/harness');

async function ignoreFailure(promise) {
  try {
    return await promise;
  } catch {
    return undefined;
  }
}

// Each test creates a real JetStream-backed KV bucket on the shared dev
// broker; leaving them undestroyed accumulates stream state across runs
// (unlike deleteFlow(), which only removes the Node-RED flow, not the
// bucket it created).
const destroyBucket = async bucket => {
  const nc = await connectDirectNats();
  try {
    let kv;
    try {
      kv = await new Kvm(nc).open(bucket);
    } catch {
      kv = null;
    }
    if (kv) await kv.destroy();
  } finally {
    await ignoreFailure(nc.close());
  }
};

const serverNode = id => ({
  id,
  type: 'nats-suite-server',
  server: NATS_CONTAINER_URL,
  authMethod: 'none',
  enableTLS: false,
  tlsRejectUnauthorized: true,
  reconnectTimeWait: 1000,
  timeout: 10000,
  pingInterval: 30000,
  maxPingOut: 3,
  debug: false,
});

test('kv put -> get round-trip, with history', async t => {
  const skipReason = await ensureStackUp();
  if (skipReason) {
    t.skip(skipReason);
    return;
  }

  const bucket = `test_kv_${Date.now().toString(36)}`;
  const key = 'widget';

  const nodes = [
    serverNode('kv-srv'),
    {
      id: 'kv-put',
      type: 'nats-suite-kv-put',
      z: 'FLOW',
      server: 'kv-srv',
      bucket,
      bucketConfig: '',
      history: 10,
      maxAge: 0,
      maxBytes: 0,
      maxValueSize: 0,
      compression: false,
      replicas: 1,
      storage: 'file',
      operation: 'put',
      keyFrom: 'config',
      key,
      valueFrom: 'config',
      value: JSON.stringify({ n: 1 }),
      stringifyJSON: false,
      debug: false,
      wires: [['kv-put-debug']],
    },
    {
      id: 'kv-put-debug',
      type: 'debug',
      z: 'FLOW',
      active: true,
      tosidebar: true,
      console: false,
      complete: 'true',
      wires: [],
    },
    {
      id: 'kv-get',
      type: 'nats-suite-kv-get',
      z: 'FLOW',
      server: 'kv-srv',
      bucket,
      bucketConfig: '',
      history: 10,
      maxAge: 0,
      maxBytes: 0,
      maxValueSize: 0,
      compression: false,
      replicas: 1,
      storage: 'file',
      mode: 'get',
      keyFrom: 'config',
      key,
      includeHistory: true,
      historyLimit: 5,
      ignoreDeletes: true,
      parseJSON: true,
      debug: false,
      wires: [['kv-debug']],
    },
    {
      id: 'kv-debug',
      type: 'debug',
      z: 'FLOW',
      active: true,
      tosidebar: true,
      console: false,
      complete: 'true',
      wires: [],
    },
    {
      id: 'kv-inject-put',
      type: 'inject',
      z: 'FLOW',
      props: [{ p: 'payload' }],
      repeat: '',
      once: false,
      payload: '',
      payloadType: 'str',
      wires: [['kv-put']],
    },
    {
      id: 'kv-inject-get',
      type: 'inject',
      z: 'FLOW',
      props: [{ p: 'payload' }],
      repeat: '',
      once: false,
      payload: '',
      payloadType: 'str',
      wires: [['kv-get']],
    },
  ];

  const comms = connectComms();
  let flowId;

  try {
    await comms.ready;

    // kv-put/kv-get don't paint a "connected" status of their own (unlike
    // publish/subscribe) - they only turn green after a successful
    // operation, so the only pre-condition to wait on here is the server.
    const srvConnected = comms.waitForStatus(
      'kv-srv',
      d => d.fill === 'green',
      20000
    );

    flowId = await deployFlow(nodes);
    await srvConnected;

    const putDone = comms.waitForDebug('kv-put-debug', 10000);
    await triggerInject('kv-inject-put');
    await putDone;

    const firstGet = comms.waitForDebug('kv-debug', 10000);
    await triggerInject('kv-inject-get');
    const firstMsg = await firstGet;
    assert.deepEqual(
      firstMsg.payload,
      { n: 1 },
      'kv-get should retrieve the JSON value kv-put stored'
    );
    assert.equal(firstMsg.key, key);
    assert.equal(firstMsg.bucket, bucket);
    assert.equal(typeof firstMsg.revision, 'number');
    assert.ok(
      Array.isArray(firstMsg._history) && firstMsg._history.length >= 1,
      'includeHistory should attach at least one entry'
    );
  } finally {
    if (flowId) await ignoreFailure(deleteFlow(flowId));
    comms.close();
    await destroyBucket(bucket);
  }
});

test('kv history accumulates revisions for repeated puts to the same key', async t => {
  const skipReason = await ensureStackUp();
  if (skipReason) {
    t.skip(skipReason);
    return;
  }

  const bucket = `test_kv_hist_${Date.now().toString(36)}`;
  const key = 'counter';

  const nodes = [
    serverNode('kvh-srv'),
    {
      id: 'kvh-put',
      type: 'nats-suite-kv-put',
      z: 'FLOW',
      server: 'kvh-srv',
      bucket,
      bucketConfig: '',
      history: 10,
      maxAge: 0,
      maxBytes: 0,
      maxValueSize: 0,
      compression: false,
      replicas: 1,
      storage: 'file',
      operation: 'put',
      keyFrom: 'config',
      key,
      valueFrom: 'config',
      value: 'v1',
      stringifyJSON: false,
      debug: false,
      wires: [['kvh-put-debug']],
    },
    {
      id: 'kvh-put-debug',
      type: 'debug',
      z: 'FLOW',
      active: true,
      tosidebar: true,
      console: false,
      complete: 'true',
      wires: [],
    },
    {
      id: 'kvh-get',
      type: 'nats-suite-kv-get',
      z: 'FLOW',
      server: 'kvh-srv',
      bucket,
      bucketConfig: '',
      history: 10,
      maxAge: 0,
      maxBytes: 0,
      maxValueSize: 0,
      compression: false,
      replicas: 1,
      storage: 'file',
      mode: 'get',
      keyFrom: 'config',
      key,
      includeHistory: true,
      historyLimit: 10,
      ignoreDeletes: true,
      parseJSON: false,
      debug: false,
      wires: [['kvh-debug']],
    },
    {
      id: 'kvh-debug',
      type: 'debug',
      z: 'FLOW',
      active: true,
      tosidebar: true,
      console: false,
      complete: 'true',
      wires: [],
    },
    {
      id: 'kvh-inject-put',
      type: 'inject',
      z: 'FLOW',
      props: [{ p: 'payload' }],
      repeat: '',
      once: false,
      payload: '',
      payloadType: 'str',
      wires: [['kvh-put']],
    },
    {
      id: 'kvh-inject-get',
      type: 'inject',
      z: 'FLOW',
      props: [{ p: 'payload' }],
      repeat: '',
      once: false,
      payload: '',
      payloadType: 'str',
      wires: [['kvh-get']],
    },
  ];

  const comms = connectComms();
  let flowId;

  try {
    await comms.ready;

    const connected = d => d.fill === 'green';
    const srvConnected = comms.waitForStatus('kvh-srv', connected, 20000);
    flowId = await deployFlow(nodes);
    await srvConnected;

    // Put the same key 3 times (value fixed by config = 'v1'); the server
    // still bumps the revision each time, which is all history needs to
    // grow. Wait for each put's own debug output before firing the next,
    // rather than a fixed sleep, so ordering is driven by an observable
    // completion signal.
    for (let i = 0; i < 3; i++) {
      const putDone = comms.waitForDebug('kvh-put-debug', 10000);
      await triggerInject('kvh-inject-put');
      await putDone;
    }

    const got = comms.waitForDebug('kvh-debug', 10000);
    await triggerInject('kvh-inject-get');
    const msg = await got;

    assert.ok(Array.isArray(msg._history), 'history array should be present');
    assert.ok(
      msg._history.length >= 3,
      `expected at least 3 history entries, got ${msg._history.length}`
    );
    assert.equal(msg._history[0].operation, 'PUT');
  } finally {
    if (flowId) await ignoreFailure(deleteFlow(flowId));
    comms.close();
    await destroyBucket(bucket);
  }
});

test('kv watch mode emits an event when another node puts to the bucket', async t => {
  const skipReason = await ensureStackUp();
  if (skipReason) {
    t.skip(skipReason);
    return;
  }

  const bucket = `test_kv_watch_${Date.now().toString(36)}`;
  const key = 'signal';

  const nodes = [
    serverNode('kvw-srv'),
    {
      id: 'kvw-watch',
      type: 'nats-suite-kv-get',
      z: 'FLOW',
      server: 'kvw-srv',
      bucket,
      bucketConfig: '',
      history: 10,
      maxAge: 0,
      maxBytes: 0,
      maxValueSize: 0,
      compression: false,
      replicas: 1,
      storage: 'file',
      mode: 'watch',
      keyFrom: 'config',
      key: '',
      watchPattern: key,
      includeHistory: false,
      historyLimit: 1,
      ignoreDeletes: true,
      parseJSON: false,
      debug: false,
      wires: [['kvw-debug']],
    },
    {
      id: 'kvw-debug',
      type: 'debug',
      z: 'FLOW',
      active: true,
      tosidebar: true,
      console: false,
      complete: 'true',
      wires: [],
    },
    {
      id: 'kvw-put',
      type: 'nats-suite-kv-put',
      z: 'FLOW',
      server: 'kvw-srv',
      bucket,
      bucketConfig: '',
      history: 10,
      maxAge: 0,
      maxBytes: 0,
      maxValueSize: 0,
      compression: false,
      replicas: 1,
      storage: 'file',
      operation: 'put',
      keyFrom: 'config',
      key,
      valueFrom: 'config',
      value: 'triggered',
      stringifyJSON: false,
      debug: false,
      wires: [[]],
    },
    {
      id: 'kvw-inject',
      type: 'inject',
      z: 'FLOW',
      props: [{ p: 'payload' }],
      repeat: '',
      once: false,
      payload: '',
      payloadType: 'str',
      wires: [['kvw-put']],
    },
  ];

  const comms = connectComms();
  let flowId;

  try {
    await comms.ready;

    const connected = d => d.fill === 'green';
    const srvConnected = comms.waitForStatus('kvw-srv', connected, 20000);
    const watching = comms.waitForStatus(
      'kvw-watch',
      d => d.text === 'watching',
      20000
    );

    flowId = await deployFlow(nodes);
    await srvConnected;
    await watching;

    const watchEvent = comms.waitForDebug('kvw-debug', 10000);
    await triggerInject('kvw-inject');
    const msg = await watchEvent;

    assert.equal(msg.key, key);
    assert.equal(msg.payload, 'triggered');
    assert.equal(msg.operation, 'PUT');
    assert.equal(msg._watchEvent, true);
  } finally {
    if (flowId) await ignoreFailure(deleteFlow(flowId));
    comms.close();
    await destroyBucket(bucket);
  }
});
