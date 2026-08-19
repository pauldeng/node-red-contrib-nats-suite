'use strict';

// Tests for nats-suite-publish.js against its post-rewrite contract (13
// config properties: name, server, debug, mode, dataformat, datapointid,
// enableTopicOverride, requestTimeout, requestFallbackToPublish,
// enableAutoReply, enableHeaders, headers). Deliberately does NOT cover the
// deleted disk buffer / batching / rate limiting / message expiration.
//
// Everything runs against the real Node-RED + real NATS docker-compose
// stack via test/lib/harness.js - no mocks, no node-red-node-test-helper.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  ensureStackUp,
  deployFlow,
  deleteFlow,
  triggerInject,
  connectComms,
  connectDirectNats,
  subscribeOnce,
  subscribeOnceMsg,
  serverNode,
} = require('./lib/harness');

// --- Small node-config builders (local to this file; each test assembles a
// short flow from these instead of repeating full node objects) -----------

function publishNode(id, overrides) {
  return {
    id,
    type: 'nats-suite-publish',
    z: 'FLOW',
    name: '',
    server: '',
    debug: false,
    mode: 'publish',
    dataformat: 'string',
    datapointid: '',
    enableTopicOverride: false,
    requestTimeout: 5000,
    requestFallbackToPublish: true,
    enableAutoReply: false,
    enableHeaders: false,
    headers: '',
    outputs: 0,
    wires: [],
    ...overrides,
  };
}

function injectNode(id, wireTo, props) {
  return {
    id,
    type: 'inject',
    z: 'FLOW',
    name: '',
    props,
    repeat: '',
    once: false,
    topic: '',
    payload: '',
    payloadType: 'date',
    wires: [[wireTo]],
  };
}

function debugNode(id) {
  return {
    id,
    type: 'debug',
    z: 'FLOW',
    name: '',
    active: true,
    tosidebar: true,
    console: false,
    complete: 'true',
    wires: [],
  };
}

function catchNode(id, scope, wireTo) {
  return { id, type: 'catch', z: 'FLOW', name: '', scope, uncaught: false, wires: [[wireTo]] };
}

function completeNode(id, scope, wireTo) {
  return { id, type: 'complete', z: 'FLOW', name: '', scope, wires: [[wireTo]] };
}

// --- Shared setup / teardown ----------------------------------------------

// Skips (does not fail) when Docker itself is unavailable; any other startup
// problem is a real failure, per harness.ensureStackUp's own contract.
async function checkStack(t) {
  const skipReason = await ensureStackUp();
  if (skipReason) {
    t.skip(skipReason);
    return false;
  }
  return true;
}

let seq = 0;
const uid = (base) => `${base}${Date.now().toString(36)}${seq++}`;

async function natsMonitorConnections() {
  const port = process.env.NATS_HTTP_PORT || 8222;
  const res = await fetch(`http://localhost:${port}/varz`);
  if (!res.ok) throw new Error(`NATS monitor /varz returned HTTP ${res.status}`);
  return (await res.json()).connections;
}

// --- 1. Payload encoding per dataformat -----------------------------------
// Table-driven: json/string/buffer differ only in the inject typedInput used
// to build msg.payload and how the raw wire bytes are checked back out.

const dataformatCases = [
  {
    dataformat: 'json',
    injectProp: { p: 'payload', v: JSON.stringify({ a: 1, note: 'hello nats' }), vt: 'json' },
    checkWire: (raw) => assert.deepEqual(JSON.parse(Buffer.from(raw).toString('utf8')), { a: 1, note: 'hello nats' }),
  },
  {
    dataformat: 'string',
    injectProp: { p: 'payload', v: 'plain string payload €', vt: 'str' },
    checkWire: (raw) => assert.equal(Buffer.from(raw).toString('utf8'), 'plain string payload €'),
  },
  {
    dataformat: 'buffer',
    injectProp: { p: 'payload', v: '[0,1,2,255,254,253]', vt: 'bin' },
    checkWire: (raw) => assert.equal(Buffer.compare(Buffer.from(raw), Buffer.from([0, 1, 2, 255, 254, 253])), 0),
  },
];

for (const c of dataformatCases) {
  test(`publish: dataformat "${c.dataformat}" reaches NATS with the exact bytes`, async (t) => {
    if (!(await checkStack(t))) return;

    const id = uid(`df-${c.dataformat}-`);
    const subject = `test.publish.dataformat.${id}`;
    const srv = `${id}srv`;
    const pub = `${id}pub`;
    const inj = `${id}inj`;

    const nodes = [
      serverNode(srv),
      publishNode(pub, { server: srv, dataformat: c.dataformat, datapointid: subject }),
      injectNode(inj, pub, [c.injectProp]),
    ];

    const comms = connectComms();
    const directNc = await connectDirectNats();
    let flowId;
    try {
      await comms.ready;
      const connected = comms.waitForStatus(pub, (d) => d.fill === 'green', 15000);
      flowId = await deployFlow(nodes);
      await connected;

      const received = subscribeOnceMsg(directNc, subject, 8000);
      await triggerInject(inj);
      const msg = await received;

      c.checkWire(msg.data);
    } finally {
      if (flowId) await deleteFlow(flowId).catch(() => {});
      comms.close();
      await directNc.close().catch(() => {});
    }
  });
}

// --- 2. Subject resolution: datapointid vs msg.topic ----------------------

const subjectCases = [
  {
    name: 'enableTopicOverride off: datapointid wins even when msg.topic is set',
    enableTopicOverride: false,
    expected: 'configured',
  },
  {
    name: 'enableTopicOverride on: msg.topic wins over datapointid',
    enableTopicOverride: true,
    expected: 'topic',
  },
];

for (const c of subjectCases) {
  test(`publish: subject resolution - ${c.name}`, async (t) => {
    if (!(await checkStack(t))) return;

    const id = uid(`subj-${c.expected}-`);
    const configuredSubject = `test.publish.subject.configured.${id}`;
    const topicSubject = `test.publish.subject.topic.${id}`;
    const probe = `probe-${id}`;
    const srv = `${id}srv`;
    const pub = `${id}pub`;
    const inj = `${id}inj`;

    const nodes = [
      serverNode(srv),
      publishNode(pub, {
        server: srv,
        dataformat: 'string',
        datapointid: configuredSubject,
        enableTopicOverride: c.enableTopicOverride,
      }),
      injectNode(inj, pub, [
        { p: 'payload', v: probe, vt: 'str' },
        { p: 'topic', v: topicSubject, vt: 'str' },
      ]),
    ];

    const comms = connectComms();
    const directNc = await connectDirectNats();
    let flowId;
    try {
      await comms.ready;
      const connected = comms.waitForStatus(pub, (d) => d.fill === 'green', 15000);
      flowId = await deployFlow(nodes);
      await connected;

      const expectedSubject = c.expected === 'configured' ? configuredSubject : topicSubject;
      const otherSubject = c.expected === 'configured' ? topicSubject : configuredSubject;

      const arrived = subscribeOnce(directNc, expectedSubject, 8000);
      await triggerInject(inj);
      assert.equal(await arrived, probe, `message should have been published on ${expectedSubject}`);

      // A message can never appear on a subject nothing published to under
      // NATS's exact-subject matching, so this rejection is a deterministic
      // proof of "not this subject", not a merely bounded observation.
      await assert.rejects(
        subscribeOnce(directNc, otherSubject, 500),
        /Timed out/,
        `message should NOT have been published on ${otherSubject}`
      );
    } finally {
      if (flowId) await deleteFlow(flowId).catch(() => {});
      comms.close();
      await directNc.close().catch(() => {});
    }
  });
}

// --- 3. Request/reply -------------------------------------------------------

test('publish: mode "request" emits the responder\'s reply on its output', async (t) => {
  if (!(await checkStack(t))) return;

  const id = uid('req-ok-');
  const subject = `test.publish.request.${id}`;
  const probe = `probe-${id}`;
  const srv = `${id}srv`;
  const pub = `${id}pub`;
  const inj = `${id}inj`;
  const dbg = `${id}dbg`;

  const nodes = [
    serverNode(srv),
    publishNode(pub, {
      server: srv,
      mode: 'request',
      datapointid: subject,
      requestTimeout: 5000,
      outputs: 1,
      wires: [[dbg]],
    }),
    injectNode(inj, pub, [{ p: 'payload', v: probe, vt: 'str' }]),
    debugNode(dbg),
  ];

  const comms = connectComms();
  const directNc = await connectDirectNats();
  let flowId;
  try {
    await comms.ready;
    const connected = comms.waitForStatus(pub, (d) => d.fill === 'green', 15000);
    flowId = await deployFlow(nodes);
    await connected;

    // Test process acts as the responder.
    const sub = directNc.subscribe(subject);
    (async () => {
      for await (const m of sub) {
        const requestText = new TextDecoder().decode(m.data);
        m.respond(new TextEncoder().encode(JSON.stringify({ ok: true, echoed: requestText })));
        break;
      }
    })().catch(() => {});

    const debugCaught = comms.waitForDebug(dbg, 8000);
    await triggerInject(inj);
    const debugMsg = await debugCaught;

    assert.deepEqual(debugMsg.payload, { ok: true, echoed: probe });
  } finally {
    if (flowId) await deleteFlow(flowId).catch(() => {});
    comms.close();
    await directNc.close().catch(() => {});
  }
});

test('publish: requestTimeout with requestFallbackToPublish=false surfaces an error, no fallback publish happens', async (t) => {
  if (!(await checkStack(t))) return;

  const id = uid('req-timeout-nofb-');
  const subject = `test.publish.request.timeout.nofb.${id}`;
  const probe = `probe-${id}`;
  const srv = `${id}srv`;
  const pub = `${id}pub`;
  const inj = `${id}inj`;
  const dbg = `${id}dbg`;

  const nodes = [
    serverNode(srv),
    publishNode(pub, {
      server: srv,
      mode: 'request',
      datapointid: subject,
      requestTimeout: 500,
      requestFallbackToPublish: false,
      outputs: 1,
      wires: [[dbg]],
    }),
    injectNode(inj, pub, [{ p: 'payload', v: probe, vt: 'str' }]),
    debugNode(dbg),
  ];

  // A subscriber that never replies is required to exercise the *timeout* path.
  // With zero subscribers NATS answers immediately with 503 no-responders and
  // never waits for requestTimeout, which is a different code path (and one
  // where falling back to publish would discard the message anyway, since 503
  // means nothing at all is listening on the subject).
  const comms = connectComms();
  const directNc = await connectDirectNats();
  let flowId;
  try {
    await comms.ready;
    const connected = comms.waitForStatus(pub, (d) => d.fill === 'green', 15000);
    flowId = await deployFlow(nodes);
    await connected;

    const silentSub = subscribeOnce(directNc, subject, 5000);
    const debugCaught = comms.waitForDebug(dbg, 5000);
    const errorStatus = comms.waitForStatus(pub, (d) => d.fill !== 'green', 5000);
    await triggerInject(inj);
    await silentSub.catch(() => {});

    const [debugMsg, status] = await Promise.all([debugCaught, errorStatus]);

    assert.equal(debugMsg.error.code, 'TIMEOUT', 'msg.error should record a timeout, not a fallback');
    assert.equal(debugMsg.fallback, undefined, 'no fallback publish should have happened');
    assert.notEqual(status.fill, 'green', 'node status should show a non-connected/error state on timeout');
  } finally {
    if (flowId) await deleteFlow(flowId).catch(() => {});
    comms.close();
    await directNc.close().catch(() => {});
  }
});

test('publish: requestTimeout with requestFallbackToPublish=true publishes instead of erroring', async (t) => {
  if (!(await checkStack(t))) return;

  const id = uid('req-timeout-fb-');
  const subject = `test.publish.request.timeout.fb.${id}`;
  const probe = `probe-${id}`;
  const srv = `${id}srv`;
  const pub = `${id}pub`;
  const inj = `${id}inj`;
  const dbg = `${id}dbg`;

  const nodes = [
    serverNode(srv),
    publishNode(pub, {
      server: srv,
      mode: 'request',
      datapointid: subject,
      requestTimeout: 500,
      requestFallbackToPublish: true,
      outputs: 1,
      wires: [[dbg]],
    }),
    injectNode(inj, pub, [{ p: 'payload', v: probe, vt: 'str' }]),
    debugNode(dbg),
  ];

  const comms = connectComms();
  const directNc = await connectDirectNats();
  let flowId;
  try {
    await comms.ready;
    const connected = comms.waitForStatus(pub, (d) => d.fill === 'green', 15000);
    flowId = await deployFlow(nodes);
    await connected;

    // Nobody responds; after requestTimeout the node should instead publish
    // the same payload to `subject` as a plain message.
    const fallbackDelivered = subscribeOnce(directNc, subject, 5000);
    const debugCaught = comms.waitForDebug(dbg, 5000);
    const backToGreen = comms.waitForStatus(pub, (d) => d.fill === 'green', 5000);

    await triggerInject(inj);

    const [wirePayload, debugMsg] = await Promise.all([fallbackDelivered, debugCaught, backToGreen]);

    assert.equal(wirePayload, probe, 'fallback publish should carry the original request payload');
    assert.equal(debugMsg.fallback, 'publish');
    assert.equal(debugMsg.error, undefined, 'fallback success should not leave msg.error set');
  } finally {
    if (flowId) await deleteFlow(flowId).catch(() => {});
    comms.close();
    await directNc.close().catch(() => {});
  }
});

// --- 4. Headers --------------------------------------------------------------

test('publish: enableHeaders sends configured + dynamic msg.headers as real NATS headers', async (t) => {
  if (!(await checkStack(t))) return;

  const id = uid('hdr-');
  const subject = `test.publish.headers.${id}`;
  const srv = `${id}srv`;
  const pub = `${id}pub`;
  const inj = `${id}inj`;

  const nodes = [
    serverNode(srv),
    publishNode(pub, {
      server: srv,
      dataformat: 'string',
      datapointid: subject,
      enableHeaders: true,
      headers: JSON.stringify({ 'X-Static': 'config-value' }),
    }),
    injectNode(inj, pub, [
      { p: 'payload', v: 'header-test-payload', vt: 'str' },
      { p: 'headers', v: JSON.stringify({ 'X-Dynamic': 'msg-value' }), vt: 'json' },
    ]),
  ];

  const comms = connectComms();
  const directNc = await connectDirectNats();
  let flowId;
  try {
    await comms.ready;
    const connected = comms.waitForStatus(pub, (d) => d.fill === 'green', 15000);
    flowId = await deployFlow(nodes);
    await connected;

    const received = subscribeOnceMsg(directNc, subject, 8000);
    await triggerInject(inj);
    const msg = await received;

    assert.ok(msg.headers, 'message should carry real NATS headers');
    assert.equal(msg.headers.get('X-Static'), 'config-value');
    assert.equal(msg.headers.get('X-Dynamic'), 'msg-value');
    assert.equal(Buffer.from(msg.data).toString('utf8'), 'header-test-payload');
  } finally {
    if (flowId) await deleteFlow(flowId).catch(() => {});
    comms.close();
    await directNc.close().catch(() => {});
  }
});

// --- 5. Node-RED contracts ---------------------------------------------------

test('publish: a wired Catch node receives node.error(err, msg) with the original msg intact on failure', async (t) => {
  if (!(await checkStack(t))) return;

  const id = uid('catch-');
  const probe = `probe-${id}`;
  const srv = `${id}srv`;
  const pub = `${id}pub`;
  const inj = `${id}inj`;
  const cat = `${id}cat`;
  const dbg = `${id}dbg`;

  const nodes = [
    serverNode(srv),
    // No subject configured and no override -> deterministic, config-driven
    // failure on every input message (not a connectivity/timeout race).
    publishNode(pub, { server: srv, dataformat: 'string', datapointid: '', enableTopicOverride: false }),
    injectNode(inj, pub, [{ p: 'payload', v: probe, vt: 'str' }]),
    catchNode(cat, [pub], dbg),
    debugNode(dbg),
  ];

  const comms = connectComms();
  let flowId;
  try {
    await comms.ready;
    const connected = comms.waitForStatus(pub, (d) => d.fill === 'green', 15000);
    flowId = await deployFlow(nodes);
    await connected;

    const caught = comms.waitForDebug(dbg, 5000);
    await triggerInject(inj);
    const caughtMsg = await caught;

    // Node.prototype.error() only dispatches to Catch nodes when called with
    // a real msg object (see @node-red/runtime Node.js `error(logMessage,msg)`
    // -> `if (msg && typeof msg === 'object') this._flow.handleError(...)`).
    // A 1-arg node.error(err) would just log and never reach this Catch node
    // at all, so the Catch node firing IS the proof of the 2-arg form.
    assert.ok(caughtMsg.error, 'caught message should carry the error');
    assert.equal(caughtMsg.error.source.id, pub, 'error should be attributed to the publish node');
    assert.equal(caughtMsg.payload, probe, 'the ORIGINAL message payload must survive into the Catch node');
  } finally {
    if (flowId) await deleteFlow(flowId).catch(() => {});
    comms.close();
  }
});

test('publish: a wired Complete node fires (done()) only after a real publish succeeds', async (t) => {
  if (!(await checkStack(t))) return;

  const id = uid('complete-');
  const subject = `test.publish.complete.${id}`;
  const probe = `probe-${id}`;
  const srv = `${id}srv`;
  const pub = `${id}pub`;
  const inj = `${id}inj`;
  const cmp = `${id}cmp`;
  const dbg = `${id}dbg`;

  const nodes = [
    serverNode(srv),
    publishNode(pub, { server: srv, dataformat: 'string', datapointid: subject }),
    injectNode(inj, pub, [{ p: 'payload', v: probe, vt: 'str' }]),
    completeNode(cmp, [pub], dbg),
    debugNode(dbg),
  ];

  const comms = connectComms();
  const directNc = await connectDirectNats();
  let flowId;
  try {
    await comms.ready;
    const connected = comms.waitForStatus(pub, (d) => d.fill === 'green', 15000);
    flowId = await deployFlow(nodes);
    await connected;

    const delivered = subscribeOnce(directNc, subject, 8000).then(
      (v) => { console.log('DIAG delivered OK', v); return v; },
      (e) => { console.log('DIAG delivered FAILED', e.message); throw e; }
    );
    const completed = comms.waitForDebug(dbg, 8000).then(
      (v) => { console.log('DIAG debug OK', JSON.stringify(v)); return v; },
      (e) => { console.log('DIAG debug FAILED', e.message); throw e; }
    );
    await triggerInject(inj);

    const [wirePayload, completeMsg] = await Promise.all([delivered, completed]);

    assert.equal(wirePayload, probe, 'the message should genuinely have reached NATS');
    assert.equal(completeMsg.complete.source.id, pub, 'complete event should be attributed to the publish node');
    assert.equal(completeMsg.payload, probe);
  } finally {
    if (flowId) await deleteFlow(flowId).catch(() => {});
    comms.close();
    await directNc.close().catch(() => {});
  }
});

// --- 6. Teardown / redeploy leak check ---------------------------------------

test('publish: redeploying the flow repeatedly does not grow NATS connection count', async (t) => {
  if (!(await checkStack(t))) return;

  const id = uid('teardown-');
  const subject = `test.publish.teardown.${id}`;
  const srv = `${id}srv`;
  const pub = `${id}pub`;
  const inj = `${id}inj`;
  const buildNodes = () => [
    serverNode(srv),
    publishNode(pub, { server: srv, dataformat: 'string', datapointid: subject }),
    injectNode(inj, pub, [{ p: 'payload', v: 'ping', vt: 'str' }]),
  ];

  const comms = connectComms();
  try {
    await comms.ready;
    const baseline = await natsMonitorConnections();

    for (let i = 0; i < 3; i++) {
      const connected = comms.waitForStatus(pub, (d) => d.fill === 'green', 15000);
      const flowId = await deployFlow(buildNodes());
      await connected;

      const directNc = await connectDirectNats();
      try {
        const received = subscribeOnce(directNc, subject, 5000);
        await triggerInject(inj);
        assert.equal(await received, 'ping', `iteration ${i}: publish should still work after redeploy`);
      } finally {
        await directNc.close().catch(() => {});
      }

      await deleteFlow(flowId);
      // Bounded settle window: the server-manager's close handler calls
      // connection.close() without awaiting it, so give the real socket
      // teardown a moment to complete before the next deploy or the final
      // connection count check.
      await new Promise((r) => setTimeout(r, 1500));
    }

    const final = await natsMonitorConnections();
    assert.ok(
      final <= baseline + 1,
      `NATS connection count should not grow across redeploys (baseline ${baseline}, final ${final})`
    );
  } finally {
    comms.close();
  }
});
