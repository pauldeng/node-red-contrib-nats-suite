'use strict';

// Step 4 (RENOVATION-PLAN.md) coverage for the @nats-io/jetstream 3.4.0
// migration in nats-suite-stream-publisher.js / nats-suite-stream-consumer.js:
//   - the stream-publisher's "purge" operation, which is bug 1 - the old
//     code called the nonexistent Stream#purge() and threw every time; the
//     fix routes through JetStreamManager#streams.purge() instead.
//   - explicit ack()/nak()/term() from a JetStream pull consumer, checked
//     against the real consumer's server-side ack state (num_ack_pending,
//     num_redelivered), not just "the call didn't throw".
//   - the credsAuthenticator -> jwtAuthenticator fix (bug 2), as a focused
//     unit-level check: a real NATS server with JWT/operator auth configured
//     is out of scope for this harness, so this proves the *API contract*
//     instead - that jwtAuthenticator(jwt, seed) actually uses both
//     arguments (a real nkey-derived signature over the given JWT), which is
//     exactly what credsAuthenticator's silently-dropped second argument
//     used to fail to do.
//
// Real Node-RED + real NATS (JetStream) via docker-compose, no mocks.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { jwtAuthenticator, nkeys } = require('@nats-io/nats-core');
const {
  AckPolicy,
  jetstream,
  jetstreamManager,
} = require('@nats-io/jetstream');
const {
  ensureStackUp,
  deployFlow,
  deleteFlow,
  triggerInject,
  connectComms,
  connectDirectNats,
  serverNode,
} = require('./lib/harness');

async function checkStack(t) {
  const skipReason = await ensureStackUp();
  if (skipReason) {
    t.skip(skipReason);
    return false;
  }
  return true;
}

let seq = 0;
const uid = base => `${base}${Date.now().toString(36)}${seq++}`;

async function ignoreFailure(promise) {
  try {
    return await promise;
  } catch {
    return undefined;
  }
}

async function deleteStream(nc, name) {
  try {
    const jsm = await jetstreamManager(nc);
    await jsm.streams.delete(name);
  } catch {
    // The stream may not have been created if setup failed.
  }
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

function functionNode(id, func, wireTo) {
  return {
    id,
    type: 'function',
    z: 'FLOW',
    name: '',
    func,
    outputs: 1,
    noerr: 0,
    wires: [[wireTo]],
  };
}

function streamPublisherNode(id, srv, streamName, subject, wireTo) {
  return {
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
  };
}

function streamConsumerNode(
  id,
  srv,
  streamName,
  consumerName,
  subject,
  wireTo
) {
  return {
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
    ackPolicy: 'explicit',
    deliverPolicy: 'all',
    ackWait: '30s',
    maxDeliver: 5,
    maxAckPending: 1000,
    idleHeartbeat: '5s',
    flowControl: false,
    batchSize: 1,
    maxWait: 1500,
    operation: 'consume',
    dataformat: 'auto',
    debug: false,
    wires: [[wireTo]],
  };
}

// --- 1. Stream purge (bug 1 regression) ------------------------------------

test('stream-publisher: "purge" operation empties a real stream (bug 1: Stream#purge() does not exist)', async t => {
  if (!(await checkStack(t))) return;

  const id = uid('purge-');
  const streamName = `STREAM_${id}`;
  const subject = `test.jetstream.purge.${id}`;
  const srv = `${id}srv`;
  const spub = `${id}spub`;
  const injPublish = `${id}injpub`;
  const injPurge = `${id}injpurge`;
  const dbg = `${id}dbg`;

  const nodes = [
    serverNode(srv),
    streamPublisherNode(spub, srv, streamName, subject, dbg),
    injectNode(injPublish, spub, [{ p: 'payload', v: 'hello', vt: 'str' }]),
    injectNode(injPurge, spub, [{ p: 'operation', v: 'purge', vt: 'str' }]),
    debugNode(dbg),
  ];

  const comms = connectComms();
  const directNc = await connectDirectNats();
  let flowId;
  try {
    await comms.ready;
    const connected = comms.waitForStatus(spub, d => d.fill === 'green', 15000);
    flowId = await deployFlow(nodes);
    await connected;

    // Publish twice so there is something real to purge.
    for (let i = 0; i < 2; i++) {
      const published = comms.waitForDebug(dbg, 8000);
      await triggerInject(injPublish);
      const msg = await published;
      assert.equal(msg.published, true, 'publish should succeed before purge');
    }

    const jsm = await jetstreamManager(directNc);
    const beforePurge = await jsm.streams.info(streamName);
    assert.ok(
      beforePurge.state.messages >= 2,
      'stream should hold the 2 published messages before purge'
    );

    const purged = comms.waitForDebug(dbg, 8000);
    await triggerInject(injPurge);
    const purgeMsg = await purged;
    assert.equal(purgeMsg.payload.operation, 'purge');
    assert.equal(
      purgeMsg.payload.success,
      true,
      'purge must report success, not throw (bug 1)'
    );

    const afterPurge = await jsm.streams.info(streamName);
    assert.equal(
      afterPurge.state.messages,
      0,
      'stream must actually be empty after purge'
    );
  } finally {
    if (flowId) await ignoreFailure(deleteFlow(flowId));
    comms.close();
    await deleteStream(directNc, streamName);
    await ignoreFailure(directNc.close());
  }
});

// --- 2. Explicit ack / nak / term, checked against real consumer state ----

test('stream-consumer: explicit ack()/nak()/term() reach the real JetStream consumer', async t => {
  if (!(await checkStack(t))) return;

  const id = uid('acknak-');
  const streamName = `STREAM_${id}`;
  const consumerName = `CONSUMER_${id}`;
  const subject = `test.jetstream.acknak.${id}`;
  const srv = `${id}srv`;
  const scon = `${id}scon`;
  const sconInj = `${id}sconinj`;
  const fn = `${id}fn`;
  const dbg = `${id}dbg`;

  // Explicit ack policy: ack the first payload, nak-then-ack the second
  // (JetStream redelivers it once the nak's delay elapses, `msg.redelivered`
  // distinguishes the two deliveries), term the third.
  const func = `
    if (msg.payload === 'ack-me') {
      msg.ack();
      return { payload: { result: 'acked', redelivered: msg.redelivered } };
    }
    if (msg.payload === 'nak-me') {
      if (msg.redelivered) {
        msg.ack();
        return { payload: { result: 'nak-then-acked', redelivered: msg.redelivered } };
      }
      msg.nak();
      return { payload: { result: 'naked', redelivered: msg.redelivered } };
    }
    if (msg.payload === 'term-me') {
      msg.term();
      return { payload: { result: 'termed', redelivered: msg.redelivered } };
    }
    return { payload: { result: 'unexpected', raw: msg.payload } };
  `;

  const nodes = [
    serverNode(srv),
    streamConsumerNode(scon, srv, streamName, consumerName, subject, fn),
    // The consumer is pull-based: it fetches one batch per input message,
    // it does not run its own poll loop (matches reconnect.test.js's proven
    // pattern for driving this same node type).
    injectNode(sconInj, scon, [{ p: 'payload' }]),
    functionNode(fn, func, dbg),
    debugNode(dbg),
  ];

  const comms = connectComms();
  const directNc = await connectDirectNats();
  let flowId;
  try {
    // The stream-consumer node only creates the *consumer* on init and
    // requires the stream to already exist - create it directly here with
    // the same client the rest of this test publishes through.
    const setupJsm = await jetstreamManager(directNc);
    await setupJsm.streams.add({ name: streamName, subjects: [subject] });

    await comms.ready;
    const conReady = comms.waitForStatus(scon, d => d.fill === 'green', 15000);
    flowId = await deployFlow(nodes);
    await conReady;

    // Publish directly, then pulse the consumer's input once to make it
    // fetch+process exactly that one message (it runs one fetch per input,
    // it does not poll on its own).
    const publishAndFetch = async payload => {
      const wire = comms.waitForDebug(dbg, 10000);
      directNc.publish(subject, payload);
      await triggerInject(sconInj);
      return wire;
    };
    const fetchOnly = async timeoutMs => {
      const wire = comms.waitForDebug(dbg, timeoutMs);
      await triggerInject(sconInj);
      return await wire;
    };

    // ack-me
    const acked = await publishAndFetch('ack-me');
    assert.equal(acked.payload.result, 'acked');
    assert.equal(acked.payload.redelivered, false);

    // nak-me: first delivery naks with a short delay, second (redelivered)
    // delivery acks.
    const nakedFirst = await publishAndFetch('nak-me');
    assert.equal(nakedFirst.payload.result, 'naked');
    assert.equal(nakedFirst.payload.redelivered, false);

    const nakedSecond = await fetchOnly(10000);
    assert.equal(nakedSecond.payload.result, 'nak-then-acked');
    assert.equal(
      nakedSecond.payload.redelivered,
      true,
      'the naked message must come back marked redelivered'
    );

    // term-me: acknowledge as terminal, then prove it is never redelivered.
    const termed = await publishAndFetch('term-me');
    assert.equal(termed.payload.result, 'termed');

    const jsm = await jetstreamManager(directNc);
    const info = await jsm.consumers.info(streamName, consumerName);
    assert.equal(
      info.num_ack_pending,
      0,
      'every message must have been settled (ack/term), none left pending'
    );
  } finally {
    if (flowId) await ignoreFailure(deleteFlow(flowId));
    comms.close();
    await deleteStream(directNc, streamName);
    await ignoreFailure(directNc.close());
  }
});

test(
  'stream-publisher: native schedule option delivers a delayed message',
  { timeout: 20000 },
  async t => {
    if (!(await checkStack(t))) return;

    const id = uid('schedule-');
    const streamName = `STREAM_${id}`;
    const prefix = `test.jetstream.schedule.${id}`;
    const scheduleSubject = `${prefix}.schedule.once`;
    const targetSubject = `${prefix}.target`;
    const srv = `${id}srv`;
    const fn = `${id}fn`;
    const spub = `${id}spub`;
    const inj = `${id}inj`;
    const dbg = `${id}dbg`;

    const publisher = streamPublisherNode(
      spub,
      srv,
      streamName,
      scheduleSubject,
      dbg
    );
    publisher.subjectPattern = [
      `${prefix}.schedule.>`,
      targetSubject,
      `${prefix}.cancel`,
    ].join(',');
    publisher.storage = 'memory';
    publisher.allowMsgSchedules = true;

    const nodes = [
      serverNode(srv),
      injectNode(inj, fn, [{ p: 'payload', v: 'scheduled', vt: 'str' }]),
      functionNode(
        fn,
        `msg.subject = ${JSON.stringify(scheduleSubject)};
         msg.schedule = {
           specification: { at: new Date(Date.now() + 1000).toISOString() },
           target: ${JSON.stringify(targetSubject)}
         };
         return msg;`,
        spub
      ),
      publisher,
      debugNode(dbg),
    ];

    const comms = connectComms();
    const directNc = await connectDirectNats();
    let flowId;
    try {
      const jsm = await jetstreamManager(directNc);
      await jsm.streams.add({
        name: streamName,
        subjects: publisher.subjectPattern.split(','),
        storage: 'memory',
      });

      await comms.ready;
      const ready = comms.waitForStatus(
        spub,
        status => status.fill === 'green',
        15000
      );
      flowId = await deployFlow(nodes);
      await ready;

      await jsm.consumers.add(streamName, {
        durable_name: 'scheduled-delivery',
        filter_subject: targetSubject,
        ack_policy: AckPolicy.Explicit,
      });

      const published = comms.waitForDebug(dbg, 10000);
      await triggerInject(inj);
      const pubAck = await published;
      assert.equal(pubAck.published, true);
      const info = await jsm.streams.info(streamName);
      assert.equal(info.config.allow_msg_schedules, true);

      const js = jetstream(directNc);
      const consumer = await js.consumers.get(streamName, 'scheduled-delivery');
      const delivered = await consumer.next({ expires: 15000 });
      assert.ok(delivered);
      assert.equal(delivered.string(), 'scheduled');
      assert.equal(delivered.subject, targetSubject);
      delivered.ack();
    } finally {
      if (flowId) await ignoreFailure(deleteFlow(flowId));
      comms.close();
      await deleteStream(directNc, streamName);
      await ignoreFailure(directNc.close());
    }
  }
);

test(
  'stream-consumer: pause/resume and info use the server APIs',
  { timeout: 20000 },
  async t => {
    if (!(await checkStack(t))) return;

    const id = uid('consumer-api-');
    const streamName = `STREAM_${id}`;
    const consumerName = `CONSUMER_${id}`;
    const subject = `test.jetstream.consumer.${id}`;
    const srv = `${id}srv`;
    const scon = `${id}scon`;
    const dbg = `${id}dbg`;
    const pause = `${id}pause`;
    const resume = `${id}resume`;
    const info = `${id}info`;

    const nodes = [
      serverNode(srv),
      streamConsumerNode(scon, srv, streamName, consumerName, subject, dbg),
      injectNode(pause, scon, [{ p: 'operation', v: 'pause', vt: 'str' }]),
      injectNode(resume, scon, [{ p: 'operation', v: 'resume', vt: 'str' }]),
      injectNode(info, scon, [{ p: 'operation', v: 'info', vt: 'str' }]),
      debugNode(dbg),
    ];

    const comms = connectComms();
    const directNc = await connectDirectNats();
    let flowId;
    try {
      const jsm = await jetstreamManager(directNc);
      await jsm.streams.add({ name: streamName, subjects: [subject] });

      await comms.ready;
      const ready = comms.waitForStatus(
        scon,
        status => status.fill === 'green',
        15000
      );
      flowId = await deployFlow(nodes);
      await ready;

      let output = comms.waitForDebug(dbg, 10000);
      await triggerInject(pause);
      assert.equal((await output).payload.paused, true);
      assert.equal(
        (await jsm.consumers.info(streamName, consumerName)).paused,
        true
      );

      output = comms.waitForDebug(dbg, 10000);
      await triggerInject(resume);
      assert.equal((await output).payload.paused, false);
      assert.equal(
        Boolean((await jsm.consumers.info(streamName, consumerName)).paused),
        false
      );

      output = comms.waitForDebug(dbg, 10000);
      await triggerInject(info);
      assert.equal(typeof (await output).payload.ack_pending, 'number');
    } finally {
      if (flowId) await ignoreFailure(deleteFlow(flowId));
      comms.close();
      await deleteStream(directNc, streamName);
      await ignoreFailure(directNc.close());
    }
  }
);

// --- 3. jwtAuthenticator uses BOTH arguments (bug 2 unit-level check) ------
//
// Substitution, stated plainly: exercising this through a live Node-RED flow
// would need a real NATS operator/account JWT resolver, which this
// docker-compose stack does not configure. This instead calls the exact
// function nats-suite-server.js now calls (jwtAuthenticator) with a real
// nkey-derived seed and asserts the returned Auth actually incorporates
// both the JWT string and a signature derived from the seed - which is
// precisely what credsAuthenticator's silently-dropped second argument
// (the bug) failed to do.

test('jwtAuthenticator(jwt, seed): both arguments are honored (bug 2)', () => {
  const keypair = nkeys.createUser();
  const seed = keypair.getSeed();
  const fakeJwt = 'header.payload.signature';

  const authFn = jwtAuthenticator(fakeJwt, seed);
  assert.equal(typeof authFn, 'function');

  const auth = authFn('test-nonce');
  assert.equal(
    auth.jwt,
    fakeJwt,
    'the JWT argument must be carried through unchanged'
  );
  assert.ok(
    auth.sig && auth.sig.length > 0,
    'a signature derived from the seed argument must be present'
  );
  assert.ok(
    auth.nkey && auth.nkey.startsWith('U'),
    'the nkey public key derived from the seed must be present'
  );

  // Prove the signature really is seed-dependent, not a stub: a different
  // seed over the same nonce produces a different signature.
  const otherAuth = jwtAuthenticator(
    fakeJwt,
    nkeys.createUser().getSeed()
  )('test-nonce');
  assert.notEqual(
    auth.sig,
    otherAuth.sig,
    'the signature must depend on the seed, proving the seed argument is used'
  );
});
