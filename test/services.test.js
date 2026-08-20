'use strict';

// Direct integration tests for the @nats-io/services calls used by the
// registered service node. Real Node-RED wrapper behavior is covered
// separately in promotion.test.js.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Svcm } = require('@nats-io/services');
const { ensureStackUp, connectDirectNats } = require('./lib/harness');

test('services: add with a synchronous addEndpoint, request, discover, stats, ping', async t => {
  const skipReason = await ensureStackUp();
  if (skipReason) {
    t.skip(skipReason);
    return;
  }

  const nc = await connectDirectNats();
  const serviceName = `test_svc_${Date.now().toString(36)}`;

  try {
    const service = await new Svcm(nc).add({
      name: serviceName,
      version: '1.0.0',
      description: 'object-4 migration probe',
    });

    // addEndpoint(name, handler) with no explicit subject subscribes on the
    // bare endpoint name when called on the top-level service (its root
    // subject is "") - not `${serviceName}.${name}` as the node's own
    // "endpoint subject" logging/UI field implies (pre-existing behavior,
    // unrelated to this migration; verified against the installed package's
    // service.js ServiceGroupImpl.calcSubject). Pass an explicit unique
    // subject instead of relying on that default, so this test can't
    // collide with anything else answering on the shared dev broker.
    const endpointSubject = `${serviceName}.process`;
    const iter = service.addEndpoint('process', {
      subject: endpointSubject,
      handler: (err, msg) => {
        if (err) return;
        const payload = JSON.parse(msg.string());
        msg.respond(JSON.stringify({ echoed: payload }));
      },
    });
    // addEndpoint() is synchronous - it returns a QueuedIterator<ServiceMsg>
    // immediately rather than a Promise (the pre-migration code awaited it).
    assert.equal(
      typeof iter.stop,
      'function',
      'addEndpoint() should return a QueuedIterator synchronously'
    );

    // addEndpoint() returns before its SUB has round-tripped the server;
    // flush() is the observable signal that the subscription is live.
    await nc.flush();

    const requestPayload = { hello: 'world' };
    const response = await nc.request(
      endpointSubject,
      JSON.stringify(requestPayload),
      { timeout: 2000 }
    );
    assert.deepEqual(response.json(), { echoed: requestPayload });

    const client = new Svcm(nc).client();

    const infos = [];
    for await (const info of await client.info(serviceName)) infos.push(info);
    assert.ok(
      infos.some(i => i.name === serviceName),
      'discovery via client.info() should find the service'
    );

    const stats = [];
    for await (const s of await client.stats(serviceName)) stats.push(s);
    assert.ok(
      stats.length > 0,
      'client.stats() should return at least one entry'
    );
    assert.ok(
      stats[0].endpoints.some(e => e.num_requests >= 1),
      'the request above should be reflected in endpoint stats'
    );

    const pings = [];
    for await (const p of await client.ping(serviceName)) pings.push(p);
    assert.ok(pings.some(p => p.name === serviceName));

    await service.stop();
  } finally {
    try {
      await nc.close();
    } catch {
      // Cleanup is best-effort after the assertion result is known.
    }
  }
});
