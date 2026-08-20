'use strict';

const { headers: natsHeaders } = require('@nats-io/nats-core');
const {
  JetStreamApiCodes,
  JetStreamApiError,
  jetstream,
  jetstreamManager,
} = require('@nats-io/jetstream');
const { resolveServer } = require('../lib/connect');
const { attachStatus } = require('../lib/status');
const { toPayload } = require('../lib/payload');
const { parseDuration } = require('../lib/duration');

module.exports = function (RED) {
  function NatsStreamPublisherNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    this.serverConfig = resolveServer(RED, node, config);
    if (!this.serverConfig) return;

    // Validate stream configuration
    if (!config.streamName) {
      node.error('Stream name is required');
      node.status({ fill: 'red', shape: 'ring', text: 'no stream' });
      return;
    }

    let jsClient = null;
    let jsm = null;
    let streamReady = false;
    let statusRevertTimer = null;

    // Helper: Update node status based on connection state
    const updateConnectionStatus = () => {
      const currentStatus = node.serverConfig.connectionStatus;
      if (currentStatus === 'connected') {
        node.status({ fill: 'green', shape: 'dot', text: 'connected' });
      } else if (currentStatus === 'disconnected') {
        node.status({ fill: 'red', shape: 'ring', text: 'disconnected' });
      } else if (currentStatus === 'connecting') {
        node.status({ fill: 'yellow', shape: 'ring', text: 'connecting' });
      }
    };

    // Helper: Revert a transient status message (e.g. "stream created") back
    // to the connection status after 2 seconds. Tracked so close() can cancel
    // a pending revert.
    const scheduleStatusRevert = () => {
      if (statusRevertTimer) clearTimeout(statusRevertTimer);
      statusRevertTimer = setTimeout(() => {
        statusRevertTimer = null;
        updateConnectionStatus();
      }, 2000);
    };

    // Helper: Acquire the JetStream client + manager once and cache them. The
    // connection object is stable across native reconnects, so there is no
    // need to re-derive these on every reconnect.
    const ensureJetStream = async () => {
      if (jsClient && jsm) return;
      const nc = await node.serverConfig.getConnection();
      jsClient = jetstream(nc);
      jsm = await jetstreamManager(nc);
    };

    // Helper: Parse subject patterns (comma-separated or array)
    const parseSubjects = subjects => {
      if (!subjects) return ['*'];
      if (Array.isArray(subjects)) return subjects;
      if (typeof subjects === 'string') {
        return subjects
          .split(',')
          .map(s => s.trim())
          .filter(s => s.length > 0);
      }
      return ['*'];
    };

    // Helper: Get or create stream
    const ensureStream = async () => {
      try {
        await ensureJetStream();

        // Try to get existing stream
        try {
          const info = await jsm.streams.info(config.streamName);
          if (
            (config.allowMsgTtl && !info.config.allow_msg_ttl) ||
            (config.allowMsgSchedules && !info.config.allow_msg_schedules)
          ) {
            await jsm.streams.update(config.streamName, {
              ...info.config,
              allow_msg_ttl: info.config.allow_msg_ttl || !!config.allowMsgTtl,
              allow_msg_schedules:
                info.config.allow_msg_schedules || !!config.allowMsgSchedules,
            });
          }
          streamReady = true;
          node.log(`[STREAM PUB] Stream exists: ${config.streamName}`);
          return true;
        } catch (err) {
          // Stream doesn't exist, create it
          if (
            err instanceof JetStreamApiError &&
            err.code === JetStreamApiCodes.StreamNotFound
          ) {
            node.log(`[STREAM PUB] Creating stream: ${config.streamName}`);

            const streamConfig = {
              name: config.streamName,
              subjects: parseSubjects(config.subjectPattern),
              retention: config.retention || 'limits',
              storage: config.storage === 'memory' ? 'memory' : 'file',
              max_msgs: parseInt(config.maxMessages, 10) || 10000,
              max_bytes: parseInt(config.maxBytes, 10) || 10485760, // 10MB
              max_age: parseDuration(config.maxAge || '24h'),
              duplicate_window: parseDuration(config.duplicateWindow || '2m'),
              num_replicas: parseInt(config.replicas, 10) || 1,
              discard: 'old', // Discard old messages when limits reached
              max_consumers: parseInt(config.maxConsumers, 10) || -1,
              max_msgs_per_subject:
                parseInt(config.maxMsgsPerSubject, 10) || -1,
              max_msg_size: parseInt(config.maxMsgSize, 10) || -1,
              compression: config.compression || 'none',
              allow_direct: !!config.allowDirect,
              mirror_direct: !!config.mirrorDirect,
              deny_delete: !!config.denyDelete,
              deny_purge: !!config.denyPurge,
              allow_rollup_hdrs: !!config.allowRollupHdrs,
              allow_msg_ttl: !!config.allowMsgTtl,
              allow_msg_schedules: !!config.allowMsgSchedules,
            };

            await jsm.streams.add(streamConfig);
            streamReady = true;

            node.log(`[STREAM PUB] Stream created: ${config.streamName}`);

            // Show creation status for 2 seconds
            node.status({
              fill: 'green',
              shape: 'dot',
              text: `stream ${config.streamName} created`,
            });

            // Revert to connection status after 2 seconds
            scheduleStatusRevert();

            return true;
          }
          throw err;
        }
      } catch (err) {
        streamReady = false;
        node.error(`Failed to ensure stream: ${err.message}`);

        // Show error status for 2 seconds
        node.status({ fill: 'red', shape: 'ring', text: 'error' });

        // Revert to connection status after 2 seconds
        scheduleStatusRevert();

        return false;
      }
    };

    // Register with connection pool
    this.serverConfig.registerConnectionUser(node.id);

    // Initialize stream only for publish operation when createOnInit is enabled
    const operation = config.operation || 'publish';
    if (operation === 'publish' && config.createOnInit !== false) {
      ensureStream();
    }

    // Status listener for connection changes (status painting only; the
    // JetStream client and stream handle are established once at node start
    // and stay valid across native reconnects, so there is nothing to tear
    // down or rebuild here). Default connected/disconnected/connecting paint
    // is exactly what this node needs - no custom handlers.
    const detachStatus = attachStatus(node, this.serverConfig);

    // Stream Management Operations
    const performStreamOperation = async msg => {
      try {
        await ensureJetStream();

        const operation = msg.operation || config.operation || 'publish';
        const streamName = msg.stream || config.streamName || '';

        if (!streamName && operation !== 'list') {
          node.error('Stream name required for operation');
          return;
        }

        switch (operation) {
          case 'create': {
            // Accept full stream config from msg.payload or build from individual properties
            let streamConfig;

            if (
              msg.payload &&
              typeof msg.payload === 'object' &&
              msg.payload.name
            ) {
              // Use msg.payload as full stream config (NATS native format)
              streamConfig = { ...msg.payload };
              node.log(
                `[STREAM PUB] Creating stream from payload config: ${streamConfig.name}`
              );
            } else {
              // Build config from individual msg/node properties (legacy support)
              const targetStreamName = msg.stream || streamName;
              streamConfig = {
                name: targetStreamName,
                subjects: msg.subjects
                  ? parseSubjects(msg.subjects)
                  : parseSubjects(
                      config.subjectPattern || targetStreamName + '.>'
                    ),
                retention: msg.retention || config.retention || 'limits',
                storage:
                  msg.storage ||
                  (config.storage === 'memory' ? 'memory' : 'file'),
                max_msgs:
                  msg.maxMessages !== undefined
                    ? parseInt(msg.maxMessages, 10)
                    : config.maxMessages !== undefined
                      ? parseInt(config.maxMessages, 10)
                      : -1,
                max_bytes:
                  msg.maxBytes !== undefined
                    ? parseInt(msg.maxBytes, 10)
                    : config.maxBytes !== undefined
                      ? parseInt(config.maxBytes, 10)
                      : -1,
                max_age: msg.maxAge
                  ? parseDuration(msg.maxAge)
                  : config.maxAge
                    ? parseDuration(config.maxAge)
                    : parseDuration('24h'),
                duplicate_window: msg.duplicateWindow
                  ? parseDuration(msg.duplicateWindow)
                  : config.duplicateWindow
                    ? parseDuration(config.duplicateWindow)
                    : parseDuration('2m'),
                num_replicas:
                  msg.replicas !== undefined
                    ? parseInt(msg.replicas, 10)
                    : config.replicas !== undefined
                      ? parseInt(config.replicas, 10)
                      : 1,
                discard: msg.discard || config.discard || 'old',
                // Extended NATS config options
                max_consumers:
                  msg.maxConsumers !== undefined
                    ? parseInt(msg.maxConsumers, 10)
                    : config.maxConsumers !== undefined
                      ? parseInt(config.maxConsumers, 10)
                      : -1,
                max_msgs_per_subject:
                  msg.maxMsgsPerSubject !== undefined
                    ? parseInt(msg.maxMsgsPerSubject, 10)
                    : config.maxMsgsPerSubject !== undefined
                      ? parseInt(config.maxMsgsPerSubject, 10)
                      : -1,
                max_msg_size:
                  msg.maxMsgSize !== undefined
                    ? parseInt(msg.maxMsgSize, 10)
                    : config.maxMsgSize !== undefined
                      ? parseInt(config.maxMsgSize, 10)
                      : -1,
                compression: msg.compression || config.compression || 'none',
                allow_direct:
                  msg.allowDirect !== undefined
                    ? msg.allowDirect
                    : config.allowDirect || false,
                mirror_direct:
                  msg.mirrorDirect !== undefined
                    ? msg.mirrorDirect
                    : config.mirrorDirect || false,
                deny_delete:
                  msg.denyDelete !== undefined
                    ? msg.denyDelete
                    : config.denyDelete || false,
                deny_purge:
                  msg.denyPurge !== undefined
                    ? msg.denyPurge
                    : config.denyPurge || false,
                allow_rollup_hdrs:
                  msg.allowRollupHdrs !== undefined
                    ? msg.allowRollupHdrs
                    : config.allowRollupHdrs || false,
                allow_msg_ttl:
                  msg.allowMsgTtl !== undefined
                    ? msg.allowMsgTtl
                    : config.allowMsgTtl || false,
                allow_msg_schedules:
                  msg.allowMsgSchedules !== undefined
                    ? msg.allowMsgSchedules
                    : config.allowMsgSchedules || false,
              };
              node.log(
                `[STREAM PUB] Creating stream from properties: ${streamConfig.name}`
              );
            }

            // Create the stream
            const createdStream = await jsm.streams.add(streamConfig);

            // Return full status with config and state
            msg.payload = {
              operation: 'create',
              success: true,
              stream: createdStream.config.name,
              config: createdStream.config,
              state: createdStream.state,
              created: createdStream.created,
            };
            node.log(
              `[STREAM PUB] Stream created successfully: ${createdStream.config.name}`
            );

            // Show creation status for 2 seconds
            node.status({
              fill: 'green',
              shape: 'dot',
              text: `stream ${createdStream.config.name} created`,
            });

            // Revert to connection status after 2 seconds
            scheduleStatusRevert();

            break;
          }

          case 'update': {
            // Accept full stream config from msg.payload or build from individual properties
            let streamConfig;
            const targetStreamName =
              msg.payload?.name || msg.stream || streamName;

            // Get current stream config first
            const currentStream = await jsm.streams.info(targetStreamName);

            if (
              msg.payload &&
              typeof msg.payload === 'object' &&
              msg.payload.name
            ) {
              // Merge msg.payload with current config (NATS native format)
              streamConfig = { ...currentStream.config, ...msg.payload };
              streamConfig.sealed =
                currentStream.config.sealed || streamConfig.sealed;
              streamConfig.allow_msg_ttl =
                currentStream.config.allow_msg_ttl ||
                streamConfig.allow_msg_ttl;
              streamConfig.allow_msg_schedules =
                currentStream.config.allow_msg_schedules ||
                streamConfig.allow_msg_schedules;
              node.log(
                `[STREAM PUB] Updating stream from payload config: ${streamConfig.name}`
              );
            } else {
              // Build config from individual msg/node properties (legacy support)
              streamConfig = {
                ...currentStream.config,
                subjects: msg.subjects
                  ? parseSubjects(msg.subjects)
                  : config.subjectPattern
                    ? parseSubjects(config.subjectPattern)
                    : currentStream.config.subjects,
                retention:
                  msg.retention ||
                  config.retention ||
                  currentStream.config.retention,
                max_msgs:
                  msg.maxMessages !== undefined
                    ? parseInt(msg.maxMessages, 10)
                    : config.maxMessages !== undefined
                      ? parseInt(config.maxMessages, 10)
                      : currentStream.config.max_msgs,
                max_bytes:
                  msg.maxBytes !== undefined
                    ? parseInt(msg.maxBytes, 10)
                    : config.maxBytes !== undefined
                      ? parseInt(config.maxBytes, 10)
                      : currentStream.config.max_bytes,
                max_age: msg.maxAge
                  ? parseDuration(msg.maxAge)
                  : config.maxAge
                    ? parseDuration(config.maxAge)
                    : currentStream.config.max_age,
                duplicate_window: msg.duplicateWindow
                  ? parseDuration(msg.duplicateWindow)
                  : config.duplicateWindow
                    ? parseDuration(config.duplicateWindow)
                    : currentStream.config.duplicate_window,
                num_replicas:
                  msg.replicas !== undefined
                    ? parseInt(msg.replicas, 10)
                    : config.replicas !== undefined
                      ? parseInt(config.replicas, 10)
                      : currentStream.config.num_replicas,
                discard:
                  msg.discard || config.discard || currentStream.config.discard,
                // Extended NATS config options
                max_consumers:
                  msg.maxConsumers !== undefined
                    ? parseInt(msg.maxConsumers, 10)
                    : config.maxConsumers !== undefined
                      ? parseInt(config.maxConsumers, 10)
                      : currentStream.config.max_consumers,
                max_msgs_per_subject:
                  msg.maxMsgsPerSubject !== undefined
                    ? parseInt(msg.maxMsgsPerSubject, 10)
                    : config.maxMsgsPerSubject !== undefined
                      ? parseInt(config.maxMsgsPerSubject, 10)
                      : currentStream.config.max_msgs_per_subject,
                max_msg_size:
                  msg.maxMsgSize !== undefined
                    ? parseInt(msg.maxMsgSize, 10)
                    : config.maxMsgSize !== undefined
                      ? parseInt(config.maxMsgSize, 10)
                      : currentStream.config.max_msg_size,
                compression:
                  msg.compression ||
                  config.compression ||
                  currentStream.config.compression,
                allow_direct:
                  msg.allowDirect !== undefined
                    ? msg.allowDirect
                    : config.allowDirect !== undefined
                      ? config.allowDirect
                      : currentStream.config.allow_direct,
                mirror_direct:
                  msg.mirrorDirect !== undefined
                    ? msg.mirrorDirect
                    : config.mirrorDirect !== undefined
                      ? config.mirrorDirect
                      : currentStream.config.mirror_direct,
                sealed:
                  msg.sealed !== undefined
                    ? msg.sealed
                    : config.sealed !== undefined
                      ? config.sealed
                      : currentStream.config.sealed,
                deny_delete:
                  msg.denyDelete !== undefined
                    ? msg.denyDelete
                    : config.denyDelete !== undefined
                      ? config.denyDelete
                      : currentStream.config.deny_delete,
                deny_purge:
                  msg.denyPurge !== undefined
                    ? msg.denyPurge
                    : config.denyPurge !== undefined
                      ? config.denyPurge
                      : currentStream.config.deny_purge,
                allow_rollup_hdrs:
                  msg.allowRollupHdrs !== undefined
                    ? msg.allowRollupHdrs
                    : config.allowRollupHdrs !== undefined
                      ? config.allowRollupHdrs
                      : currentStream.config.allow_rollup_hdrs,
                allow_msg_ttl:
                  currentStream.config.allow_msg_ttl ||
                  msg.allowMsgTtl === true ||
                  config.allowMsgTtl === true,
                allow_msg_schedules:
                  currentStream.config.allow_msg_schedules ||
                  msg.allowMsgSchedules === true ||
                  config.allowMsgSchedules === true,
              };
              node.log(
                `[STREAM PUB] Updating stream from properties: ${streamConfig.name}`
              );
            }

            // Update the stream
            const updatedStream = await jsm.streams.update(
              targetStreamName,
              streamConfig
            );

            // Return full status with config and state
            msg.payload = {
              operation: 'update',
              success: true,
              stream: updatedStream.config.name,
              config: updatedStream.config,
              state: updatedStream.state,
            };
            node.log(
              `[STREAM PUB] Stream updated successfully: ${updatedStream.config.name}`
            );

            // Show update status for 2 seconds
            node.status({
              fill: 'green',
              shape: 'dot',
              text: `stream ${updatedStream.config.name} updated`,
            });

            // Revert to connection status after 2 seconds
            scheduleStatusRevert();

            break;
          }

          case 'update-subjects': {
            // Update stream subjects only (without changing other config)
            const currentStream = await jsm.streams.info(streamName);
            const updatedConfig = {
              ...currentStream.config,
              subjects: msg.subjects
                ? parseSubjects(msg.subjects)
                : currentStream.config.subjects,
            };

            await jsm.streams.update(streamName, updatedConfig);
            msg.payload = {
              operation: 'update-subjects',
              stream: streamName,
              subjects: updatedConfig.subjects,
              success: true,
            };
            node.log(
              `[STREAM PUB] Updated subjects for ${streamName}: ${updatedConfig.subjects.join(', ')}`
            );
            break;
          }

          case 'info': {
            const targetStreamName =
              msg.payload?.name || msg.stream || streamName;
            const info = await jsm.streams.info(targetStreamName, msg.options);
            msg.payload = {
              operation: 'info',
              success: true,
              stream: targetStreamName,
              config: info.config,
              state: info.state,
              created: info.created,
              cluster: info.cluster,
              mirror: info.mirror,
              sources: info.sources,
            };
            break;
          }

          case 'delete': {
            await jsm.streams.delete(streamName);
            msg.payload = {
              operation: 'delete',
              stream: streamName,
              success: true,
            };

            // Show delete status for 2 seconds
            node.status({
              fill: 'green',
              shape: 'dot',
              text: `stream ${streamName} deleted`,
            });

            // Revert to connection status after 2 seconds
            scheduleStatusRevert();

            break;
          }

          case 'purge': {
            // jsClient.streams.get(name) returns a Stream with no .purge() -
            // only JetStreamManager.streams has it (verified against
            // @nats-io/jetstream 3.4.0 types).
            await jsm.streams.purge(streamName, msg.options);
            msg.payload = {
              operation: 'purge',
              stream: streamName,
              success: true,
            };
            break;
          }

          case 'list': {
            const streams = [];
            for await (const stream of jsm.streams.list(msg.subject)) {
              streams.push({
                name: stream.config.name,
                subjects: stream.config.subjects,
                messages: stream.state.messages,
                bytes: stream.state.bytes,
              });
            }
            msg.payload = streams;
            msg.operation = 'list';
            msg.count = streams.length;
            break;
          }

          default:
            node.error(`Unknown operation: ${operation}`);
            return;
        }

        node.send(msg);
      } catch (err) {
        node.error(`Stream operation failed: ${err.message}`, msg);
        msg.error = err.message;

        // Show error status for 2 seconds
        node.status({ fill: 'red', shape: 'ring', text: 'error' });

        // Revert to connection status after 2 seconds
        scheduleStatusRevert();

        node.send(msg);
      }
    };

    // Input handler
    node.on('input', async function (msg) {
      try {
        // Check if this is a stream management operation
        const operation = msg.operation || config.operation || 'publish';

        if (operation !== 'publish') {
          await performStreamOperation(msg);
          return;
        }

        // Ensure we have a JetStream client
        if (!streamReady) {
          const ready = await ensureStream();
          if (!ready) {
            node.error('Stream not ready', msg);
            return;
          }
        }

        // Determine subject
        let subject = msg.subject || config.defaultSubject;
        if (!subject) {
          node.error(
            'No subject specified (use msg.subject or configure default subject)',
            msg
          );
          return;
        }

        // Prepare payload
        const payload = toPayload(msg.payload);

        // Prepare headers if provided
        let msgHeaders;
        if (msg.headers && typeof msg.headers === 'object') {
          msgHeaders = natsHeaders();
          Object.keys(msg.headers).forEach(key => {
            msgHeaders.append(key, String(msg.headers[key]));
          });
        }

        // Pass native JetStreamPublishOptions through so new client options
        // don't require a new node release. Direct schedule aliases keep
        // delayed delivery convenient in Node-RED flows.
        const publishOptions = { ...msg.options };
        if (msgHeaders) publishOptions.headers = msgHeaders;
        if (msg._msgID !== undefined) publishOptions.msgID = msg._msgID;
        if (msg.schedule !== undefined) publishOptions.schedule = msg.schedule;
        if (msg.cancelSchedule !== undefined)
          publishOptions.cancelSchedule = msg.cancelSchedule;

        const pubAck = await jsClient.publish(subject, payload, publishOptions);

        // Update message with publish info
        msg.stream = pubAck.stream;
        msg.sequence = pubAck.seq;
        msg.published = true;
        msg.subject = subject;
        msg._duplicate = pubAck.duplicate || false;

        // Send message to output
        node.send(msg);
      } catch (err) {
        msg.published = false;
        msg.error = err.message;

        node.error(`Stream publish error: ${err.message}`, msg);

        // Send error message to output
        node.send(msg);
      }
    });

    // Cleanup on close
    node.on('close', function (done) {
      if (statusRevertTimer) {
        clearTimeout(statusRevertTimer);
        statusRevertTimer = null;
      }
      detachStatus();
      this.serverConfig.unregisterConnectionUser(node.id);
      jsClient = null;
      jsm = null;
      streamReady = false;
      node.status({});
      done();
    });
  }

  RED.nodes.registerType(
    'nats-suite-stream-publisher',
    NatsStreamPublisherNode
  );
};
