'use strict';

const {
  JetStreamApiCodes,
  JetStreamApiError,
  jetstream,
  jetstreamManager,
} = require('@nats-io/jetstream');
const { resolveServer } = require('../lib/connect');
const { attachStatus } = require('../lib/status');
const { fromMsg } = require('../lib/payload');
const { parseDuration } = require('../lib/duration');

module.exports = function (RED) {
  function NatsStreamConsumerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    this.serverConfig = resolveServer(RED, node, config);
    if (!this.serverConfig) return;

    // Validate stream/consumer configuration
    if (!config.streamName) {
      node.error('Stream name is required');
      node.status({ fill: 'red', shape: 'ring', text: 'no stream' });
      return;
    }

    if (!config.consumerName) {
      node.error('Consumer name is required');
      node.status({ fill: 'red', shape: 'ring', text: 'no consumer' });
      return;
    }

    let jsClient = null;
    let jsm = null;
    let consumer = null;
    let isConsuming = false;
    let isPaused = false; // Pause state
    let idleTimeout = null; // Timer for idle status
    let fetchTask = null;
    let activeMessages = null;
    let closing = false;

    const setGreenStatus = (text, shape = 'dot') => {
      if (node.serverConfig.connectionStatus !== 'connected') return false;
      node.status({ fill: 'green', shape, text });
      return true;
    };

    // Output configuration
    const isDebug = !!config.debug;
    const parseMode = config.dataformat || 'auto';

    // Helper: Convert BigInt to Number safely
    const toNumber = value => {
      if (value === undefined || value === null) return 0;
      if (typeof value === 'bigint') return Number(value);
      return value;
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

    // Helper: Get or create consumer
    const ensureConsumer = async () => {
      try {
        await ensureJetStream();

        // Check if stream exists
        try {
          await jsm.streams.info(config.streamName);
        } catch (err) {
          if (
            err instanceof JetStreamApiError &&
            err.code === JetStreamApiCodes.StreamNotFound
          ) {
            node.status({
              fill: 'red',
              shape: 'ring',
              text: 'stream not found',
            });
            throw new Error(`Stream not found: ${config.streamName}`, {
              cause: err,
            });
          }
          throw err;
        }

        // Check if createOnInit is enabled (default to true for backwards compatibility)
        const createOnInit = config.createOnInit !== false;

        // Try to get existing consumer first
        try {
          consumer = await jsClient.consumers.get(
            config.streamName,
            config.consumerName
          );
          node.log(`[STREAM CONSUMER] Consumer exists: ${config.consumerName}`);
        } catch (err) {
          // Consumer doesn't exist
          if (
            err instanceof JetStreamApiError &&
            err.code === JetStreamApiCodes.ConsumerNotFound
          ) {
            if (createOnInit) {
              // Configure consumer
              const consumerConfig = {
                durable_name: config.consumerName,
                ack_policy: config.ackPolicy || 'explicit',
                ack_wait: parseDuration(config.ackWait || '30s'),
                max_deliver: parseInt(config.maxDeliver, 10) || 5,
                max_ack_pending: parseInt(config.maxAckPending, 10) || 1000,
                deliver_policy: config.deliverPolicy || 'new',
              };

              // ConsumerConfig.idle_heartbeat and flow_control both require a
              // push-based consumer (verified against a real nats-server:
              // "consumer idle heartbeat requires a push based consumer");
              // this node is pull-only, so neither is ever set.

              // Add filter subject if specified
              if (config.filterSubject) {
                consumerConfig.filter_subject = config.filterSubject;
              }

              // Add deliver policy specific options
              if (config.deliverPolicy === 'by_start_sequence') {
                consumerConfig.opt_start_seq =
                  parseInt(config.startSequence, 10) || 1;
              } else if (config.deliverPolicy === 'by_start_time') {
                consumerConfig.opt_start_time =
                  config.startTime || new Date().toISOString();
              }

              node.log(
                `[STREAM CONSUMER] Creating consumer: ${config.consumerName}`
              );
              await jsm.consumers.add(config.streamName, consumerConfig);
              consumer = await jsClient.consumers.get(
                config.streamName,
                config.consumerName
              );
              node.log(
                `[STREAM CONSUMER] Consumer created: ${config.consumerName}`
              );
            } else {
              // createOnInit is disabled, consumer must exist
              node.status({
                fill: 'red',
                shape: 'ring',
                text: 'consumer not found',
              });
              throw new Error(
                `Consumer not found: ${config.consumerName}. Enable "Create Consumer on initialization" or create the consumer manually.`,
                { cause: err }
              );
            }
          } else {
            throw err;
          }
        }

        setGreenStatus(`${config.consumerName} (ready)`);
        return true;
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'consumer error' });
        throw err;
      }
    };

    // Helper: Process a single message
    const processMessage = async (send, jetMsg) => {
      try {
        // Decode payload
        const data = jetMsg.string();

        if (isDebug) {
          node.log(
            `[STREAM CONSUMER] Processing message from subject: ${jetMsg.subject}`
          );
        }

        // Parse based on mode (shared with the subscribe node's decoder)
        let payload;
        try {
          payload = fromMsg(jetMsg, parseMode);
        } catch (parseError) {
          // Only forced 'json' mode can throw here (see lib/payload.js).
          if (isDebug) {
            node.log(
              `[STREAM CONSUMER] JSON parsing failed: ${parseError.message}`
            );
          }
          const err = new Error('JSON parsing failed', { cause: parseError });
          err.code = 'JSON_PARSE_ERROR';
          err.rawData = data;
          throw err;
        }

        // Build output message
        const outMsg = {
          payload: payload,
          stream: jetMsg.info.stream,
          consumer: jetMsg.info.consumer,
          subject: jetMsg.subject,
          sequence: toNumber(jetMsg.seq),
          timestamp: Date.now(),
          redelivered: jetMsg.info.redelivered || false,
          redelivery_count: toNumber(jetMsg.info.redeliveryCount) || 0,
          pending: toNumber(jetMsg.info.pending) || 0,
        };

        // Add headers if present
        if (jetMsg.headers) {
          const headers = {};
          for (const [key, values] of jetMsg.headers) {
            headers[key] = values.length === 1 ? values[0] : values;
          }
          outMsg.headers = headers;
        }

        // Add acknowledgment functions based on policy
        if (config.ackPolicy === 'explicit') {
          outMsg.ack = () => {
            try {
              jetMsg.ack();
              node.log(`[STREAM CONSUMER] ACK: seq ${jetMsg.seq}`);
            } catch (err) {
              node.warn(`Failed to ACK message: ${err.message}`);
            }
          };

          outMsg.nak = delay => {
            try {
              if (delay) {
                jetMsg.nak(delay);
              } else {
                jetMsg.nak();
              }
              node.log(`[STREAM CONSUMER] NAK: seq ${jetMsg.seq}`);
            } catch (err) {
              node.warn(`Failed to NAK message: ${err.message}`);
            }
          };

          outMsg.term = () => {
            try {
              jetMsg.term();
              node.log(`[STREAM CONSUMER] TERM: seq ${jetMsg.seq}`);
            } catch (err) {
              node.warn(`Failed to TERM message: ${err.message}`);
            }
          };

          outMsg.inProgress = () => {
            try {
              jetMsg.working();
              node.log(`[STREAM CONSUMER] IN-PROGRESS: seq ${jetMsg.seq}`);
            } catch (err) {
              node.warn(`Failed to mark in-progress: ${err.message}`);
            }
          };
        } else if (config.ackPolicy === 'all') {
          // Auto-acknowledge
          jetMsg.ack();
        }
        // 'none' policy doesn't require acknowledgment

        // Send message
        if (isDebug) {
          node.log(
            `[STREAM CONSUMER] Sending output message for seq ${jetMsg.seq}`
          );
        }
        send(outMsg);

        // Update status
        const pending = toNumber(jetMsg.info.pending) || 0;
        node.status({
          fill: 'blue',
          shape: 'dot',
          text: `${config.consumerName} (${pending} pending)`,
        });

        // Clear any existing idle timeout
        if (idleTimeout) {
          clearTimeout(idleTimeout);
          idleTimeout = null;
        }

        // Set timeout to change to idle status after 2 seconds
        idleTimeout = setTimeout(() => {
          if (!closing && !isConsuming && !isPaused)
            setGreenStatus(`${config.consumerName} (idle)`, 'ring');
          idleTimeout = null;
        }, 2000);
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'message error' });
        throw err;
      }
    };

    // Helper: Consume messages
    const consumeMessages = async (msg, send) => {
      if (!consumer || isConsuming || isPaused || closing) return;

      try {
        isConsuming = true;

        const maxWait =
          parseInt(msg.options?.expires ?? config.maxWait, 10) || 1000;
        const batch =
          parseInt(
            msg.options?.max_messages ?? msg.batchSize ?? config.batchSize,
            10
          ) || 1;

        // Set waiting status (blue) before fetching
        node.status({
          fill: 'blue',
          shape: 'ring',
          text: `${config.consumerName} (waiting)`,
        });

        node.log(
          `[STREAM CONSUMER] Fetching ${batch} messages (max wait: ${maxWait}ms)`
        );

        // Fetch messages
        fetchTask = consumer.fetch({
          ...msg.options,
          max_messages: batch,
          expires: maxWait,
        });
        const messages = await fetchTask;
        fetchTask = null;
        activeMessages = messages;
        if (closing) messages.stop();

        let count = 0;
        for await (const jetMsg of messages) {
          // Check if paused during iteration
          if (isPaused || closing) {
            node.log(`[STREAM CONSUMER] Paused during consumption`);
            break;
          }
          try {
            await processMessage(send, jetMsg);
          } catch (err) {
            // Don't let one bad message (e.g. JSON parse failure) abort the
            // rest of an already-fetched batch; log and move on so the
            // remaining pulled messages still get processed/acked.
            node.warn(
              `[STREAM CONSUMER] Failed to process message seq ${jetMsg.seq}: ${err.message}`
            );
          }
          count++;
        }

        if (count > 0) {
          node.log(`[STREAM CONSUMER] Processed ${count} messages`);
        } else if (!isPaused) {
          // No messages found after timeout, change to idle status (green)
          setGreenStatus(`${config.consumerName} (idle)`, 'ring');
        }
      } catch (err) {
        if (!closing && (!err.message || !err.message.includes('timeout'))) {
          node.status({ fill: 'red', shape: 'ring', text: 'consume error' });
          throw err;
        }
      } finally {
        fetchTask = null;
        activeMessages?.stop();
        activeMessages = null;
        isConsuming = false;
      }
    };

    // Register with connection pool
    this.serverConfig.registerConnectionUser(node.id);

    // Initialize consumer
    const initializeConsumer = async () => {
      try {
        await ensureConsumer();
      } catch (err) {
        node.error(`Failed to ensure consumer: ${err.message}`);
      }
    };
    initializeConsumer();

    // Status listener for connection changes (status painting only; the
    // consumer is established once at node start and its handle stays valid
    // across native reconnects, so there is nothing to tear down or rebuild
    // here). disconnected/connecting use the default paint; 'connected'
    // shows the consumer name instead of the generic text.
    const detachStatus = attachStatus(node, this.serverConfig, {
      connected: () => setGreenStatus(`${config.consumerName} (ready)`),
    });

    // Stream Management Operations
    // Returns null on success, or the error to report via done(err) -
    // node.error() itself is the caller's job now, so one failure doesn't
    // fire Catch nodes twice.
    const performStreamOperation = async (msg, send) => {
      try {
        await ensureJetStream();

        const operation = msg.operation || config.operation || 'consume';
        const streamName = msg.stream || config.streamName || '';

        if (!streamName) {
          return new Error('Stream name required for operation');
        }

        switch (operation) {
          case 'info': {
            const streamInfo = await jsm.streams.info(streamName, msg.options);
            msg.payload = {
              operation: 'info',
              stream: streamName,
              config: streamInfo.config,
              state: streamInfo.state,
            };
            setGreenStatus(streamName);
            break;
          }

          case 'delete': {
            await jsm.streams.delete(streamName);
            msg.payload = {
              operation: 'delete',
              stream: streamName,
              success: true,
            };
            setGreenStatus(`deleted: ${streamName}`);
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
            setGreenStatus(`purged: ${streamName}`);
            break;
          }

          default:
            return new Error(`Unknown operation: ${operation}`);
        }

        send(msg);
        return null;
      } catch (err) {
        msg.error = err.message;
        send(msg);
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        return err;
      }
    };

    // Consumer Management Operations. Same return contract as
    // performStreamOperation above.
    const performConsumerOperation = async (msg, send) => {
      try {
        await ensureJetStream();

        const operation = msg.operation || config.operation || 'consume';
        const streamName = msg.stream || config.streamName || '';
        const consumerName = msg.consumer || config.consumerName || '';

        if (!streamName && operation !== 'list') {
          return new Error('Stream name required for consumer operation');
        }

        if (!consumerName && operation !== 'list' && operation !== 'create') {
          return new Error('Consumer name required for operation');
        }

        switch (operation) {
          case 'create':
          case 'add': {
            const consumerConfig = msg.config || {
              durable_name: consumerName || config.consumerName,
              deliver_subject: msg.deliverSubject,
              deliver_policy:
                msg.deliverPolicy || config.deliverPolicy || 'all',
              ack_policy: msg.ackPolicy || config.ackPolicy || 'explicit',
              max_deliver: msg.maxDeliver
                ? parseInt(msg.maxDeliver, 10)
                : config.maxDeliver
                  ? parseInt(config.maxDeliver, 10)
                  : -1,
              filter_subject: msg.filterSubject || config.filterSubject,
              ack_wait: msg.ackWait
                ? parseDuration(msg.ackWait)
                : config.ackWait
                  ? parseDuration(config.ackWait)
                  : undefined,
              max_ack_pending: msg.maxAckPending
                ? parseInt(msg.maxAckPending, 10)
                : config.maxAckPending
                  ? parseInt(config.maxAckPending, 10)
                  : undefined,
            };

            // Remove undefined values
            Object.keys(consumerConfig).forEach(key => {
              if (consumerConfig[key] === undefined) delete consumerConfig[key];
            });

            const createdConsumer = await jsm.consumers.add(
              streamName,
              consumerConfig,
              msg.options
            );
            msg.payload = {
              operation: 'create',
              consumer: createdConsumer.name,
              success: true,
            };
            setGreenStatus(`created: ${createdConsumer.name}`);
            break;
          }

          case 'info': {
            const consumerInfo = await jsm.consumers.info(
              streamName,
              consumerName
            );
            msg.payload = {
              operation: 'info',
              stream: streamName,
              consumer: consumerName,
              config: consumerInfo.config,
              delivered: consumerInfo.delivered,
              ack_pending: consumerInfo.num_ack_pending,
            };
            setGreenStatus(consumerName);
            break;
          }

          case 'delete': {
            await jsm.consumers.delete(streamName, consumerName);
            msg.payload = {
              operation: 'delete',
              consumer: consumerName,
              success: true,
            };
            setGreenStatus(`deleted: ${consumerName}`);
            break;
          }

          case 'list': {
            const consumers = [];
            for await (const consumer of jsm.consumers.list(streamName)) {
              consumers.push({
                name: consumer.name,
                stream: streamName,
                config: consumer.config,
              });
            }
            msg.payload = consumers;
            msg.operation = 'list';
            msg.count = consumers.length;
            setGreenStatus(`${consumers.length} consumers`);
            break;
          }

          case 'pause': {
            const result = await jsm.consumers.pause(
              streamName,
              consumerName,
              new Date(msg.until || '9999-12-31T23:59:59.999Z')
            );
            isPaused = result.paused;
            msg.payload = {
              operation: 'pause',
              consumer: consumerName,
              success: true,
              ...result,
            };
            node.status({
              fill: 'yellow',
              shape: 'ring',
              text: `${consumerName} (paused)`,
            });
            node.log(`[STREAM CONSUMER] Consumer paused: ${consumerName}`);
            break;
          }

          case 'resume': {
            const result = await jsm.consumers.resume(streamName, consumerName);
            isPaused = result.paused;
            msg.payload = {
              operation: 'resume',
              consumer: consumerName,
              success: true,
              ...result,
            };
            setGreenStatus(`${consumerName} (resumed)`);
            node.log(`[STREAM CONSUMER] Consumer resumed: ${consumerName}`);
            break;
          }

          case 'monitor': {
            // Get detailed consumer monitoring stats
            const consumerInfo = await jsm.consumers.info(
              streamName,
              consumerName
            );

            // Calculate additional stats
            const pending = consumerInfo.num_pending || 0;
            const delivered = consumerInfo.delivered?.consumer_seq || 0;
            const ackPending = consumerInfo.num_ack_pending || 0;
            const redelivered = consumerInfo.num_redelivered || 0;
            const waiting = consumerInfo.num_waiting || 0;

            msg.payload = {
              operation: 'monitor',
              stream: streamName,
              consumer: consumerName,
              stats: {
                pending: pending,
                delivered: delivered,
                ack_pending: ackPending,
                redelivered: redelivered,
                waiting: waiting,
                paused: isPaused,
              },
              config: consumerInfo.config,
              timestamp: Date.now(),
            };

            node.status({
              fill: 'blue',
              shape: 'dot',
              text: `${consumerName} (${pending}p/${ackPending}ap)`,
            });

            node.log(
              `[STREAM CONSUMER] Monitor: pending=${pending}, ack_pending=${ackPending}, delivered=${delivered}`
            );
            break;
          }

          default:
            return new Error(`Unknown consumer operation: ${operation}`);
        }

        send(msg);
        return null;
      } catch (err) {
        msg.error = err.message;
        send(msg);
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        return err;
      }
    };

    // Input handler - trigger message consumption, stream management, or consumer management
    node.on('input', async function (msg, send, done) {
      try {
        // Check if this is a stream management or consumer management operation
        const operation = msg.operation || config.operation || 'consume';

        // Stream management operations
        if (operation === 'purge') {
          done(await performStreamOperation(msg, send));
          return;
        }

        // Consumer management operations
        if (
          [
            'create',
            'add',
            'info',
            'delete',
            'list',
            'pause',
            'resume',
            'monitor',
          ].includes(operation)
        ) {
          done(await performConsumerOperation(msg, send));
          return;
        }

        // Default: consume messages
        if (operation !== 'consume') {
          done(new Error(`Unknown operation: ${operation}`));
          return;
        }

        // Ensure consumer is ready
        if (!consumer) {
          await ensureConsumer();
        }

        await consumeMessages(msg, send);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        done(err);
      }
    });

    // Cleanup on close
    node.on('close', async function (done) {
      let closeError;
      try {
        closing = true;
        if (idleTimeout) clearTimeout(idleTimeout);
        const messages = activeMessages || (fetchTask && (await fetchTask));
        if (messages) {
          messages.stop();
          await messages.closed();
        }
      } catch (err) {
        closeError = err;
      } finally {
        detachStatus();
        this.serverConfig.unregisterConnectionUser(node.id);
        consumer = null;
        jsClient = null;
        jsm = null;
        node.status({});
        done(closeError);
      }
    });
  }

  RED.nodes.registerType('nats-suite-stream-consumer', NatsStreamConsumerNode);
};
