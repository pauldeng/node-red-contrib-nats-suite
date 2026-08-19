'use strict';

const { StringCodec, headers: natsHeaders } = require('nats');

module.exports = function (RED) {
  function NatsPublishNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const sc = StringCodec();

    const isDebug = !!config.debug;
    const mode = config.mode || 'publish';
    const enableTopicOverride = !!config.enableTopicOverride;
    const requestTimeout = config.requestTimeout || 5000;
    const requestFallbackToPublish = config.requestFallbackToPublish !== false;
    const enableAutoReply = !!config.enableAutoReply;

    const setStatusRed = () => node.status({ fill: 'red', shape: 'ring', text: 'disconnected' });
    const setStatusGreen = () => node.status({ fill: 'green', shape: 'dot', text: 'connected' });

    let connectionTimeout = null;
    let connectionStartTime = null;
    let statusRestoreTimer = null;

    setStatusRed();

    if (!config.server) {
      node.error('NATS server configuration not selected. Please select a NATS server node.');
      return;
    }

    this.config = RED.nodes.getNode(config.server);

    if (!this.config) {
      node.error('NATS server configuration not found. Please configure a NATS server node.');
      return;
    }

    const statusListener = (statusInfo) => {
      const status = statusInfo.status || statusInfo; // Backward compatibility
      switch (status) {
        case 'connected':
          if (connectionTimeout) {
            clearTimeout(connectionTimeout);
            connectionTimeout = null;
          }
          setStatusGreen();
          break;
        case 'disconnected':
          if (connectionTimeout) {
            clearTimeout(connectionTimeout);
            connectionTimeout = null;
          }
          node.status({ fill: 'red', shape: 'ring', text: `disconnected (${statusInfo.reconnectAttempts || 0})` });
          break;
        case 'connecting':
          node.status({ fill: 'yellow', shape: 'ring', text: `connecting (${statusInfo.reconnectAttempts || 0})` });
          connectionStartTime = Date.now();
          if (connectionTimeout) clearTimeout(connectionTimeout);
          connectionTimeout = setTimeout(() => {
            const elapsed = Math.floor((Date.now() - connectionStartTime) / 1000);
            node.warn(`NATS connection taking longer than expected (${elapsed}s). Check server availability.`);
          }, 10000);
          break;
        default:
        // Unknown status - ignore
      }
    };

    this.config.addStatusListener(statusListener);
    this.config.registerConnectionUser(node.id);

    // Resolve the subject to publish/reply on for the current mode
    function resolveSubject(msg) {
      if (mode === 'reply') {
        return msg._reply || msg._unsreply || null;
      }
      if (enableTopicOverride && msg.topic) {
        return msg.topic;
      }
      return config.datapointid || null;
    }

    // Encode msg.payload per the configured data format; undefined = unknown format
    function encodePayload(payload) {
      switch (config.dataformat) {
        case 'string':
          return String(payload);
        case 'buffer':
          if (Buffer.isBuffer(payload)) return payload;
          if (typeof payload === 'string') return Buffer.from(payload);
          return Buffer.from(JSON.stringify(payload));
        case 'json':
          return typeof payload === 'object' ? JSON.stringify(payload) : String(payload);
        default:
          return undefined;
      }
    }

    // Merge static config headers with dynamic msg.headers into a NATS MsgHdrs
    function buildHeaders(msg) {
      if (!config.enableHeaders) return undefined;
      const headersObj = {};
      if (config.headers && config.headers.trim() !== '') {
        try {
          Object.assign(headersObj, JSON.parse(config.headers));
        } catch (err) {
          node.warn(`[NATS-SUITE PUBLISH] Failed to parse static headers: ${err.message}`);
        }
      }
      if (msg.headers && typeof msg.headers === 'object') {
        Object.assign(headersObj, msg.headers);
      }
      if (Object.keys(headersObj).length === 0) return undefined;
      const msgHeaders = natsHeaders();
      Object.keys(headersObj).forEach((key) => msgHeaders.append(key, String(headersObj[key])));
      return msgHeaders;
    }

    // Briefly show a transient status, then fall back to 'connected'
    function restoreStatusSoon() {
      if (statusRestoreTimer) clearTimeout(statusRestoreTimer);
      statusRestoreTimer = setTimeout(() => {
        statusRestoreTimer = null;
        if (node.config.connectionStatus === 'connected') setStatusGreen();
      }, 3000);
    }

    // Request-Reply mode (client side): send a request and wait for the reply.
    // Returns null on success, or the error object to report via done(err).
    async function handleRequest(msg, send) {
      if (node.config.connectionStatus !== 'connected') {
        msg.error = { message: 'Cannot send request - NATS server is not connected', code: 'NOT_CONNECTED' };
        send(msg);
        return msg.error;
      }

      const subject = resolveSubject(msg);
      if (!subject) {
        msg.error = {
          message: 'No subject specified. Set subject in node config' + (enableTopicOverride ? ' or provide msg.topic' : ''),
          code: 'NO_SUBJECT',
        };
        send(msg);
        return msg.error;
      }

      const natsnc = await node.config.getConnection();
      const requestPayload = typeof msg.payload === 'object' ? JSON.stringify(msg.payload) : String(msg.payload);
      const startTime = Date.now();

      node.status({ fill: 'blue', shape: 'ring', text: `request: ${subject}` });

      try {
        const response = await natsnc.request(subject, sc.encode(requestPayload), { timeout: requestTimeout });
        const requestTime = Date.now() - startTime;
        let responsePayload;
        try {
          responsePayload = JSON.parse(sc.decode(response.data));
        } catch {
          responsePayload = sc.decode(response.data);
        }
        msg.payload = responsePayload;
        msg.requestTime = requestTime;
        msg.subject = response.subject;
        delete msg.error;
        node.status({ fill: 'green', shape: 'dot', text: `reply: ${requestTime}ms` });
        restoreStatusSoon();
        send(msg);
        return null;
      } catch (requestErr) {
        const isTimeout = requestErr.code === 'TIMEOUT' || (requestErr.message || '').includes('timeout');

        if (isTimeout && requestFallbackToPublish) {
          try {
            natsnc.publish(subject, sc.encode(requestPayload));
            msg.fallback = 'publish';
            msg.fallbackReason = 'request_timeout';
            delete msg.error;
            node.status({ fill: 'green', shape: 'dot', text: 'fallback publish' });
            restoreStatusSoon();
            send(msg);
            return null;
          } catch (publishErr) {
            node.warn(`[NATS-SUITE PUBLISH] Request timeout fallback failed: ${publishErr.message}`);
          }
        }

        msg.error = isTimeout
          ? { message: `Request timeout after ${requestTimeout}ms`, code: 'TIMEOUT' }
          : { message: requestErr.message || 'Request failed', code: requestErr.code || 'REQUEST_FAILED' };
        node.status({
          fill: isTimeout ? 'yellow' : 'red',
          shape: 'ring',
          text: isTimeout ? `timeout: ${requestTimeout}ms` : 'request failed',
        });
        restoreStatusSoon();
        send(msg);
        return msg.error;
      }
    }

    node.on('input', async function (msg, send, done) {
      try {
        // Request-Reply mode: full round trip handled separately, then stop
        if (mode === 'request' && !msg._requestReplyProcessed) {
          msg._requestReplyProcessed = true;
          const err = await handleRequest(msg, send);
          done(err);
          return;
        }

        // Auto-Reply (service side): forward to output; the real publish happens
        // when the processed response loops back through this same input
        if (enableAutoReply && !msg._autoReplyProcessed) {
          msg._autoReplyProcessed = true;
          if (isDebug) node.log('[NATS-SUITE PUBLISH] Auto-reply: forwarded to output, awaiting response message');
          send(msg);
          done();
          return;
        }

        if (node.config.connectionStatus !== 'connected') {
          done({
            message: 'Cannot publish - NATS server is not connected',
            code: 'NOT_CONNECTED',
            status: node.config.connectionStatus,
            reconnectAttempts: node.config.connectionStats.reconnectAttempts,
          });
          return;
        }

        const subject = resolveSubject(msg);
        if (!subject) {
          if (mode === 'reply') {
            node.warn('Reply mode: no reply subject (msg._reply or msg._unsreply) found. Cannot send reply.');
            done();
            return;
          }
          done(
            new Error(
              'No subject specified. Set subject in node config' + (enableTopicOverride ? ' or provide msg.topic' : '')
            )
          );
          return;
        }

        const encoded = encodePayload(msg.payload);
        if (encoded === undefined) {
          done(new Error(`Unknown data format: ${config.dataformat}. Use 'json', 'string', or 'buffer'`));
          return;
        }

        const natsnc = await node.config.getConnection();
        const publishOptions = {};
        const hdrs = buildHeaders(msg);
        if (hdrs) publishOptions.headers = hdrs;

        natsnc.publish(subject, Buffer.isBuffer(encoded) ? encoded : sc.encode(encoded), publishOptions);
        if (isDebug) node.log(`[NATS-SUITE PUBLISH] Published to ${subject}`);

        send(msg);
        done();
      } catch (err) {
        done(err);
      }
    });

    node.on('close', function (done) {
      // Both timers outlive the node otherwise, firing warn()/status() after close
      if (connectionTimeout) clearTimeout(connectionTimeout);
      if (statusRestoreTimer) clearTimeout(statusRestoreTimer);
      connectionTimeout = statusRestoreTimer = null;
      node.config.removeStatusListener(statusListener);
      node.config.unregisterConnectionUser(node.id);
      done();
    });
  }

  RED.nodes.registerType('nats-suite-publish', NatsPublishNode);
};
