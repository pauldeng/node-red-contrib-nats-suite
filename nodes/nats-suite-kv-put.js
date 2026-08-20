'use strict';

const { Kvm } = require('@nats-io/kv');
const { JetStreamApiCodes, JetStreamApiError } = require('@nats-io/jetstream');

module.exports = function (RED) {
  function UnsKvPutNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Server configuration (required)
    if (!config.server) {
      node.error('NATS server configuration not selected');
      node.status({ fill: 'red', shape: 'ring', text: 'no server' });
      return;
    }

    this.serverConfig = RED.nodes.getNode(config.server);
    if (!this.serverConfig) {
      node.error('NATS server configuration not found');
      node.status({ fill: 'red', shape: 'ring', text: 'server not found' });
      return;
    }

    // Bucket configuration - can use bucketConfig node OR direct settings
    this.bucket = config.bucket || '';
    this.bucketConfig = config.bucketConfig
      ? RED.nodes.getNode(config.bucketConfig)
      : null;

    if (this.bucketConfig) {
      this.bucket = this.bucketConfig.bucket;
      this.serverConfig = this.bucketConfig.serverConfig;
    } else {
      // Direct bucket settings
      this.description = config.description || '';
      this.history = parseInt(config.history) || 10;
      this.maxAge = parseInt(config.maxAge) || 0;
      this.maxBytes = parseInt(config.maxBytes) || 0;
      this.maxValueSize = parseInt(config.maxValueSize) || 0;
      this.compression = !!config.compression;
      this.replicas = parseInt(config.replicas) || 1;
      this.storage = config.storage || 'file';
    }

    let kvStore = null;
    const isDebug = config.debug || this.serverConfig.debug || false;

    // Helper: Get or create KV bucket. Kvm#create() is create-or-open (a
    // no-op if the bucket already exists), so it replaces the old
    // try-bare-open-then-create-with-options fallback outright.
    const getKVBucket = async () => {
      if (kvStore) return kvStore;

      try {
        if (node.bucketConfig) {
          kvStore = await node.bucketConfig.getKVBucket();
          return kvStore;
        }

        const nc = await node.serverConfig.getConnection();
        const createOptions = {
          history: node.history,
          ttl: node.maxAge * 1000,
          max_bytes: node.maxBytes || undefined,
          maxValueSize: node.maxValueSize || undefined,
          compression: node.compression,
          replicas: node.replicas,
          storage: node.storage === 'memory' ? 'memory' : 'file',
        };

        Object.keys(createOptions).forEach(key => {
          if (createOptions[key] === undefined) delete createOptions[key];
        });

        kvStore = await new Kvm(nc).create(node.bucket, createOptions);
        return kvStore;
      } catch (err) {
        node.error(`Failed to get KV bucket: ${err.message}`);
        throw err;
      }
    };

    // Helper: Stringify value if needed
    const prepareValue = value => {
      if (config.stringifyJSON && typeof value === 'object') {
        return JSON.stringify(value);
      }
      return String(value);
    };

    // Bucket Management Operations
    const performBucketOperation = async msg => {
      try {
        const nc = await node.serverConfig.getConnection();
        const kvm = new Kvm(nc);

        const operation = msg.operation || config.operation || 'put';
        const bucketName = msg.bucket || node.bucket || '';

        if (!bucketName && operation !== 'bucket-list') {
          node.error('Bucket name required for operation');
          return;
        }

        switch (operation) {
          case 'bucket-create': {
            const createOptions = {
              history: msg.history
                ? parseInt(msg.history, 10)
                : node.history || 10,
              ttl: msg.maxAge
                ? parseInt(msg.maxAge, 10) * 1000
                : node.maxAge
                  ? node.maxAge * 1000
                  : undefined,
              max_bytes: msg.maxBytes
                ? parseInt(msg.maxBytes, 10)
                : node.maxBytes || undefined,
              maxValueSize: msg.maxValueSize
                ? parseInt(msg.maxValueSize, 10)
                : node.maxValueSize || undefined,
              compression:
                msg.compression !== undefined
                  ? !!msg.compression
                  : node.compression,
              replicas: msg.replicas
                ? parseInt(msg.replicas, 10)
                : node.replicas || 1,
              storage:
                msg.storage || node.storage === 'memory' ? 'memory' : 'file',
            };

            Object.keys(createOptions).forEach(key => {
              if (createOptions[key] === undefined) delete createOptions[key];
            });

            await kvm.create(bucketName, createOptions);
            msg.payload = {
              operation: 'bucket-create',
              bucket: bucketName,
              success: true,
            };
            node.status({
              fill: 'green',
              shape: 'dot',
              text: `created: ${bucketName}`,
            });
            break;
          }

          case 'bucket-info': {
            const kv = await kvm.create(bucketName);
            const status = await kv.status();
            msg.payload = {
              operation: 'bucket-info',
              bucket: bucketName,
              values: status.values || 0,
              bytes: status.size || 0,
            };
            node.status({ fill: 'green', shape: 'dot', text: bucketName });
            break;
          }

          case 'bucket-delete': {
            const kv = await kvm.create(bucketName);
            await kv.destroy();
            msg.payload = {
              operation: 'bucket-delete',
              bucket: bucketName,
              success: true,
            };
            node.status({
              fill: 'green',
              shape: 'dot',
              text: `deleted: ${bucketName}`,
            });
            break;
          }

          case 'bucket-list': {
            const buckets = [];
            for await (const status of kvm.list()) {
              buckets.push({
                name: status.bucket,
                values: status.values || 0,
                bytes: status.size || 0,
              });
            }
            msg.payload = buckets;
            msg.operation = 'bucket-list';
            msg.count = buckets.length;
            node.status({
              fill: 'green',
              shape: 'dot',
              text: `${buckets.length} buckets`,
            });
            break;
          }

          default:
            node.error(`Unknown bucket operation: ${operation}`);
            return;
        }

        node.send(msg);
      } catch (err) {
        node.error(`Bucket operation failed: ${err.message}`, msg);
        msg.error = err.message;
        node.send(msg);
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
      }
    };

    // Register with connection pool
    this.serverConfig.registerConnectionUser(node.id);

    node.status({ fill: 'yellow', shape: 'ring', text: 'ready' });

    // Input handler
    node.on('input', async function (msg) {
      try {
        // Check if this is a bucket management operation
        const operation = msg.operation || config.operation || 'put';

        if (
          [
            'bucket-create',
            'bucket-info',
            'bucket-delete',
            'bucket-list',
          ].includes(operation)
        ) {
          await performBucketOperation(msg);
          return;
        }

        // Determine key
        let key;
        if (config.keyFrom === 'config') {
          key = config.key;
        } else if (config.keyFrom === 'msg') {
          key = msg.key;
        } else if (config.keyFrom === 'topic') {
          key = msg.topic;
        }

        if (!key) {
          node.error('No key specified', msg);
          node.status({ fill: 'red', shape: 'ring', text: 'no key' });
          return;
        }

        const kv = await getKVBucket();

        // Debug logging for operation
        if (isDebug) {
          node.log(
            `[KV PUT] Operation: ${operation.toUpperCase()}, Key: ${key}, Bucket: ${node.bucket}`
          );
        }

        // Perform operation - use msg.operation if provided, otherwise config.operation
        const op = operation;
        switch (op) {
          case 'put': {
            // Determine value
            let value;
            if (config.valueFrom === 'payload') {
              value = msg.payload;
            } else if (config.valueFrom === 'config') {
              value = config.value;
            } else if (config.valueFrom === 'msg') {
              value = msg.value;
            }

            if (value === undefined || value === null) {
              node.error('No value specified', msg);
              return;
            }

            const preparedValue = prepareValue(value);

            // Note: TTL is only supported at bucket level, not per-key
            // Individual key TTL is not supported by NATS KV Store
            const revision = await kv.put(key, preparedValue);

            if (isDebug) {
              node.log(
                `[KV PUT] PUT successful - Key: ${key}, Revision: ${revision}`
              );
            }

            msg.revision = revision;
            msg.operation = 'PUT';
            msg.key = key;
            msg.bucket = node.bucket;

            node.send(msg);
            node.status({ fill: 'green', shape: 'dot', text: `put: ${key}` });
            break;
          }

          case 'create': {
            // Create only if key doesn't exist
            let value;
            if (config.valueFrom === 'payload') {
              value = msg.payload;
            } else if (config.valueFrom === 'config') {
              value = config.value;
            } else if (config.valueFrom === 'msg') {
              value = msg.value;
            }

            if (value === undefined || value === null) {
              node.error('No value specified', msg);
              return;
            }

            const preparedValue = prepareValue(value);

            // Note: TTL is only supported at bucket level, not per-key
            // Individual key TTL is not supported by NATS KV Store
            try {
              const revision = await kv.create(key, preparedValue);

              if (isDebug) {
                node.log(
                  `[KV PUT] CREATE successful - Key: ${key}, Revision: ${revision}`
                );
              }

              msg.revision = revision;
              msg.operation = 'PUT';
              msg.key = key;
              msg.bucket = node.bucket;
              msg._created = true;

              node.send(msg);
              node.status({
                fill: 'green',
                shape: 'dot',
                text: `created: ${key}`,
              });
            } catch (err) {
              if (
                err instanceof JetStreamApiError &&
                [
                  JetStreamApiCodes.StreamWrongLastSequence,
                  JetStreamApiCodes.StreamWrongLastSequenceUnknown,
                ].includes(err.code)
              ) {
                node.error(`Key already exists: ${key}`, msg);
                node.status({
                  fill: 'red',
                  shape: 'ring',
                  text: 'already exists',
                });
              } else {
                throw err;
              }
            }
            break;
          }

          case 'update': {
            // Update only if key exists
            let value;
            if (config.valueFrom === 'payload') {
              value = msg.payload;
            } else if (config.valueFrom === 'config') {
              value = config.value;
            } else if (config.valueFrom === 'msg') {
              value = msg.value;
            }

            if (value === undefined || value === null) {
              node.error('No value specified', msg);
              return;
            }

            const preparedValue = prepareValue(value);

            // Get current revision first
            const current = await kv.get(key);
            if (!current) {
              node.error(`Key does not exist: ${key}`, msg);
              node.status({ fill: 'red', shape: 'ring', text: 'not found' });
              return;
            }

            // Note: TTL is only supported at bucket level, not per-key
            // Individual key TTL is not supported by NATS KV Store
            const revision = await kv.update(
              key,
              preparedValue,
              current.revision
            );

            if (isDebug) {
              node.log(
                `[KV PUT] UPDATE successful - Key: ${key}, Revision: ${revision}`
              );
            }

            msg.revision = revision;
            msg.operation = 'PUT';
            msg.key = key;
            msg.bucket = node.bucket;
            msg._updated = true;

            node.send(msg);
            node.status({
              fill: 'green',
              shape: 'dot',
              text: `updated: ${key}`,
            });
            break;
          }

          case 'delete': {
            // Soft delete (mark as deleted)
            await kv.delete(key);

            if (isDebug) {
              node.log(`[KV PUT] DELETE successful - Key: ${key}`);
            }

            msg.operation = 'DEL';
            msg.key = key;
            msg.bucket = node.bucket;
            msg._deleted = true;

            node.send(msg);
            node.status({
              fill: 'blue',
              shape: 'dot',
              text: `deleted: ${key}`,
            });
            break;
          }

          case 'purge': {
            // Hard delete (remove all revisions)
            await kv.purge(key);

            if (isDebug) {
              node.log(`[KV PUT] PURGE successful - Key: ${key}`);
            }

            msg.operation = 'PURGE';
            msg.key = key;
            msg.bucket = node.bucket;
            msg._purged = true;

            node.send(msg);
            node.status({ fill: 'blue', shape: 'dot', text: `purged: ${key}` });
            break;
          }

          default:
            node.error(`Unknown operation: ${op}`, msg);
            return;
        }

        // Reset status after 1 second
        setTimeout(() => {
          node.status({ fill: 'yellow', shape: 'ring', text: 'ready' });
        }, 1000);
      } catch (err) {
        node.error(`KV PUT error: ${err.message}`, msg);
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
      }
    });

    // Cleanup on close
    node.on('close', function () {
      this.serverConfig.unregisterConnectionUser(node.id);
      kvStore = null;
      node.status({});
    });
  }

  RED.nodes.registerType('nats-suite-kv-put', UnsKvPutNode);
};
