'use strict';

const { Kvm } = require('@nats-io/kv');

module.exports = function (RED) {
  function UnsKvGetNode(config) {
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
    let watcher = null;
    let isWatching = false;
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

    // Helper: Parse value if JSON
    const parseValue = value => {
      if (!config.parseJSON) return value;

      if (typeof value === 'string') {
        try {
          return JSON.parse(value);
        } catch {
          // Not JSON, return as-is
          return value;
        }
      }
      return value;
    };

    // Helper: Get single key
    const getValue = async key => {
      const kv = await getKVBucket();
      const entry = await kv.get(key);

      if (!entry) {
        return null;
      }

      const value = entry.string();
      const parsedValue = parseValue(value);

      const result = {
        payload: parsedValue,
        key: key,
        operation: 'GET',
        revision: entry.revision,
        created: entry.created ? new Date(entry.created).toISOString() : null,
        bucket: node.bucket,
      };

      // Include history if requested
      if (config.includeHistory) {
        try {
          const limit = parseInt(config.historyLimit) || 1;

          if (isDebug) {
            node.log(
              `[KV GET] History - config.historyLimit: ${JSON.stringify(config.historyLimit)} (type: ${typeof config.historyLimit}), parsed limit: ${limit}`
            );
          }

          const history = await kv.history({ key });
          const historyArray = [];

          for await (const h of history) {
            if (isDebug) {
              node.log(
                `[KV GET] History entry #${historyArray.length + 1} - revision: ${h.revision}, operation: ${h.operation}, key: ${h.key}`
              );
            }
            historyArray.push({
              revision: h.revision,
              value: parseValue(h.string()),
              created: h.created ? new Date(h.created).toISOString() : null,
              operation: h.operation,
            });
            if (limit > 0 && historyArray.length >= limit) {
              break;
            }
          }

          if (isDebug) {
            node.log(
              `[KV GET] History collected ${historyArray.length} entries (limit was ${limit})`
            );
          }

          result._history = historyArray;
        } catch (histErr) {
          node.warn(`Failed to get history: ${histErr.message}`);
        }
      }

      return result;
    };

    // Helper: List all keys
    const listKeys = async () => {
      const kv = await getKVBucket();
      const keys = await kv.keys();
      const keyArray = [];

      for await (const key of keys) {
        keyArray.push(key);
      }

      return {
        payload: keyArray,
        operation: 'LIST',
        bucket: node.bucket,
        count: keyArray.length,
      };
    };

    // Helper: Start watching keys
    const startWatch = async () => {
      if (isWatching) return;

      try {
        const kv = await getKVBucket();

        // Determine watch options
        const watchOptions = {};
        if (config.watchPattern) {
          watchOptions.key = config.watchPattern;
        }

        if (config.ignoreDeletes) {
          watchOptions.ignoreDeletes = true;
        }

        watcher = await kv.watch(watchOptions);
        isWatching = true;

        node.status({ fill: 'green', shape: 'dot', text: 'watching' });
        if (isDebug)
          node.log(`[KV GET] Started watching: ${config.watchPattern || '*'}`);

        // Process watch events
        (async () => {
          try {
            for await (const entry of watcher) {
              if (!isWatching) break;

              const value = entry.value ? entry.string() : null;
              const parsedValue = value !== null ? parseValue(value) : null;

              const msg = {
                payload: parsedValue,
                key: entry.key,
                operation: entry.operation,
                revision: entry.revision,
                created: entry.created
                  ? new Date(entry.created).toISOString()
                  : null,
                bucket: node.bucket,
                _watchEvent: true,
              };

              node.send(msg);

              node.status({
                fill: 'blue',
                shape: 'dot',
                text: `${entry.operation}: ${entry.key}`,
              });

              // Reset status after 1 second
              setTimeout(() => {
                if (isWatching) {
                  node.status({
                    fill: 'green',
                    shape: 'dot',
                    text: 'watching',
                  });
                }
              }, 1000);
            }
          } catch (err) {
            if (isWatching) {
              node.error(`Watch error: ${err.message}`);
              node.status({ fill: 'red', shape: 'ring', text: 'watch error' });
            }
          }
        })();
      } catch (err) {
        node.error(`Failed to start watch: ${err.message}`);
        node.status({ fill: 'red', shape: 'ring', text: 'watch failed' });
      }
    };

    // Helper: Stop watching
    const stopWatch = async () => {
      if (watcher) {
        try {
          await watcher.stop();
          if (isDebug) node.log(`[KV GET] Stopped watching`);
        } catch (err) {
          node.warn(`Error stopping watch: ${err.message}`);
        }
        watcher = null;
      }
      isWatching = false;
    };

    // Register with connection pool
    this.serverConfig.registerConnectionUser(node.id);

    // Initialize based on mode
    if (config.mode === 'watch') {
      // Start watching immediately
      startWatch();
    } else {
      node.status({ fill: 'yellow', shape: 'ring', text: 'ready' });
    }

    // Input handler
    node.on('input', async function (msg) {
      try {
        // Determine operation mode - msg.operation takes precedence
        const mode = msg.operation || config.mode;

        if (mode === 'get') {
          // Determine key
          let key;
          if (config.keyFrom === 'config') {
            key = config.key;
          } else if (config.keyFrom === 'msg') {
            key = msg.key;
          } else if (config.keyFrom === 'payload') {
            key = msg.payload;
          } else if (config.keyFrom === 'topic') {
            key = msg.topic;
          }

          if (!key) {
            node.error('No key specified', msg);
            return;
          }

          if (isDebug) {
            node.log(
              `[KV GET] GET operation - Key: ${key}, Bucket: ${node.bucket}`
            );
          }

          const result = await getValue(key);

          if (result === null) {
            if (isDebug) {
              node.log(`[KV GET] Key not found: ${key}`);
            }
            msg.payload = null;
            msg.key = key;
            msg.operation = 'GET';
            msg._notFound = true;
            msg.bucket = node.bucket;
            node.send(msg);
            node.status({ fill: 'grey', shape: 'ring', text: 'not found' });
          } else {
            if (isDebug) {
              node.log(
                `[KV GET] GET successful - Key: ${key}, Revision: ${result.revision || 'unknown'}`
              );
            }
            // Merge result into msg
            Object.assign(msg, result);
            node.send(msg);
            node.status({ fill: 'green', shape: 'dot', text: 'retrieved' });
          }

          // Reset status
          setTimeout(() => {
            node.status({ fill: 'yellow', shape: 'ring', text: 'ready' });
          }, 1000);
        } else if (mode === 'keys' || mode === 'list') {
          if (isDebug) {
            node.log(`[KV GET] LIST operation - Bucket: ${node.bucket}`);
          }

          const result = await listKeys();

          if (isDebug) {
            node.log(`[KV GET] LIST successful - Found ${result.count} keys`);
          }

          Object.assign(msg, result);
          node.send(msg);
          node.status({
            fill: 'green',
            shape: 'dot',
            text: `${result.count} keys`,
          });

          setTimeout(() => {
            node.status({ fill: 'yellow', shape: 'ring', text: 'ready' });
          }, 1000);
        } else if (mode === 'watch') {
          // Watch mode doesn't use input, but we can trigger a re-watch
          if (!isWatching) {
            await startWatch();
          }
        } else {
          node.error(`Unknown operation: ${mode}`, msg);
          return;
        }
      } catch (err) {
        node.error(`KV GET error: ${err.message}`, msg);
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
      }
    });

    // Cleanup on close
    node.on('close', async function () {
      await stopWatch();
      this.serverConfig.unregisterConnectionUser(node.id);
      kvStore = null;
      node.status({});
    });
  }

  RED.nodes.registerType('nats-suite-kv-get', UnsKvGetNode);
};
