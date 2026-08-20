'use strict';

const { Objm } = require('@nats-io/obj');
const { nanos } = require('@nats-io/nats-core');

module.exports = function (RED) {
  function NatsObjectGetNode(config) {
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
      this.description = config.description || '';
      this.maxAge = parseInt(config.maxAge) || 0;
      this.maxBytes = parseInt(config.maxBytes) || 0;
      this.storage = config.storage || 'file';
      this.replicas = parseInt(config.replicas) || 1;
      this.compression = !!config.compression;
    }

    let objectStore = null;
    const isDebug = config.debug || this.serverConfig.debug || false;

    // ObjectInfo.headers is a MsgHdrs instance (@nats-io/nats-core), not a
    // plain object - flatten it for msg.metadata.
    // ponytail: hdrs.get() returns only the last value per key, so a
    // multi-valued header collapses to one string - matches how the rest of
    // this codebase treats headers. Switch to hdrs.values(key) if a caller
    // ever needs every value.
    const headersToObject = hdrs => {
      if (!hdrs) return {};
      const obj = {};
      for (const key of hdrs.keys()) obj[key] = hdrs.get(key);
      return obj;
    };

    // Objm#create() is create-or-open (a no-op if the bucket already
    // exists), so it replaces the old try-bare-open-then-create fallback.
    const getObjectStore = async () => {
      if (objectStore) return objectStore;

      try {
        if (node.bucketConfig) {
          objectStore = await node.bucketConfig.getObjectStore();
          return objectStore;
        }

        const nc = await node.serverConfig.getConnection();
        const createOptions = {
          description: node.description || undefined,
          max_bytes: node.maxBytes || undefined,
          ttl: node.maxAge ? nanos(node.maxAge * 1000) : undefined,
          storage: node.storage === 'memory' ? 'memory' : 'file',
          replicas: node.replicas,
          compression: node.compression,
        };

        Object.keys(createOptions).forEach(key => {
          if (createOptions[key] === undefined) delete createOptions[key];
        });

        objectStore = await new Objm(nc).create(node.bucket, createOptions);
        return objectStore;
      } catch (err) {
        node.error(`Failed to get Object Store: ${err.message}`);
        throw err;
      }
    };

    this.serverConfig.registerConnectionUser(node.id);
    node.status({ fill: 'yellow', shape: 'ring', text: 'ready' });

    node.on('input', async function (msg) {
      try {
        // Check if this is a list operation
        const operation = msg.operation || config.operation || 'get';

        if (operation === 'list') {
          const os = await getObjectStore();
          const objects = [];

          // os.list() resolves to a plain array, not an async iterable.
          for (const obj of await os.list()) {
            objects.push({
              name: obj.name,
              size: obj.size,
              chunks: obj.chunks,
              mtime: obj.mtime,
              metadata: headersToObject(obj.headers),
            });
          }

          msg.payload = objects;
          msg.operation = 'LIST';
          msg.bucket = node.bucket;
          msg.count = objects.length;

          if (isDebug) {
            node.log(`[OBJECT LIST] Found ${objects.length} objects`);
          }

          node.send(msg);
          node.status({
            fill: 'green',
            shape: 'dot',
            text: `${objects.length} objects`,
          });
          return;
        }

        // Default: get operation
        let objectName;
        if (config.nameFrom === 'config') {
          objectName = config.objectName;
        } else if (config.nameFrom === 'msg') {
          objectName = msg.objectName || msg.name;
        } else if (config.nameFrom === 'topic') {
          objectName = msg.topic;
        }

        if (!objectName) {
          node.error('No object name specified', msg);
          node.status({ fill: 'red', shape: 'ring', text: 'no name' });
          return;
        }

        const os = await getObjectStore();
        const obj = await os.get(objectName);

        if (!obj) {
          node.error(`Object not found: ${objectName}`, msg);
          msg.error = 'Object not found';
          node.send(msg);
          return;
        }

        // Read data
        const chunks = [];
        for await (const chunk of obj.data) {
          chunks.push(chunk);
        }
        const data = Buffer.concat(chunks);

        // Set payload based on output format
        if (config.outputFormat === 'buffer') {
          msg.payload = data;
        } else if (config.outputFormat === 'string') {
          msg.payload = data.toString('utf8');
        } else if (config.outputFormat === 'json') {
          try {
            msg.payload = JSON.parse(data.toString('utf8'));
          } catch {
            msg.payload = data.toString('utf8');
          }
        } else {
          msg.payload = data;
        }

        msg.info = { ...obj.info, headers: headersToObject(obj.info.headers) };
        msg.operation = 'GET';
        msg.objectName = objectName;
        msg.bucket = node.bucket;
        msg.size = obj.info.size;
        msg.metadata = headersToObject(obj.info.headers);

        if (isDebug) {
          node.log(
            `[OBJECT GET] Retrieved - Name: ${objectName}, Size: ${obj.info.size}`
          );
        }

        node.send(msg);
        node.status({
          fill: 'green',
          shape: 'dot',
          text: `got: ${objectName}`,
        });
      } catch (err) {
        node.error(`Object Store GET failed: ${err.message}`, msg);
        msg.error = err.message;
        msg.operation = 'GET';
        node.send(msg);
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
      }
    });

    node.on('close', function () {
      this.serverConfig.removeConnectionUser(node.id);
    });
  }

  RED.nodes.registerType('nats-suite-object-get', NatsObjectGetNode);
};
