'use strict';

const { Objm } = require('@nats-io/obj');
const { nanos, headers: natsHeaders } = require('@nats-io/nats-core');
const { resolveServer } = require('../lib/connect');

module.exports = function (RED) {
  function NatsObjectPutNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    this.serverConfig = resolveServer(RED, node, config);
    if (!this.serverConfig) return;

    // Bucket configuration - can use bucketConfig node OR direct settings
    this.bucket = config.bucket || '';
    this.bucketConfig = config.bucketConfig
      ? RED.nodes.getNode(config.bucketConfig)
      : null;

    // If bucketConfig node exists, use it; otherwise use direct settings
    if (this.bucketConfig) {
      this.bucket = this.bucketConfig.bucket;
      this.serverConfig = this.bucketConfig.serverConfig;
    } else {
      // Direct bucket settings
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
    // plain object - flatten it so msg.info stays JSON-serializable.
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

    // Helper: Get or create Object Store. Objm#create() is create-or-open
    // (a no-op if the bucket already exists), so it replaces the old
    // try-bare-open-then-create fallback.
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

    // Bucket Management Operations
    const performBucketOperation = async msg => {
      try {
        const nc = await node.serverConfig.getConnection();
        const objm = new Objm(nc);

        const operation = msg.operation || config.operation || 'put';
        const bucketName = msg.bucket || node.bucket || '';

        if (!bucketName && operation !== 'bucket-list') {
          node.error('Bucket name required for operation');
          return;
        }

        switch (operation) {
          case 'bucket-create': {
            const createOptions = {
              description: msg.description || node.description || undefined,
              max_bytes: msg.maxBytes
                ? parseInt(msg.maxBytes, 10)
                : node.maxBytes || undefined,
              ttl: msg.maxAge
                ? nanos(parseInt(msg.maxAge, 10) * 1000)
                : node.maxAge
                  ? nanos(node.maxAge * 1000)
                  : undefined,
              storage:
                (msg.storage || node.storage) === 'memory' ? 'memory' : 'file',
              replicas: msg.replicas
                ? parseInt(msg.replicas, 10)
                : node.replicas || 1,
              compression:
                msg.compression !== undefined
                  ? !!msg.compression
                  : node.compression,
            };

            Object.keys(createOptions).forEach(key => {
              if (createOptions[key] === undefined) delete createOptions[key];
            });

            await objm.create(bucketName, createOptions);
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
            const os = await objm.create(bucketName);
            const status = await os.status();
            const objects = await os.list();
            msg.payload = {
              operation: 'bucket-info',
              bucket: bucketName,
              size: status.size || 0,
              objects: objects.length,
              deleted: status.streamInfo.state.num_deleted || 0,
            };
            node.status({ fill: 'green', shape: 'dot', text: bucketName });
            break;
          }

          case 'bucket-delete': {
            const os = await objm.create(bucketName);
            await os.destroy();
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
            const statuses = [];
            for await (const status of objm.list()) {
              statuses.push(status);
            }
            const buckets = [];
            for (const status of statuses) {
              const os = await objm.open(status.bucket);
              buckets.push({
                name: status.bucket,
                objects: (await os.list()).length,
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

        // Check if this is a delete operation
        if (operation === 'delete') {
          // Determine object name
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
          await os.delete(objectName);

          if (isDebug) {
            node.log(`[OBJECT DELETE] Deleted: ${objectName}`);
          }

          msg.operation = 'DELETE';
          msg.objectName = objectName;
          msg.bucket = node.bucket;
          msg.success = true;

          node.send(msg);
          node.status({
            fill: 'green',
            shape: 'dot',
            text: `deleted: ${objectName}`,
          });
          return;
        }

        // Default: put operation
        // Determine object name
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

        // Get data from payload
        let data;
        if (config.dataFrom === 'payload') {
          data = msg.payload;
        } else if (config.dataFrom === 'buffer') {
          data = Buffer.isBuffer(msg.payload)
            ? msg.payload
            : Buffer.from(String(msg.payload));
        } else if (config.dataFrom === 'file') {
          const fs = require('fs');
          const filePath = msg.filePath || config.filePath;
          if (!filePath) {
            node.error('No file path specified', msg);
            return;
          }
          data = fs.readFileSync(filePath);
        }

        if (!data) {
          node.error('No data specified', msg);
          return;
        }

        // Convert to Buffer if needed
        if (!Buffer.isBuffer(data)) {
          if (typeof data === 'string') {
            data = Buffer.from(data, 'utf8');
          } else if (typeof data === 'object') {
            data = Buffer.from(JSON.stringify(data), 'utf8');
          } else {
            data = Buffer.from(String(data));
          }
        }

        // Prepare metadata
        const metadata = {};
        if (msg.metadata && typeof msg.metadata === 'object') {
          Object.assign(metadata, msg.metadata);
        }
        if (config.description) {
          metadata.description = config.description;
        }
        if (msg.contentType) {
          metadata['content-type'] = msg.contentType;
        } else if (config.contentType) {
          metadata['content-type'] = config.contentType;
        }

        // Upload object. putBlob() takes the fully-buffered bytes directly;
        // put() wants a ReadableStream, which we don't have here. headers
        // must be a MsgHdrs instance, not a plain object.
        let msgHeaders;
        const headerEntries = Object.entries(metadata).filter(
          ([k]) => k !== 'description'
        );
        if (headerEntries.length > 0) {
          msgHeaders = natsHeaders();
          headerEntries.forEach(([k, v]) => msgHeaders.set(k, String(v)));
        }

        const info = await os.putBlob(
          {
            name: objectName,
            description: metadata.description,
            headers: msgHeaders,
          },
          data
        );

        if (isDebug) {
          node.log(
            `[OBJECT PUT] Upload successful - Name: ${objectName}, Size: ${info.size}, Chunks: ${info.chunks}`
          );
        }

        msg.info = { ...info, headers: headersToObject(info.headers) };
        msg.operation = 'PUT';
        msg.objectName = objectName;
        msg.bucket = node.bucket;
        msg.size = info.size;
        msg.chunks = info.chunks;

        node.send(msg);
        node.status({
          fill: 'green',
          shape: 'dot',
          text: `put: ${objectName}`,
        });
      } catch (err) {
        node.error(`Object Store PUT failed: ${err.message}`, msg);
        msg.error = err.message;
        msg.operation = 'PUT';
        node.send(msg);
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
      }
    });

    // Cleanup
    node.on('close', function () {
      this.serverConfig.unregisterConnectionUser(node.id);
    });
  }

  RED.nodes.registerType('nats-suite-object-put', NatsObjectPutNode);
};
