const { connect, credsAuthenticator, nkeyAuthenticator, Events, DebugEvents } = require('nats');
const fs = require('fs');

module.exports = function (RED) {
  function NatsServerNode(n) {
    RED.nodes.createNode(this, n);
    this.server = n.server;

    // Get credentials from Node-RED credentials store (secure)
    this.authMethod = n.authMethod || 'userpass';
    this.user = this.credentials.user || '';
    this.pass = this.credentials.pass || '';
    this.token = this.credentials.token || '';
    this.jwt = this.credentials.jwt || '';
    this.nkeySeed = this.credentials.nkeySeed || '';

    // TLS Configuration
    this.enableTLS = !!n.enableTLS;
    this.tlsRejectUnauthorized = n.tlsRejectUnauthorized !== false; // Default true
    this.tlsCaFile = n.tlsCaFile || '';
    this.tlsCertFile = n.tlsCertFile || '';
    this.tlsKeyFile = n.tlsKeyFile || '';

    const isDebug = !!n.debug;

    if (isDebug) {
      this.log(`[NATS] Configuration loaded:`);
      this.log(`  - Auth Method: ${this.authMethod}`);
      this.log(`  - TLS Enabled: ${this.enableTLS}`);
      if (this.enableTLS) {
        this.log(`  - TLS CA File: ${this.tlsCaFile || 'none'}`);
        this.log(`  - TLS Cert File: ${this.tlsCertFile || 'none'}`);
        this.log(`  - TLS Key File: ${this.tlsKeyFile || 'none'}`);
        this.log(`  - Verify Certificate: ${this.tlsRejectUnauthorized}`);
      }
    }
    if (this.authMethod === 'nkey' && this.nkeySeed.length == 0) {
      this.error(`[NATS] Auth Method: ${this.authMethod} requires the nkeySeed to be set.`);
    }

    // Debug connection info
    if (isDebug) this.log(`[NATS] Connecting to: ${this.server}`);
    this.connection = null;
    this.connectionStatus = 'disconnected';
    this.listeners = new Set();

    // Connection Pool: Track which nodes are using this connection
    this.connectionUsers = new Set(); // Set of node IDs using this connection
    this.connectionRefCount = 0; // Reference counter

    // Define helper functions early
    this.getUptime = () => {
      if (!this.connectionStats.connectionStartTime) {
        return 0;
      }
      return Date.now() - this.connectionStats.connectionStartTime;
    };

    this.formatUptime = (ms) => {
      if (ms === 0 || ms < 1000) return '';

      const seconds = Math.floor(ms / 1000);
      const minutes = Math.floor(seconds / 60);
      const hours = Math.floor(minutes / 60);
      const days = Math.floor(hours / 24);

      if (days > 0) return `${days}d ${hours % 24}h ${minutes % 60}m`;
      if (hours > 0) return `${hours}h ${minutes % 60}m`;
      if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
      return `${seconds}s`;
    };

    // Define emitStatusChange early
    this.emitStatusChange = () => {
      const statusInfo = {
        status: this.connectionStatus,
        reconnectAttempts: this.connectionStats.reconnectAttempts,
        maxReconnectAttempts: this.connectionStats.maxReconnectAttempts,
        uptime: this.getUptime(),
        uptimeFormatted: this.formatUptime(this.getUptime()),
        lastConnected: this.connectionStats.lastConnected,
        lastDisconnected: this.connectionStats.lastDisconnected
      };

      // OPC UA-style status display
      let statusText;
      switch (this.connectionStatus) {
        case 'connected':
          statusText = 'connected';
          break;
        case 'connecting':
          statusText = `connecting (${this.connectionStats.reconnectAttempts})`;
          break;
        case 'disconnected':
          statusText = `disconnected (${this.connectionStats.reconnectAttempts})`;
          break;
        default:
          statusText = this.connectionStatus;
      }

      // Update node status like OPC UA
      this.status({
        fill: this.connectionStatus === 'connected' ? 'green' :
              this.connectionStatus === 'connecting' ? 'yellow' : 'red',
        shape: this.connectionStatus === 'connected' ? 'dot' : 'ring',
        text: statusText
      });

      this.listeners.forEach(listener => listener(statusInfo));
    };

    // Connection tracking
    this.connectionStats = {
      reconnectAttempts: 0,
      maxReconnectAttempts: n.maxReconnectAttempts || 10,
      lastConnected: null,
      lastDisconnected: null,
      totalUptime: 0,
      totalDowntime: 0,
      connectionStartTime: null
    };

    const servers = this.server.split(',');

    // Build Connection Options
    const ConnectionOptions = {
      servers: servers,
      reconnect: true, // Use the NATS client's native reconnect: backoff, jitter and server-pool
                        // rotation are all handled internally - do not reimplement any of it here.
      maxReconnectAttempts: -1, // Never give up once a connection has been established
      reconnectTimeWait: n.reconnectTimeWait || 1000,
      waitOnFirstConnect: false, // Fail fast on the *first* connect attempt so getConnection() can
                                  // retry on demand instead of blocking; native reconnect above only
                                  // governs recovery after that first connection succeeds.
      timeout: n.timeout || 10000, // 10 second timeout
      pingInterval: n.pingInterval || 30000, // 30 second ping interval
      maxPingOut: n.maxPingOut || 3, // Max ping outs before disconnect
    };

    // Authentication Configuration
    try {
      switch (this.authMethod) {
        case 'userpass':
          if (this.user) {
            ConnectionOptions.user = this.user;
            ConnectionOptions.pass = this.pass || '';
            if (isDebug) this.log(`[NATS] Using username/password authentication`);
          }
          break;

        case 'token':
          if (this.token) {
            ConnectionOptions.token = this.token;
            if (isDebug) this.log(`[NATS] Using token authentication`);
          }
          break;

        case 'jwt':
          if (this.jwt && this.nkeySeed) {
            // JWT requires both JWT and NKey seed
            ConnectionOptions.authenticator = credsAuthenticator(
              new TextEncoder().encode(this.jwt),
              new TextEncoder().encode(this.nkeySeed)
            );
            if (isDebug) this.log(`[NATS] Using JWT authentication`);
          } else if (this.jwt) {
            this.warn('[NATS] JWT authentication requires both JWT token and NKey seed. Missing NKey seed.');
          }
          break;

        case 'nkey':
          if (this.nkeySeed) {
            // NKey authentication
            ConnectionOptions.authenticator = nkeyAuthenticator(
              new TextEncoder().encode(this.nkeySeed)
            );
            if (isDebug) this.log(`[NATS] Using NKey authentication`);
          }
          break;

        case 'none':
        default:
          if (isDebug) this.log(`[NATS] No authentication configured`);
          break;
      }
    } catch (authErr) {
      this.error(`[NATS] Authentication configuration error: ${authErr.message}`);
      if (isDebug) this.log(`[NATS] Auth error stack: ${authErr.stack}`);
    }

    // TLS Configuration
    if (this.enableTLS) {
      ConnectionOptions.tls = {
        rejectUnauthorized: this.tlsRejectUnauthorized
      };

      try {
        // Load CA certificate if provided
        if (this.tlsCaFile && fs.existsSync(this.tlsCaFile)) {
          ConnectionOptions.tls.ca = fs.readFileSync(this.tlsCaFile);
          if (isDebug) this.log(`[NATS] Loaded CA certificate from: ${this.tlsCaFile}`);
        }

        // Load client certificate if provided (for mTLS)
        if (this.tlsCertFile && fs.existsSync(this.tlsCertFile)) {
          ConnectionOptions.tls.cert = fs.readFileSync(this.tlsCertFile);
          if (isDebug) this.log(`[NATS] Loaded client certificate from: ${this.tlsCertFile}`);
        }

        // Load client key if provided (for mTLS)
        if (this.tlsKeyFile && fs.existsSync(this.tlsKeyFile)) {
          ConnectionOptions.tls.key = fs.readFileSync(this.tlsKeyFile);
          if (isDebug) this.log(`[NATS] Loaded client key from: ${this.tlsKeyFile}`);
        }

        if (isDebug) {
          this.log(`[NATS] TLS enabled with:`);
          this.log(`  - Reject Unauthorized: ${this.tlsRejectUnauthorized}`);
          this.log(`  - CA Certificate: ${this.tlsCaFile ? 'loaded' : 'none'}`);
          this.log(`  - Client Certificate: ${this.tlsCertFile ? 'loaded' : 'none'}`);
          this.log(`  - Client Key: ${this.tlsKeyFile ? 'loaded' : 'none'}`);
        }
      } catch (tlsErr) {
        this.error(`[NATS] TLS configuration error: ${tlsErr.message}`);
        if (isDebug) this.log(`[NATS] TLS error stack: ${tlsErr.stack}`);
      }
    } else {
      if (isDebug) this.log(`[NATS] TLS is disabled for this connection`);
      ConnectionOptions.tls = null;
    }

    // Watch the connection's lifecycle events. Started exactly once below, right after the
    // connection object is created, since native reconnect keeps that object's identity stable
    // across every disconnect/reconnect cycle for as long as the connection lives.
    const watchConnectionStatus = async (nc) => {
      for await (const s of nc.status()) {
        switch (s.type) {
          case Events.Disconnect:
            if (isDebug) this.log(`[NATS] Connection disconnected`);
            this.connectionStatus = 'disconnected';
            this.connectionStats.lastDisconnected = Date.now();
            this.connectionStats.connectionStartTime = null;
            this.emitStatusChange();
            break;

          case DebugEvents.Reconnecting:
            if (isDebug) this.log(`[NATS] Reconnecting...`);
            this.connectionStatus = 'connecting';
            this.connectionStats.reconnectAttempts++;
            this.emitStatusChange();
            break;

          case Events.Reconnect:
            if (isDebug) this.log(`[NATS] Reconnected`);
            this.connectionStatus = 'connected';
            this.connectionStats.lastConnected = Date.now();
            this.connectionStats.connectionStartTime = Date.now();
            this.connectionStats.reconnectAttempts = 0;
            this.emitStatusChange();
            break;

          case Events.Error:
            // Async error reported by the server (e.g. a permissions violation) - surface it,
            // don't swallow it. Connection status is unaffected here; a disconnect/reconnecting
            // event will follow separately if the connection is actually impacted.
            this.error(`[NATS] Connection error: ${s.data}`);
            break;

          case Events.LDM:
            this.warn('[NATS] Server is entering Lame Duck Mode; it will disconnect clients soon.');
            break;

          case DebugEvents.StaleConnection:
            // Precedes an actual disconnect - treat it the same way so status reflects it early.
            if (isDebug) this.log(`[NATS] Stale connection detected, heading for a disconnect`);
            this.connectionStatus = 'disconnected';
            this.emitStatusChange();
            break;

          case DebugEvents.PingTimer:
            // Pure noise, fires constantly - ignore entirely.
            break;

          case Events.Update:
            if (isDebug) this.log(`[NATS] Cluster topology update: ${JSON.stringify(s.data)}`);
            break;

          default:
            if (isDebug) this.log(`[NATS] Unhandled connection status event: ${s.type}`);
        }
      }
    };

    const connectNats = async () => {
      try {
        this.connectionStatus = 'connecting';
        this.connectionStats.reconnectAttempts++;
        this.emitStatusChange();

        // Start connection timeout warning
        const connectionStartTime = Date.now();
        const connectionTimeout = setTimeout(() => {
          const elapsed = Math.floor((Date.now() - connectionStartTime) / 1000);
          if (isDebug) this.log(`[NATS] WARNING: Connection attempt taking longer than expected (${elapsed}s)`);
          this.warn(`NATS connection attempt taking longer than expected (${elapsed}s). Check server availability.`);
        }, 10000);

        // Log connection attempt with auth method and TLS info
        let authInfo = 'no authentication';
        switch (this.authMethod) {
          case 'userpass':
            authInfo = this.user ? `username/password (user: ***)` : 'no authentication';
            break;
          case 'token':
            authInfo = this.token ? 'token authentication (***)' : 'no authentication';
            break;
          case 'jwt':
            authInfo = this.jwt ? 'JWT authentication (***+***)' : 'no authentication';
            break;
          case 'nkey':
            authInfo = this.nkeySeed ? 'NKey authentication (***)' : 'no authentication';
            break;
        }

        const tlsInfo = this.enableTLS ? 'with TLS/SSL' : 'without TLS';

        if (isDebug) {
          this.log(`[NATS] Connection attempt ${this.connectionStats.reconnectAttempts}:`);
          this.log(`  - Servers: ${servers.join(', ')}`);
          this.log(`  - Auth: ${authInfo}`);
          this.log(`  - Security: ${tlsInfo}`);
          this.log(`  - Timeout: ${n.timeout || 10000}ms`);
        }

        try {
          this.connection = await connect(ConnectionOptions);
        } finally {
          clearTimeout(connectionTimeout);
        }
        if (isDebug) this.log(`[NATS] Connection established successfully!`);

        // Start the lifecycle watcher once for this connection object; it runs for as long as
        // the connection lives (native reconnect keeps it alive across drops).
        watchConnectionStatus(this.connection).catch(err => {
          if (isDebug) this.log(`[NATS] Status iterator ended: ${err.message}`);
        });

        this.connectionStatus = 'connected';
        this.connectionStats.lastConnected = Date.now();
        this.connectionStats.connectionStartTime = Date.now();
        this.connectionStats.reconnectAttempts = 0; // Reset counter on successful connection
        this.emitStatusChange();

        return this.connection;
      } catch (err) {
        if (isDebug) this.log(`[NATS] Connection failed with error: ${err.message}`);
        if (isDebug) this.log(`[NATS] Error details:`, {
          name: err.name,
          code: err.code,
          stack: err.stack?.split('\n')[0]
        });

        this.connectionStatus = 'disconnected';
        this.connectionStats.lastDisconnected = Date.now();
        this.emitStatusChange();
        throw err;
      }
    };

    // Set initial status
    this.emitStatusChange();

    // Single-flight connect. Every consumer node calls getConnection() at startup,
    // and the config node kicks off its own initial attempt, so without this a
    // not-yet-connected config node fans out into one connect() per caller. Only
    // the last would be assigned to this.connection; the rest leaked forever as
    // orphaned sockets (observed: 8 of 9 connections on the broker with subs=0).
    // The initial attempt below MUST go through here too, or it races the first
    // getConnection() and leaks one connection per config node on every startup.
    let pendingConnect = null;
    const connectOnce = () => {
      if (this.connection) return Promise.resolve(this.connection);
      if (!pendingConnect) {
        pendingConnect = connectNats().finally(() => {
          pendingConnect = null;
        });
      }
      return pendingConnect;
    };

    this.getConnection = async () => {
      if (this.connection) return this.connection;
      await connectOnce();
      return this.connection;
    };

    // Start the initial connection attempt. If it fails, the connection stays null and the
    // next getConnection() call retries it; once it succeeds, native reconnect (above) takes
    // over for the lifetime of the connection.
    connectOnce().catch(() => {}); // failure is already reflected via emitStatusChange()

    // Connection Pool: Register a node as user of this connection
    this.registerConnectionUser = (nodeId) => {
      if (!nodeId) {
        if (isDebug) this.log(`[NATS] Warning: registerConnectionUser called without nodeId`);
        return;
      }

      const wasNew = !this.connectionUsers.has(nodeId);
      this.connectionUsers.add(nodeId);
      this.connectionRefCount = this.connectionUsers.size;

      if (wasNew && isDebug) {
        this.log(`[NATS] Node ${nodeId} registered as connection user (total: ${this.connectionRefCount})`);
      }
    };

    // Connection Pool: Unregister a node as user of this connection
    let deferredCloseTimer = null;
    this.unregisterConnectionUser = (nodeId) => {
      if (!nodeId) {
        if (isDebug) this.log(`[NATS] Warning: unregisterConnectionUser called without nodeId`);
        return;
      }

      const hadUser = this.connectionUsers.has(nodeId);
      this.connectionUsers.delete(nodeId);
      this.connectionRefCount = this.connectionUsers.size;

      if (hadUser && isDebug) {
        this.log(`[NATS] Node ${nodeId} unregistered as connection user (remaining: ${this.connectionRefCount})`);
      }

      // Automatic cleanup: Close connection if no users left
      if (this.connectionRefCount === 0 && this.connection) {
        if (isDebug) this.log(`[NATS] No more connection users - scheduling connection cleanup in 30s`);

        if (deferredCloseTimer) {
          clearTimeout(deferredCloseTimer);
        }

        // Wait 30 seconds before closing (in case new nodes are deployed)
        deferredCloseTimer = setTimeout(() => {
          deferredCloseTimer = null;
          if (this.connectionRefCount === 0 && this.connection) {
            if (isDebug) this.log(`[NATS] Closing unused connection`);

            // Close connection
            this.connection.close();
            this.connection = null;
            this.connectionStatus = 'disconnected';
            this.emitStatusChange();
          }
        }, 30000);
      }
    };

    this.addStatusListener = listener => {
      this.listeners.add(listener);
      // Send current status to new listener (object form)
      listener({
        status: this.connectionStatus,
        reconnectAttempts: this.connectionStats.reconnectAttempts,
        maxReconnectAttempts: this.connectionStats.maxReconnectAttempts,
        uptime: this.getUptime(),
        uptimeFormatted: this.formatUptime(this.getUptime())
      });
    };

    this.removeStatusListener = listener => {
      this.listeners.delete(listener);
    };

    this.getConnectionStats = () => {
      return {
        ...this.connectionStats,
        uptime: this.getUptime(),
        uptimeFormatted: this.formatUptime(this.getUptime())
      };
    };

    this.on('close', async (done) => {
      if (isDebug) this.log(`[NATS] Node closing, cleaning up connections...`);

      if (deferredCloseTimer) {
        clearTimeout(deferredCloseTimer);
        deferredCloseTimer = null;
      }

      // Close NATS connection gracefully - drain flushes pending work before closing;
      // fall back to a hard close if drain itself fails.
      if (this.connection && !this.connection.isClosed()) {
        try {
          await this.connection.drain();
        } catch (err) {
          if (isDebug) this.log(`[NATS] Drain failed, forcing close: ${err.message}`);
          try {
            await this.connection.close();
          } catch (closeErr) {
            if (isDebug) this.log(`[NATS] Close failed: ${closeErr.message}`);
          }
        }
      }
      this.connection = null;
      this.connectionStatus = 'disconnected';

      // Clear status
      this.status({});
      done();
    });
  }
  RED.nodes.registerType('nats-suite-server', NatsServerNode, {
    credentials: {
      user: { type: "text" },
      pass: { type: "password" },
      token: { type: "password" },
      jwt: { type: "password" },
      nkeySeed: { type: "password" }
    }
  });
};
