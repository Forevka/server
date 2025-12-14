/*
 * (c) Copyright? maybe to Forevka 
 *
 */

'use strict';

const config = require('config');
const logger = require('./../../Common/sources/logger');

// Read Redis configuration from config file (populated by Docker entrypoint)
const cfgRedis = config.get('services.CoAuthoring.redis');
const redisHost = cfgRedis.host;
const redisPort = cfgRedis.port;

// Check if Redis is configured (not localhost = external Redis server)
const isRedisConfigured = redisHost && redisHost !== 'localhost' && redisHost !== '127.0.0.1';

if (!isRedisConfigured) {
  logger.warn('editorDataRedis: Redis not configured (host=%s). Falling back to memory implementation.', redisHost || 'undefined');
  
  const editorDataMemory = require('./editorDataMemory');
  module.exports = editorDataMemory;
} else {
  // Redis implementation
  const ms = require('ms');
  const Redis = require('ioredis');
  const utils = require('./../../Common/sources/utils');
  const commonDefines = require('./../../Common/sources/commondefines');
  const tenantManager = require('./../../Common/sources/tenantManager');

  const cfgRedisPrefix = cfgRedis.prefix;
  const cfgRedisOptions = cfgRedis.iooptions;
  const cfgExpPresence = ms(config.get('services.CoAuthoring.expire.presence')) / 1000;
  const cfgExpMonthUniqueUsers = ms(config.get('services.CoAuthoring.expire.monthUniqueUsers'));

  /**
   * Build Redis connection options from config
   */
  function buildRedisOptions() {
    const options = {
      host: redisHost,
      port: parseInt(redisPort, 10) || 6379,
      ...cfgRedisOptions
    };

    // Get optional auth settings from config
    const redisOptions = cfgRedis.options || {};
    if (redisOptions.username) {
      options.username = redisOptions.username;
    }
    if (redisOptions.password) {
      options.password = redisOptions.password;
    }
    if (redisOptions.database !== undefined) {
      options.db = redisOptions.database;
    }

    return options;
  }

  /**
   * Get Redis key with prefix
   * @param {...string} parts - Key parts to join
   * @returns {string} Prefixed key
   */
  function getKey(...parts) {
    return cfgRedisPrefix + parts.join(':');
  }

  /**
   * Get document-specific key
   * @param {object} ctx - Context with tenant
   * @param {string} docId - Document ID
   * @param {string} suffix - Key suffix
   * @returns {string} Prefixed document key
   */
  function getDocKey(ctx, docId, suffix) {
    return getKey(ctx.tenant, 'doc', docId, suffix);
  }

  /**
   * Get tenant-specific key
   * @param {object} ctx - Context with tenant
   * @param {string} suffix - Key suffix
   * @returns {string} Prefixed tenant key
   */
  function getTenantKey(ctx, suffix) {
    return getKey(ctx.tenant, suffix);
  }

  // ============================================================================
  // EditorCommon - Base class with Redis connection management
  // ============================================================================

  function EditorCommon() {
    this.client = null;
  }

  EditorCommon.prototype.connect = async function () {
    if (!this.client) {
      const options = buildRedisOptions();
      this.client = new Redis(options);
      
      this.client.on('error', (err) => {
        logger.error('editorDataRedis: Redis connection error: %s', err.message);
      });
      
      this.client.on('connect', () => {
        logger.info('editorDataRedis: Connected to Redis at %s:%s', redisHost, redisPort);
      });

      // If lazyConnect is enabled, we need to explicitly connect
      if (options.lazyConnect) {
        await this.client.connect();
      }
    }
  };

  EditorCommon.prototype.isConnected = function () {
    return this.client && this.client.status === 'ready';
  };

  EditorCommon.prototype.ping = async function () {
    if (!this.client) {
      throw new Error('Redis client not initialized');
    }
    return await this.client.ping();
  };

  EditorCommon.prototype.close = async function () {
    if (this.client) {
      await this.client.quit();
      this.client = null;
    }
  };

  EditorCommon.prototype.healthCheck = async function () {
    try {
      if (this.isConnected()) {
        const result = await this.ping();
        return result === 'PONG';
      }
      return false;
    } catch (err) {
      logger.error('editorDataRedis: Health check failed: %s', err.message);
      return false;
    }
  };

  /**
   * Ensure Redis client is connected (lazy auto-connect)
   * Call this at the start of any method that uses this.client
   */
  EditorCommon.prototype.ensureConnected = async function () {
    if (!this.client) {
      await this.connect();
    }
  };

  /**
   * Check and acquire a lock using Redis SET NX EX
   * @param {object} ctx - Context
   * @param {string} name - Lock name
   * @param {string} docId - Document ID
   * @param {string} fencingToken - Token to identify lock owner
   * @param {number} ttl - Time to live in seconds
   * @returns {Promise<boolean>} True if lock acquired
   */
  EditorCommon.prototype._checkAndLock = async function (ctx, name, docId, fencingToken, ttl) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, name);
    
    // Try to get current lock value
    const current = await this.client.get(key);
    
    if (current !== null) {
      // Lock exists, check if it's ours
      if (current !== fencingToken) {
        return false;
      }
    }
    
    // Set or refresh the lock
    await this.client.set(key, fencingToken, 'EX', ttl);
    return true;
  };

  /**
   * Check and release a lock
   * @param {object} ctx - Context
   * @param {string} name - Lock name
   * @param {string} docId - Document ID
   * @param {string} fencingToken - Token to identify lock owner
   * @returns {Promise<number>} Unlock result code
   */
  EditorCommon.prototype._checkAndUnlock = async function (ctx, name, docId, fencingToken) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, name);
    
    const current = await this.client.get(key);
    
    if (current === null) {
      return commonDefines.c_oAscUnlockRes.Empty;
    }
    
    if (current === fencingToken) {
      await this.client.del(key);
      return commonDefines.c_oAscUnlockRes.Unlocked;
    }
    
    return commonDefines.c_oAscUnlockRes.Locked;
  };

  // ============================================================================
  // EditorData - Document-level data operations
  // ============================================================================

  function EditorData() {
    EditorCommon.call(this);
  }
  EditorData.prototype = Object.create(EditorCommon.prototype);
  EditorData.prototype.constructor = EditorData;

  // Presence methods - use Redis Hash for document presence
  EditorData.prototype.addPresence = async function (ctx, docId, odbc, userInfo) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'presence');
    await this.client.hset(key, odbc, JSON.stringify(userInfo));
    await this.client.expire(key, cfgExpPresence);
  };

  EditorData.prototype.updatePresence = async function (ctx, docId, odbc) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'presence');
    // Refresh TTL on the presence hash
    await this.client.expire(key, cfgExpPresence);
  };

  EditorData.prototype.removePresence = async function (ctx, docId, odbc) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'presence');
    await this.client.hdel(key, odbc);
  };

  EditorData.prototype.getPresence = async function (ctx, docId, connections) {
    // For memory mode compatibility, we check local connections
    const hvals = [];
    if (connections) {
      for (let i = 0; i < connections.length; ++i) {
        const conn = connections[i];
        if (conn.docId === docId && ctx.tenant === tenantManager.getTenantByConnection(ctx, conn)) {
          hvals.push(utils.getConnectionInfoStr(conn));
        }
      }
    }
    return hvals;
  };

  EditorData.prototype.lockSave = async function (ctx, docId, userId, ttl) {
    return this._checkAndLock(ctx, 'lockSave', docId, userId, ttl);
  };

  EditorData.prototype.unlockSave = async function (ctx, docId, userId) {
    return this._checkAndUnlock(ctx, 'lockSave', docId, userId);
  };

  EditorData.prototype.lockAuth = async function (ctx, docId, userId, ttl) {
    return this._checkAndLock(ctx, 'lockAuth', docId, userId, ttl);
  };

  EditorData.prototype.unlockAuth = async function (ctx, docId, userId) {
    return this._checkAndUnlock(ctx, 'lockAuth', docId, userId);
  };

  EditorData.prototype.getDocumentPresenceExpired = async function (_now) {
    // In clustered mode, presence expiry is handled by Redis TTL
    return [];
  };

  EditorData.prototype.removePresenceDocument = async function (ctx, docId) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'presence');
    await this.client.del(key);
  };

  // Lock methods - use Redis Hash for document locks
  EditorData.prototype.addLocks = async function (ctx, docId, locks) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'locks');
    const args = [];
    for (const lockId in locks) {
      if (Object.hasOwn(locks, lockId)) {
        args.push(lockId, JSON.stringify(locks[lockId]));
      }
    }
    if (args.length > 0) {
      await this.client.hset(key, ...args);
    }
  };

  EditorData.prototype.addLocksNX = async function (ctx, docId, locks) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'locks');
    const lockConflict = {};
    
    // Get all current locks first
    const currentLocks = await this.client.hgetall(key);
    const allLocks = {};
    
    // Parse existing locks
    for (const lockId in currentLocks) {
      if (Object.hasOwn(currentLocks, lockId)) {
        try {
          allLocks[lockId] = JSON.parse(currentLocks[lockId]);
        } catch (e) {
          allLocks[lockId] = currentLocks[lockId];
        }
      }
    }
    
    // Try to add new locks
    for (const lockId in locks) {
      if (Object.hasOwn(locks, lockId)) {
        if (allLocks[lockId] === undefined) {
          // Lock doesn't exist, add it
          const added = await this.client.hsetnx(key, lockId, JSON.stringify(locks[lockId]));
          if (added) {
            allLocks[lockId] = locks[lockId];
          } else {
            // Someone else added it concurrently
            lockConflict[lockId] = locks[lockId];
            // Fetch the current value
            const currentVal = await this.client.hget(key, lockId);
            if (currentVal) {
              try {
                allLocks[lockId] = JSON.parse(currentVal);
              } catch (e) {
                allLocks[lockId] = currentVal;
              }
            }
          }
        } else {
          lockConflict[lockId] = locks[lockId];
        }
      }
    }
    
    return {lockConflict, allLocks};
  };

  EditorData.prototype.removeLocks = async function (ctx, docId, locks) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'locks');
    const lockIds = Object.keys(locks);
    if (lockIds.length > 0) {
      await this.client.hdel(key, ...lockIds);
    }
  };

  EditorData.prototype.removeAllLocks = async function (ctx, docId) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'locks');
    await this.client.del(key);
  };

  EditorData.prototype.getLocks = async function (ctx, docId) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'locks');
    const rawLocks = await this.client.hgetall(key);
    const locks = {};
    
    for (const lockId in rawLocks) {
      if (Object.hasOwn(rawLocks, lockId)) {
        try {
          locks[lockId] = JSON.parse(rawLocks[lockId]);
        } catch (e) {
          locks[lockId] = rawLocks[lockId];
        }
      }
    }
    
    return locks;
  };

  // Message methods - use Redis List
  EditorData.prototype.addMessage = async function (ctx, docId, msg) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'messages');
    await this.client.rpush(key, JSON.stringify(msg));
  };

  EditorData.prototype.removeMessages = async function (ctx, docId) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'messages');
    await this.client.del(key);
  };

  EditorData.prototype.getMessages = async function (ctx, docId) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'messages');
    const rawMessages = await this.client.lrange(key, 0, -1);
    
    return rawMessages.map(msg => {
      try {
        return JSON.parse(msg);
      } catch (e) {
        return msg;
      }
    });
  };

  // Save state methods
  EditorData.prototype.setSaved = async function (ctx, docId, status) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'saved');
    await this.client.set(key, JSON.stringify(status));
  };

  EditorData.prototype.getdelSaved = async function (ctx, docId) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'saved');
    const value = await this.client.getdel(key);
    
    if (value === null) {
      return null;
    }
    
    try {
      return JSON.parse(value);
    } catch (e) {
      return value;
    }
  };

  // Force save methods - use Redis Hash
  EditorData.prototype.setForceSave = async function (ctx, docId, time, index, baseUrl, changeInfo, convertInfo) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'forceSave');
    const data = {
      time,
      index,
      baseUrl,
      changeInfo,
      started: false,
      ended: false,
      convertInfo
    };
    await this.client.set(key, JSON.stringify(data));
  };

  EditorData.prototype.getForceSave = async function (ctx, docId) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'forceSave');
    const value = await this.client.get(key);
    
    if (value === null) {
      return null;
    }
    
    try {
      return JSON.parse(value);
    } catch (e) {
      return null;
    }
  };

  EditorData.prototype.checkAndStartForceSave = async function (ctx, docId) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'forceSave');
    
    // Use WATCH for optimistic locking
    await this.client.watch(key);
    
    try {
      const value = await this.client.get(key);
      
      if (value === null) {
        await this.client.unwatch();
        return undefined;
      }
      
      let data;
      try {
        data = JSON.parse(value);
      } catch (e) {
        await this.client.unwatch();
        return undefined;
      }
      
      if (data.started) {
        await this.client.unwatch();
        return undefined;
      }
      
      data.started = true;
      data.ended = false;
      
      const multi = this.client.multi();
      multi.set(key, JSON.stringify(data));
      const result = await multi.exec();
      
      if (result === null) {
        // Transaction failed due to concurrent modification
        return undefined;
      }
      
      return data;
    } catch (e) {
      await this.client.unwatch();
      throw e;
    }
  };

  EditorData.prototype.checkAndSetForceSave = async function (ctx, docId, time, index, started, ended, convertInfo) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'forceSave');
    
    await this.client.watch(key);
    
    try {
      const value = await this.client.get(key);
      
      if (value === null) {
        await this.client.unwatch();
        return undefined;
      }
      
      let data;
      try {
        data = JSON.parse(value);
      } catch (e) {
        await this.client.unwatch();
        return undefined;
      }
      
      if (time !== data.time || index !== data.index) {
        await this.client.unwatch();
        return undefined;
      }
      
      data.started = started;
      data.ended = ended;
      data.convertInfo = convertInfo;
      
      const multi = this.client.multi();
      multi.set(key, JSON.stringify(data));
      const result = await multi.exec();
      
      if (result === null) {
        return undefined;
      }
      
      return data;
    } catch (e) {
      await this.client.unwatch();
      throw e;
    }
  };

  EditorData.prototype.removeForceSave = async function (ctx, docId) {
    await this.ensureConnected();
    const key = getDocKey(ctx, docId, 'forceSave');
    await this.client.del(key);
  };

  // Cleanup
  EditorData.prototype.cleanDocumentOnExit = async function (ctx, docId) {
    await this.ensureConnected();
    const keys = [
      getDocKey(ctx, docId, 'locks'),
      getDocKey(ctx, docId, 'messages'),
      getDocKey(ctx, docId, 'saved'),
      getDocKey(ctx, docId, 'forceSave'),
      getDocKey(ctx, docId, 'lockSave'),
      getDocKey(ctx, docId, 'lockAuth'),
      getDocKey(ctx, docId, 'presence')
    ];
    
    await this.client.del(...keys);
    
    // Remove from force save timer
    const timerKey = getTenantKey(ctx, 'forceSaveTimer');
    await this.client.zrem(timerKey, docId);
  };

  // Force save timer methods - use Redis Sorted Set
  EditorData.prototype.addForceSaveTimerNX = async function (ctx, docId, expireAt) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, 'forceSaveTimer');
    // ZADD NX - only add if not exists
    await this.client.zadd(key, 'NX', expireAt, docId);
  };

  EditorData.prototype.getForceSaveTimer = async function (now) {
    await this.ensureConnected();
    const res = [];
    
    // We need to scan all tenant timer keys
    // Pattern: {prefix}*:forceSaveTimer
    const pattern = cfgRedisPrefix + '*:forceSaveTimer';
    let cursor = '0';
    
    do {
      const [newCursor, keys] = await this.client.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
      cursor = newCursor;
      
      for (const key of keys) {
        // Extract tenant from key: {prefix}{tenant}:forceSaveTimer
        const keyWithoutPrefix = key.substring(cfgRedisPrefix.length);
        const tenant = keyWithoutPrefix.substring(0, keyWithoutPrefix.length - ':forceSaveTimer'.length);
        
        // Get and remove expired items
        const expired = await this.client.zrangebyscore(key, '-inf', now);
        
        for (const docId of expired) {
          res.push([tenant, docId]);
          await this.client.zrem(key, docId);
        }
      }
    } while (cursor !== '0');
    
    return res;
  };

  // ============================================================================
  // EditorStat - Statistics and license operations
  // ============================================================================

  function EditorStat() {
    EditorCommon.call(this);
  }
  EditorStat.prototype = Object.create(EditorCommon.prototype);
  EditorStat.prototype.constructor = EditorStat;

  // Unique user presence - use Redis Hash with TTL stored in value
  EditorStat.prototype.addPresenceUniqueUser = async function (ctx, odbc, expireAt, userInfo) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, 'uniqueUsers');
    const data = {expireAt, userInfo};
    await this.client.hset(key, odbc, JSON.stringify(data));
  };

  EditorStat.prototype.getPresenceUniqueUser = async function (ctx, nowUTC) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, 'uniqueUsers');
    const rawUsers = await this.client.hgetall(key);
    const res = [];
    const toDelete = [];
    
    for (const odbc in rawUsers) {
      if (Object.hasOwn(rawUsers, odbc)) {
        try {
          const elem = JSON.parse(rawUsers[odbc]);
          if (elem.expireAt > nowUTC) {
            const newElem = {userid: odbc, expire: new Date(elem.expireAt * 1000)};
            Object.assign(newElem, elem.userInfo);
            res.push(newElem);
          } else {
            toDelete.push(odbc);
          }
        } catch (e) {
          toDelete.push(odbc);
        }
      }
    }
    
    // Cleanup expired entries
    if (toDelete.length > 0) {
      await this.client.hdel(key, ...toDelete);
    }
    
    return res;
  };

  // Monthly unique users - use separate Hash per period
  EditorStat.prototype.addPresenceUniqueUsersOfMonth = async function (ctx, odbc, period, userInfo) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, `uniqueUsersOfMonth:${period}`);
    await this.client.hset(key, odbc, JSON.stringify(userInfo));
    // Set TTL on the hash
    const ttlSeconds = Math.ceil(cfgExpMonthUniqueUsers / 1000);
    await this.client.expire(key, ttlSeconds);
  };

  EditorStat.prototype.getPresenceUniqueUsersOfMonth = async function (ctx) {
    await this.ensureConnected();
    const res = {};
    
    // Scan for all monthly user keys for this tenant
    const pattern = getTenantKey(ctx, 'uniqueUsersOfMonth:*');
    let cursor = '0';
    
    do {
      const [newCursor, keys] = await this.client.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
      cursor = newCursor;
      
      for (const key of keys) {
        // Extract period from key
        const periodStr = key.substring(key.lastIndexOf(':') + 1);
        const period = parseInt(periodStr, 10);
        
        if (!isNaN(period)) {
          const rawUsers = await this.client.hgetall(key);
          const users = {};
          
          for (const odbc in rawUsers) {
            if (Object.hasOwn(rawUsers, odbc)) {
              try {
                users[odbc] = JSON.parse(rawUsers[odbc]);
              } catch (e) {
                users[odbc] = rawUsers[odbc];
              }
            }
          }
          
          const date = new Date(period).toISOString();
          res[date] = users;
        }
      }
    } while (cursor !== '0');
    
    return res;
  };

  // View user presence
  EditorStat.prototype.addPresenceUniqueViewUser = async function (ctx, odbc, expireAt, userInfo) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, 'uniqueViewUsers');
    const data = {expireAt, userInfo};
    await this.client.hset(key, odbc, JSON.stringify(data));
  };

  EditorStat.prototype.getPresenceUniqueViewUser = async function (ctx, nowUTC) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, 'uniqueViewUsers');
    const rawUsers = await this.client.hgetall(key);
    const res = [];
    const toDelete = [];
    
    for (const odbc in rawUsers) {
      if (Object.hasOwn(rawUsers, odbc)) {
        try {
          const elem = JSON.parse(rawUsers[odbc]);
          if (elem.expireAt > nowUTC) {
            const newElem = {userid: odbc, expire: new Date(elem.expireAt * 1000)};
            Object.assign(newElem, elem.userInfo);
            res.push(newElem);
          } else {
            toDelete.push(odbc);
          }
        } catch (e) {
          toDelete.push(odbc);
        }
      }
    }
    
    // Cleanup expired entries
    if (toDelete.length > 0) {
      await this.client.hdel(key, ...toDelete);
    }
    
    return res;
  };

  // Monthly view users
  EditorStat.prototype.addPresenceUniqueViewUsersOfMonth = async function (ctx, odbc, period, userInfo) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, `uniqueViewUsersOfMonth:${period}`);
    await this.client.hset(key, odbc, JSON.stringify(userInfo));
    const ttlSeconds = Math.ceil(cfgExpMonthUniqueUsers / 1000);
    await this.client.expire(key, ttlSeconds);
  };

  EditorStat.prototype.getPresenceUniqueViewUsersOfMonth = async function (ctx) {
    await this.ensureConnected();
    const res = {};
    
    const pattern = getTenantKey(ctx, 'uniqueViewUsersOfMonth:*');
    let cursor = '0';
    
    do {
      const [newCursor, keys] = await this.client.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
      cursor = newCursor;
      
      for (const key of keys) {
        const periodStr = key.substring(key.lastIndexOf(':') + 1);
        const period = parseInt(periodStr, 10);
        
        if (!isNaN(period)) {
          const rawUsers = await this.client.hgetall(key);
          const users = {};
          
          for (const odbc in rawUsers) {
            if (Object.hasOwn(rawUsers, odbc)) {
              try {
                users[odbc] = JSON.parse(rawUsers[odbc]);
              } catch (e) {
                users[odbc] = rawUsers[odbc];
              }
            }
          }
          
          const date = new Date(period).toISOString();
          res[date] = users;
        }
      }
    } while (cursor !== '0');
    
    return res;
  };

  // Editor connections statistics - use Sorted Set with time as score
  EditorStat.prototype.setEditorConnections = async function (ctx, countEdit, countLiveView, countView, now, precision) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, 'editorConnections');
    const data = JSON.stringify({time: now, edit: countEdit, liveview: countLiveView, view: countView});
    
    await this.client.zadd(key, now, data);
    
    // Remove old entries beyond max precision
    const maxAge = precision[precision.length - 1].val;
    const cutoff = now - maxAge;
    await this.client.zremrangebyscore(key, '-inf', cutoff);
  };

  EditorStat.prototype.getEditorConnections = async function (ctx) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, 'editorConnections');
    const rawData = await this.client.zrange(key, 0, -1);
    
    return rawData.map(item => {
      try {
        return JSON.parse(item);
      } catch (e) {
        return {time: 0, edit: 0, liveview: 0, view: 0};
      }
    });
  };

  // Connection count by shard - use Redis String with shard key
  EditorStat.prototype.setEditorConnectionsCountByShard = async function (ctx, shardId, count) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, `editorCount:${shardId}`);
    await this.client.set(key, count.toString());
  };

  EditorStat.prototype.incrEditorConnectionsCountByShard = async function (ctx, shardId, count) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, `editorCount:${shardId}`);
    await this.client.incrby(key, count);
  };

  EditorStat.prototype.getEditorConnectionsCount = async function (ctx, connections) {
    // For memory mode compatibility, we count from local connections
    let count = 0;
    if (connections) {
      for (let i = 0; i < connections.length; ++i) {
        const conn = connections[i];
        if (!(conn.isCloseCoAuthoring || (conn.user && conn.user.view)) && ctx.tenant === tenantManager.getTenantByConnection(ctx, conn)) {
          count++;
        }
      }
    }
    return count;
  };

  EditorStat.prototype.setViewerConnectionsCountByShard = async function (ctx, shardId, count) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, `viewerCount:${shardId}`);
    await this.client.set(key, count.toString());
  };

  EditorStat.prototype.incrViewerConnectionsCountByShard = async function (ctx, shardId, count) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, `viewerCount:${shardId}`);
    await this.client.incrby(key, count);
  };

  EditorStat.prototype.getViewerConnectionsCount = async function (ctx, connections) {
    let count = 0;
    if (connections) {
      for (let i = 0; i < connections.length; ++i) {
        const conn = connections[i];
        if (conn.isCloseCoAuthoring || (conn.user && conn.user.view && ctx.tenant === tenantManager.getTenantByConnection(ctx, conn))) {
          count++;
        }
      }
    }
    return count;
  };

  EditorStat.prototype.setLiveViewerConnectionsCountByShard = async function (ctx, shardId, count) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, `liveViewerCount:${shardId}`);
    await this.client.set(key, count.toString());
  };

  EditorStat.prototype.incrLiveViewerConnectionsCountByShard = async function (ctx, shardId, count) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, `liveViewerCount:${shardId}`);
    await this.client.incrby(key, count);
  };

  EditorStat.prototype.getLiveViewerConnectionsCount = async function (ctx, connections) {
    let count = 0;
    if (connections) {
      for (let i = 0; i < connections.length; ++i) {
        const conn = connections[i];
        if (utils.isLiveViewer(conn) && ctx.tenant === tenantManager.getTenantByConnection(ctx, conn)) {
          count++;
        }
      }
    }
    return count;
  };

  // Shutdown tracking - use Redis Set
  EditorStat.prototype.addShutdown = async function (key, docId) {
    await this.ensureConnected();
    const redisKey = getKey('shutdown', key);
    await this.client.sadd(redisKey, docId);
  };

  EditorStat.prototype.removeShutdown = async function (key, docId) {
    await this.ensureConnected();
    const redisKey = getKey('shutdown', key);
    await this.client.srem(redisKey, docId);
  };

  EditorStat.prototype.getShutdownCount = async function (key) {
    await this.ensureConnected();
    const redisKey = getKey('shutdown', key);
    return await this.client.scard(redisKey);
  };

  EditorStat.prototype.cleanupShutdown = async function (key) {
    await this.ensureConnected();
    const redisKey = getKey('shutdown', key);
    await this.client.del(redisKey);
  };

  // License methods - use Redis String
  EditorStat.prototype.setLicense = async function (key, val) {
    await this.ensureConnected();
    const redisKey = getKey('license', key);
    await this.client.set(redisKey, JSON.stringify(val));
  };

  EditorStat.prototype.getLicense = async function (key) {
    await this.ensureConnected();
    const redisKey = getKey('license', key);
    const value = await this.client.get(redisKey);
    
    if (value === null) {
      return null;
    }
    
    try {
      return JSON.parse(value);
    } catch (e) {
      return value;
    }
  };

  EditorStat.prototype.removeLicense = async function (key) {
    await this.ensureConnected();
    const redisKey = getKey('license', key);
    await this.client.del(redisKey);
  };

  // Notification lock - similar to _checkAndLock but with different key structure
  EditorStat.prototype.lockNotification = async function (ctx, notificationType, ttl) {
    await this.ensureConnected();
    const key = getTenantKey(ctx, `notification:${notificationType}`);
    
    // Use SET NX EX for atomic lock acquisition
    const result = await this.client.set(key, '1', 'EX', ttl, 'NX');
    return result === 'OK';
  };

  EditorStat.prototype.deleteKey = async function (key) {
    await this.ensureConnected();
    await this.client.del(key);
  };

  module.exports = {
    EditorData,
    EditorStat
  };
}

