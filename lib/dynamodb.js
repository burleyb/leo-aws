'use strict';
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, BatchGetCommand, BatchWriteCommand, GetCommand, PutCommand, UpdateCommand, ScanCommand, QueryCommand } = require("@aws-sdk/lib-dynamodb");

const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const https = require('https');
const merge = require('lodash.merge');
const chunk = require('../utils/chunker.js');
const async = require('async');
const backoff = require('backoff');
const ls = require('leo-streams');
const logger = require('leo-logger');

module.exports = function(configuration) {
    configuration = merge({
        convertEmptyValues: true,
        requestHandler: new NodeHttpHandler({
            connectionTimeout: 2000,
            requestTimeout: 5000,
            httpsAgent: new https.Agent({
                ciphers: 'ALL',
            })
        }),
        maxAttempts: 3,
    }, configuration.dynamodb || configuration || {});

    let docClient = DynamoDBDocumentClient.from(new DynamoDBClient(configuration), { marshallOptions: { convertEmptyValues: true, removeUndefinedValues: true } });

    return {
        _service: docClient,

        batchGetHashkey: async function(table, hashkey, ids, opts = {}) {
            const results = await this.batchGetTable(table, ids.map(id => ({ [hashkey]: id })), opts);
            return results.reduce((acc, row) => ({ ...acc, [row[hashkey]]: row }), {});
        },

        batchGetTable: async function(table, keys, opts = {}) {
            opts = merge({ chunk_size: 100, concurrency: 3 }, opts);
            const results = [];
            const uniqueKeys = Array.from(new Set(keys.map(key => JSON.stringify(key))));
            const batchedKeys = uniqueKeys.map(key => JSON.parse(key));

            await async.eachLimit(chunk(batchedKeys, opts.chunk_size), opts.concurrency, async (batch) => {
                const command = new BatchGetCommand({
                    RequestItems: { [table]: { Keys: batch } }
                });
                const data = await docClient.send(command);
                results.push(...(data.Responses[table] || []));
            });

            return results;
        },

        batchTableWrite: async function(table, records) {
            const command = new BatchWriteCommand({
                RequestItems: { [table]: records }
            });
            const data = await docClient.send(command);
            return data.UnprocessedItems ? data.UnprocessedItems[table] : [];
        },

        get: async function(table, id, opts = {}) {
            const key = typeof id === 'object' ? id : { [opts.id || 'id']: id };
            const command = new GetCommand({ TableName: table, Key: key, ConsistentRead: true });
            const data = await docClient.send(command);
            return data.Item || opts.default || null;
        },

        merge: async function(table, id, obj, opts = {}) {
            const existing = await this.get(table, id, opts);
            const mergedData = merge(existing, obj);
            await this.put(table, id, mergedData, opts);
            return mergedData;
        },

        put: async function(TableName, Key, Item, opts = {}) {
            const key = typeof Key === 'object' ? Key : { [opts.id || 'id']: Key };
            const command = new PutCommand({ TableName, Item, Key: key });
            await docClient.send(command);
            return true;
        },

        query: async function(params) {
            const command = new QueryCommand(params);
            const data = await docClient.send(command);
            return data.Items;
        },

        scan: async function(table) {
            const params = typeof table === 'string' ? { TableName: table } : table;
            const command = new ScanCommand(params);
            const data = await docClient.send(command);
            return data.Items;
        },

        smartQuery: async function query(params, config = {}, stats = {}) {
            const method = config.method === 'scan' ? 'scan' : 'query';
            const command = method === 'scan' ? new ScanCommand(params) : new QueryCommand(params);

            const data = await docClient.send(command);
            stats.count += data.Count;
            stats.mb++;
            config.progress(data, stats, (shouldContinue) => {
                if (shouldContinue && data.LastEvaluatedKey && stats.mb < config.mb && (config.count == null || stats.count < config.count)) {
                    params.ExclusiveStartKey = data.LastEvaluatedKey;
                    return this.smartQuery(params, config, stats);
                } else {
                    return data;
                }
            });
        },

        streamToTable: function(table, opts = {}) {
            let self = this;
            opts = merge({ records: 25, size: 1024 * 1024 * 2, time: { seconds: 2 } }, opts || {});
            let records;

            function reset() {
                records = [];
            }
            reset();

            const retry = backoff.fibonacci({ initialDelay: 100, maxDelay: 1000, randomisationFactor: 0 });
            retry.failAfter(10);

            retry.run = function(callback) {
                retry.once('fail', callback).once('success', reset);
                retry.backoff();
            };

            return ls.buffer(opts, (obj, done) => {
                records.push(obj);
                done(null, { records: 1, size: obj.gzipSize });
            }, retry.run, () => reset());
        },

        update: async function(table, key, set, opts = {}) {

            if(typeof table === 'object') {
                const command = new UpdateCommand(table);

                const data = await docClient.send(command);
                return opts.ReturnValues && opts.ReturnValues !== 'NONE' ? data : true;
            } else {
                const keyObj = typeof key === 'object' ? key : { 'id': key };
                const expression = Object.keys(set).map(k => `#${k} = :${k}`);
                const names = Object.fromEntries(Object.keys(set).map(k => [`#${k}`, k]));
                const values = Object.fromEntries(Object.keys(set).map(k => [`:${k}`, set[k]]));

                const command = new UpdateCommand({
                    TableName: table,
                    Key: keyObj,
                    UpdateExpression: `set ${expression.join(', ')}`,
                    ExpressionAttributeNames: names,
                    ExpressionAttributeValues: values,
                    ReturnValues: opts.ReturnValues || 'NONE'
                });

                const data = await docClient.send(command);
                return opts.ReturnValues && opts.ReturnValues !== 'NONE' ? data : true;
            }
        }
    };
};
