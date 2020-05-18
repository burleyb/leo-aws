'use strict';
const AWS = require('aws-sdk');
const https = require('https');
const merge = require('lodash.merge');
const chunk = require('../utils/chunker.js');
const async = require('async');
const backoff = require('backoff');
const ls = require('leo-streams');
const logger = require('leo-logger');

/**
 *
 * @param configuration
 * @returns {{_service: AWS.DynamoDB.DocumentClient, get: get, put: put, update: update, scan: scan, query: query, smartQuery: query, batchGetTable: batchGetTable, batchGetHashkey: batchGetHashkey, writeToTableInChunks: writeToTableInChunks}}
 */
module.exports = function (configuration) {
	configuration = merge({
		convertEmptyValues: true,
		httpOptions: {
			agent: new https.Agent({
				ciphers: 'ALL',
				secureProtocol: 'TLSv1_method',
				// keepAlive: true
			}),
			connectTimeout: 2000,
			timeout: 5000,
		},
		maxRetries: 2,
	}, configuration || {});

	let docClient = new AWS.DynamoDB.DocumentClient(configuration);
	return {
		_service: docClient,
		batchGetHashkey: function (table, hashkey, ids, opts = {}) {
			return this.batchGetTable(table, ids.map((e) => {
				let ret = {};
				ret[hashkey] = e;
				return ret;
			}), opts).then(results => {
				let result = {};
				for (let i = 0; i < results.length; i++) {
					let row = results[i];
					result[row[hashkey]] = row;
				}
				return result;
			});
		},
		batchGetTable: (table, keys, opts = {}) => {
			return new Promise((resolve, reject) => {
				opts = merge({
					chunk_size: 100,
					concurrency: 3
				}, opts);
				let uniquemap = {};
				let results = [];
				let chunker = chunk(function (items, done) {
					if (items.length > 0) {
						let params = {
							RequestItems: {},
							ReturnConsumedCapacity: 'TOTAL',
						};
						params.RequestItems[table] = {
							Keys: items
						};
						docClient.batchGet(params, function (err, data) {
							if (err) {
								logger.error(err);
								done(err, items);
							} else {
								results = results.concat(data.Responses[table]);
								done(null, []);
							}
						});
					} else {
						done(null, []);
					}
				}, opts);

				for (let i = 0; i < keys.length; i++) {
					let identifier = JSON.stringify(keys[i]);
					if (!(identifier in uniquemap)) {
						uniquemap[identifier] = 1;
						chunker.add(keys[i]);
					}
				}

				chunker.end((err, rs) => {
					if (err) {
						reject(err);
					} else {
						resolve(results);
					}
				});
			});
		},
		batchTableWrite: (table, records) => {
			let request = {
				RequestItems: {},
				'ReturnConsumedCapacity': 'TOTAL',
			};
			request.RequestItems[table] = records;
			return new Promise((resolve, reject) => {
				docClient.batchWrite(request, (err, data) => {
					if (err) {
						logger.error(`All ${records.length} records failed`, err);
						return reject(err);
					} else if (table in data.UnprocessedItems && Object.keys(data.UnprocessedItems[table]).length) {
						logger.info(`Unprocessed ${data.UnprocessedItems[table].length} records`);
						return resolve(data.UnprocessedItems[table]);
					}

					resolve();
				});
			});
		},
		get: (table, id, opts = {}) => {
			let key = id;
			if (typeof key !== 'object') {
				key = {
					[opts.id || 'id']: key,
				};
			}
			return docClient.get({
				ConsistentRead: true,
				Key: key,
				ReturnConsumedCapacity: 'TOTAL',
				TableName: table,
			}).promise().then(data => {
				if (!data.Item) {
					return opts.default || null;
				} else {
					return data.Item;
				}
			});
		},
		merge: function (table, id, obj, opts = {}) {
			return new Promise((resolve, reject) => {
				this.get(table, id, opts).then(data => {
					data = merge(data, obj);
					this.put(table, id, data, opts).then(data => {
						resolve(data);
					}).catch(err => {
						reject(err);
					});
				});
			});
		},
		put: (TableName, Key, Item, opts = {}) => {
			if (typeof Key !== 'object') {
				Key = {
					[opts.id || 'id']: Key,
				};
			}

			return docClient.put({
				Item,
				Key,
				ReturnConsumedCapacity: 'TOTAL',
				TableName,
			}).promise().then(data => true);
		},
		query: (params) => {
			return docClient.query(params).promise().then(data => data.Items);
		},
		scan: function (table) {
			if (typeof table === 'string') {
				return docClient.scan({
					ReturnConsumedCapacity: 'TOTAL',
					TableName: table,
				}).promise().then(data => data.Items);
			} else {
				return docClient.scan(table).promise().then(data => data.Items);
			}
		},
		smartQuery: function query (params, configuration = {}, stats = {}) {
			let self = this;
			let config = merge({}, {
				count: params.Limit || null,
				mb: 2,
				method: 'query',
				progress: (data, stats, callback) => {
					callback(true);
					return true;
				},
			}, configuration);

			if (configuration.count != undefined && params.Limit == undefined) {
				params.Limit = configuration.count;
			}

			stats = merge({}, {
				count: 0,
				mb: 0,
			}, stats);
			let method = config.method === 'scan' ? 'scan' : 'query';
			return new Promise((resolve, reject) => {
				docClient[method](params, function (err, data) {
					if (err) {
						reject(err);
					} else {
						stats.mb++;
						stats.count += data.Count;
						config.progress(data, stats, function (shouldContinue) {
							shouldContinue = shouldContinue == null || shouldContinue == undefined || shouldContinue;
							if (shouldContinue && data.LastEvaluatedKey && stats.mb < config.mb && (config.count == null || stats.count < config.count)) {
								params.ExclusiveStartKey = data.LastEvaluatedKey;

								if (config.count) {
									params.Limit = Math.min(params.Limit || Number.POSITIVE_INFINITY, config.count - stats.count);
								}

								self.smartQuery(params, config, stats).then(function (innerData) {
									data.Items = data.Items.concat(innerData.Items);
									data.ScannedCount += innerData.ScannedCount;
									data.Count += innerData.Count;
									data.LastEvaluatedKey = innerData.LastEvaluatedKey;
									if (data.ConsumedCapacity && innerData.ConsumedCapacity) {
										data.ConsumedCapacity.CapacityUnits += innerData.ConsumedCapacity.CapacityUnits;
									}
									data._stats = innerData._stats;
									resolve(data);
								}).catch(function (err) {
									reject(err);
								});
							} else {
								data._stats = stats;
								resolve(data);
							}
						});
					}
				});
			});
		},
		streamToTable: function (table, opts = {}) {
			let self = this;
			opts = merge({
				records: 25,
				size: 1024 * 1024 * 2,
				time: {
					seconds: 2,
				},
			}, opts || {});

			let records;

			let keysArray = [opts.hash];
			opts.range && keysArray.push(opts.range);
			let key = opts.range ? (obj) => {
				return `${obj[opts.hash]}-${obj[opts.range]}`;
			} : (obj) => {
				return `${obj[opts.hash]}`;
			};
			let assign = (self, key, obj) => {
				self.data[key] = obj;
				return false;
			};
			if (opts.merge) {
				assign = (self, key, obj) => {
					if (key in self.data) {
						self.data[key] = merge(self.data[key], obj);
						return true;
					} else {
						self.data[key] = obj;
						return false;
					}
				};
			}

			function reset () {
				if (opts.hash || opts.range) {
					records = {
						data: {},
						length: 0,
						map: function (each) {
							return Object.keys(this.data).map(key => each(this.data[key]));
						},
						push: function (obj) {
							this.length++;
							return assign(this, key(obj), obj);
						},
					};
				} else {
					records = [];
				}
			}

			reset();

			let retry = backoff.fibonacci({
				initialDelay: 100,
				maxDelay: 1000,
				randomisationFactor: 0,
			});
			retry.failAfter(10);

			retry.success = function () {
				retry.reset();
				retry.emit('success');
			};

			retry.run = function (callback) {
				let fail = (err) => {
					retry.removeListener('success', success);
					callback(err || 'failed');
				};
				let success = () => {
					retry.removeListener('fail', fail);
					reset();
					callback();
				};
				retry.once('fail', fail).once('success', success);

				retry.fail = function (err) {
					retry.reset();
					callback(err);
				};
				retry.backoff();
			};

			retry.on('ready', function (number, delay) {
				if (records.length === 0) {
					retry.success();
				} else {
					logger.info('sending', records.length, number, delay);
					logger.time('dynamodb request');

					let keys = [];
					let lookup = {};
					let all = records.map((r) => {
						let wrapper = {
							PutRequest: {
								Item: r,
							},
						};

						if (opts.merge && opts.hash) {
							lookup[key(r)] = wrapper;
							keys.push({
								[opts.hash]: r[opts.hash],
								[opts.range]: opts.range && r[opts.range]
							});
						}
						return wrapper;
					});
					let getExisting = opts.merge ? ((done) => {
						this.batchGetTable(table, keys);
					}) : done => done(null, []);

					let tasks = [];
					for (let ndx = 0; ndx < all.length; ndx += 25) {
						let myRecords = all.slice(ndx, ndx + 25);
						tasks.push(function (done) {
							let retry = {
								backoff: (err) => {
									done(null, {
										backoff: err || 'error',
										records: myRecords,
									});
								},
								fail: (err) => {
									done(null, {
										fail: err || 'error',
										records: myRecords,
									});
								},
								success: () => {
									done(null, {
										success: true,
										// records: myRecords
									});
								},
							};
							self.batchTableWrite(table, myRecords).then(unprocessedItems => {
								if (unprocessedItems && unprocessedItems.length) {
									myRecords = unprocessedItems;
									retry.backoff();
								} else {
									logger.info(table, 'saved');
									retry.success();
								}
							}).catch(err => {
								logger.info(`All ${myRecords.length} records failed! Retryable: ${err.retryable}`, err);
								logger.error(myRecords);
								if (err.retryable) {
									retry.backoff(err);
								} else {
									retry.fail(err);
								}
							});
						});
					}
					getExisting((err, existing) => {
						if (err) {
							return retry.fail(err);
						}
						existing.map(e => {
							let newObj = lookup[key(e)];
							newObj.PutRequest.Item = merge(e, newObj.PutRequest.Item);
						});
						async.parallelLimit(tasks, 10, (err, results) => {
							if (err) {
								retry.fail(err);
							} else {
								let fail = false;
								let backoff = false;
								reset();
								results.map(r => {
									fail = fail || r.fail;
									backoff = backoff || r.backoff;
									if (!r.success) {
										r.records.map(m => records.push(m.PutRequest.Item));
									}
								});

								if (fail) {
									retry.fail(fail);
								} else if (backoff) {
									retry.backoff(backoff);
								} else {
									retry.success();
								}
							}
						});
					});
				}
			});

			return ls.buffer({
				buffer: opts.buffer,
				debug: opts.debug,
				label: 'toDynamoDB',
				records: opts.records,
				size: opts.size,
				time: opts.time,
				writeStream: true,
			}, function (obj, done) {
				records.push(obj);

				done(null, {
					records: 1,
					size: obj.gzipSize,
				});
			}, retry.run, function flush (done) {
				logger.info('streamToTable On Flush');
				done();
			});
		},
		update: (table, key, set, opts = {}) => {
			if (typeof key !== 'object') {
				key = {
					'id': key,
				};
			}

			let sets = [];
			let names = {};
			let attributes = {};
			for (let k in set) {
				if (set[k] != undefined) {
					let fieldName = k.replace(/[^a-z]+/ig, '_');
					let fieldOpts = (opts.fields && opts.fields[k]) || {};
					if (fieldOpts.once) {
						sets.push(`#${fieldName} = if_not_exists(#${fieldName}, :${fieldName})`);
					} else {
						sets.push(`#${fieldName} = :${fieldName}`);
					}
					names[`#${fieldName}`] = k;
					attributes[`:${fieldName}`] = set[k];
				}
			}

			if (Object.keys(attributes) == 0) {
				attributes = undefined;
			}
			if (Object.keys(names) == 0) {
				names = undefined;
			}
			let command = {
				ExpressionAttributeNames: names,
				ExpressionAttributeValues: attributes,
				Key: key,
				ReturnConsumedCapacity: 'TOTAL',
				TableName: table,
				UpdateExpression: sets.length ? 'set ' + sets.join(', ') : undefined,
			};
			if (opts.ReturnValues) {
				command.ReturnValues = opts.ReturnValues;
			}

			return docClient.update(command).promise().then(data => command.ReturnValues && command.ReturnValues !== 'NONE' ? data : true);
		},
		updateMulti: function (items, opts = {}) {
			opts = merge({
				limit: 20,
			}, opts);

			let funcs = [];
			items.forEach((item) => {
				funcs.push((done) => {
					this.update(item.table, item.key, item.set, opts).then(data => {
						done(null, data);
					}).catch(done);
				});
			});

			return new Promise((resolve, reject) => {
				async.parallelLimit(funcs, opts.limit, (err, data) => {
					if (err) {
						reject(err);
					} else {
						resolve(data);
					}
				});
			});
		},
		/**
		 * @aka createTableWriteStream
		 * @param table
		 * @param opts
		 * @returns {{put: put, delete: delete, end: end}}
		 */
		writeToTableInChunks: function (table, opts) {
			opts = merge({
				chunk_size: 25,
				concurrency: 10,
				concurrency_delay: 100,
				data_size: 400000,
				keys: [],
			}, opts || {});

			let chunker = chunk((items, done) => {
				if (opts.keys.length) {
					let hash = opts.keys[0];
					let range = opts.keys[1];

					let seen = new Set();
					// Process in reverse, so that the newest record goes through and so I can delete without readjusting keys
					for (let i = items.length - 1; i >= 0; i--) {
						let id;
						if (items[i].PutRequest) {
							id = items[i].PutRequest.Item[hash] + '' + items[i].PutRequest.Item[range];
						}
						else {
							id = items[i].DeleteRequest.Key[hash] + '' + items[i].DeleteRequest.Key[range];
						}
						if (seen.has(id)) {
							items.splice(i, 1);
						} else {
							seen.add(id);
						}
					}
				}
				if (items.length > 0) {
					this.batchTableWrite(table, items).then(unprocessedItems => {
						if (unprocessedItems && Object.keys(unprocessedItems).length) {
							done('could not write records', unprocessedItems);
						} else {
							done();
						}
					}).catch(err => {
						done('could not write records', items, err);
					});
				} else {
					done();
				}
			}, opts);
			return {
				delete: (key) => {
					chunker.add({
						DeleteRequest: {
							Key: key,
						},
					});
				},
				end: chunker.end,
				put: (item) => {
					chunker.add({
						PutRequest: {
							Item: item,
						},
					});
				},
			};
		},
	};
};
