"use strict";
const AWS = require('aws-sdk');
const https = require("https");
const merge = require('lodash.merge');
const chunk = require("../utils/chunker.js");
const async = require("async");
const backoff = require('backoff');

/**
 *
 * @param configuration
 * @returns {{_service: AWS.DynamoDB.DocumentClient, get: get, put: put, update: update, scan: scan, query: query, smartQuery: query, batchGetTable: batchGetTable, batchGetHashkey: batchGetHashkey, writeToTableInChunks: writeToTableInChunks}}
 */
module.exports = function(configuration) {
	configuration = Object.assign({
			maxRetries: 2,
			convertEmptyValues: true,
			httpOptions: {
				connectTimeout: 2000,
				timeout: 5000,
				agent: new https.Agent({
					ciphers: 'ALL',
					secureProtocol: 'TLSv1_method',
					// keepAlive: true
				})
			}
		},
		configuration || {});
	let docClient = new AWS.DynamoDB.DocumentClient(configuration);
	return {
		_service: docClient,
		get: function(table, id, opts = {}) {
			let key = id;
			if (typeof key != 'object') {
				key = {
					[opts.id || 'id']: key
				};
			}
			return docClient.get({
				TableName: table,
				Key: key,
				ConsistentRead: true,
				"ReturnConsumedCapacity": 'TOTAL'
			}).promise().then(data => data.Item);
		},
		put: function(table, id, item, opts = {}) {
			item[opts.id || 'id'] = id;
			return docClient.put({
				TableName: table,
				Key: {
					[opts.id || 'id']: id
				},
				Item: item,
				"ReturnConsumedCapacity": 'TOTAL'
			}).promise().then(data => true);
		},
		merge: function(table, id, obj, opts = {}) {
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
		update: function(table, key, set, opts = {}) {
			if (typeof key == "string") {
				key = {
					"id": key
				};
			}
			let sets = [];
			let names = {};
			let attributes = {};
			for (let k in set) {
				if (set[k] != undefined) {
					let fieldName = k.replace(/[^a-z]+/ig, "_");
					let fieldOpts = opts.fields && opts.fields[k] || {};
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
				TableName: table,
				Key: key,
				UpdateExpression: sets.length ? 'set ' + sets.join(", ") : undefined,
				ExpressionAttributeNames: names,
				ExpressionAttributeValues: attributes,
				"ReturnConsumedCapacity": 'TOTAL'
			};
			if (opts.ReturnValues) {
				command.ReturnValues = opts.ReturnValues;
			}
			return docClient.update(command).promise().then(data => true);
		},
		updateMulti: function(items, opts = {}) {
			opts = Object.assign({
				limit: 20
			}, opts);

			let funcs = [];
			items.forEach((item) => {
				funcs.push((done) => {
					this.update(item.table, item.key, item.set, opts);
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
		scan: function(table, opts = {}) {
			return docClient.scan({
				TableName: table,
				"ReturnConsumedCapacity": 'TOTAL'
			}).promise().then(data => data.Items);
		},
		query: function(params, opts = {}) {
			return docClient.query(params).promise().then(data => data.Items);
		},
		smartQuery: function query(params, configuration, stats) {
			let config = Object.assign({}, {
				mb: 2,
				count: null,
				method: "query",
				progress: function(data, stats, callback) {
					callback(true);
					return true;
				}
			}, configuration);
			stats = Object.assign({}, {
				mb: 0,
				count: 0
			}, stats);
			let method = config.method == "scan" ? "scan" : "query";
			return new Promise((resolve, reject) => {
				//console.log(params);
				docClient[method](params, function(err, data) {
					if (err) {
						reject(err);
					} else {
						stats.mb++;
						stats.count += data.Count;
						//console.log(config, stats)
						config.progress(data, stats, function(shouldContinue) {
							shouldContinue = shouldContinue == null || shouldContinue == undefined || shouldContinue;
							if (shouldContinue && data.LastEvaluatedKey && stats.mb < config.mb && (config.count == null || stats.count < config.count)) {
								//console.log("Running subquery with start:", data.LastEvaluatedKey)
								params.ExclusiveStartKey = data.LastEvaluatedKey;
								query(params, config, stats).then(function(innerData) {
									data.Items = data.Items.concat(innerData.Items)
									data.ScannedCount += innerData.ScannedCount;
									data.Count += innerData.Count;
									data.LastEvaluatedKey = innerData.LastEvaluatedKey
									if (data.ConsumedCapacity && innerData.ConsumedCapacity) {
										data.ConsumedCapacity.CapacityUnits += innerData.ConsumedCapacity.CapacityUnits;
									}
									data._stats = innerData._stats;
									resolve(data)
								}).catch(function(err) {
									reject(err);
								});

							} else {
								data._stats = stats;
								resolve(data);
							}
						})

					}
				});
			});

			return deferred;
		},
		batchGetTable: function(table, keys, opts = {}) {
			return new Promise((resolve, reject) => {
				opts = Object.assign({
					chunk_size: 100,
					concurrency: 3
				}, opts);
				let uniquemap = {};
				let results = [];
				let chunker = chunk(function(items, done) {
					if (items.length > 0) {
						let params = {
							RequestItems: {},
							"ReturnConsumedCapacity": 'TOTAL'
						};
						params.RequestItems[table] = {
							Keys: items
						};
						docClient.batchGet(params, function(err, data) {
							if (err) {
								console.log(err);
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

				chunker.end(function(err, rs) {
					if (err) {
						reject(err);
					} else {
						resolve(results);
					}
				});
			});
		},
		batchGetHashkey: function(table, hashkey, ids, opts = {}) {
			return this.batchGetTable(table, ids.map(function(e) {
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
		createTableWriteStream: function(table, opts = {}) {
			opts = Object.assign({
				chunk_size: 25,
				data_size: 400000,
				concurrency: 10,
				concurrency_delay: 100,
				keys: []
			}, opts || {});

			let chunker = chunk((items, done) => {
				if (opts.keys.length) {
					let hash = opts.keys[0];
					let range = opts.keys[1];

					let seen = {};
					//Process in reverse, so that the newest record goes through and so I can delete without readjusting keys
					for (let i = items.length - 1; i >= 0; i--) {
						let id = items[i].PutRequest.Item[hash] + "" + items[i].PutRequest.Item[range];
						if (id in seen) {
							items.splice(i, 1);
						} else {
							seen[id] = 1;
						}
					}
				}
				if (items.length > 0) {
					this.batchTableWrite(table, items).then(unprocessedItems => {
						if (Object.keys(unprocessedItems).length) {
							done('could not write records', unprocessedItems);
						}
					}).catch(err => {
						done('could not write records', items, err);
					});
				}

				done();
			}, opts);

			return {
				put: function(item) {
					chunker.add({
						PutRequest: {
							Item: item
						}
					});
				},
				end: chunker.end
			};
		},
		batchTableWrite: function(table, records) {
			let request = {
				RequestItems: {},
				"ReturnConsumedCapacity": 'TOTAL'
			};
			request.RequestItems[table] = records;
			return new Promise((resolve, reject) => {
				docClient.batchWrite(request, function (err, data) {
					if (err) {
						console.log(`All ${records.length} records failed`, err);
						reject(err);
					} else if (table in data.UnprocessedItems && Object.keys(data.UnprocessedItems[table]).length !== 0) {
						console.log(`Unprocessed ${data.UnprocessedItems[table].length} records`);
						resolve(data.UnprocessedItems[table]);
					}

					resolve();
				});
			});
		},
		streamToTable: function(table, opts = {}) {
			opts = Object.assign({
				records: 25,
				size: 1024 * 1024 * 2,
				time: {
					seconds: 2
				}
			}, opts || {});

			let records, size;

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
				}
			}

			function reset() {
				if (opts.hash || opts.range) {
					records = {
						length: 0,
						data: {},
						push: function(obj) {
							this.length++;
							return assign(this, key(obj), obj);
						},
						map: function(each) {
							return Object.keys(this.data).map(key => each(this.data[key]));
						}
					};
				} else {
					records = [];
				}
			}
			reset();

			let retry = backoff.fibonacci({
				randomisationFactor: 0,
				initialDelay: 100,
				maxDelay: 1000
			});
			retry.failAfter(10);
			retry.success = function() {
				retry.reset();
				retry.emit("success");
			};
			retry.run = function(callback) {
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

				retry.fail = function(err) {
					retry.reset();
					callback(err);
				};
				retry.backoff();
			};
			retry.on('ready', function(number, delay) {
				if (records.length === 0) {
					retry.success();
				} else {
					logger.info("sending", records.length, number, delay);
					logger.time("dynamodb request");

					let keys = [];
					let lookup = {};
					let all = records.map((r) => {
						let wrapper = {
							PutRequest: {
								Item: r
							}
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
						tasks.push(function(done) {
							let retry = {
								backoff: (err) => {
									done(null, {
										backoff: err || "error",
										records: myRecords
									});
								},
								fail: (err) => {
									done(null, {
										fail: err || "error",
										records: myRecords
									})
								},
								success: () => {
									done(null, {
										success: true,
										//records: myRecords
									});
								}
							};
							this.batchTableWrite(table, myRecords).then(unprocessedItems => {
								if (Object.keys(unprocessedItems).length) {
									myRecords = data.UnprocessedItems[table];
									retry.backoff();
								} else {
									logger.info(table, "saved");
									retry.success();
								}
							}).catch(err => {
								logger.info(`All ${myRecords.length} records failed! Retryable: ${err.retryable}`, err);
								logger.error(myRecords)
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
						})
						async.parallelLimit(tasks, 10, (err, results) => {
							if (err) {
								retry.fail(err)
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
									retry.fail(fail)
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
				writeStream: true,
				label: "toDynamoDB",
				time: opts.time,
				size: opts.size,
				records: opts.records,
				buffer: opts.buffer,
				debug: opts.debug
			}, function(obj, done) {
				size += obj.gzipSize;
				records.push(obj);

				done(null, {
					size: obj.gzipSize,
					records: 1
				});
			}, retry.run, function flush(done) {
				logger.info("toDynamoDB On Flush");
				done();
			});
		},
		writeToTableInChunks: function(table, opts) {
			opts = Object.assign({
				chunk_size: 25,
				data_size: 400000,
				concurrency: 10,
				concurrency_delay: 100,
				keys: []
			}, opts || {});

			let chunker = chunk(function(items, done) {
				if (opts.keys.length) {
					let hash = opts.keys[0];
					let range = opts.keys[1];

					let seen = {};
					//Process in reverse, so that the newest record goes through and so I can delete without readjusting keys
					for (let i = items.length - 1; i >= 0; i--) {
						let id = items[i].PutRequest.Item[hash] + "" + items[i].PutRequest.Item[range];
						if (id in seen) {
							items.splice(i, 1);
						} else {
							seen[id] = 1;
						}
					}
				}
				if (items.length > 0) {
					let request = {
						RequestItems: {},
						"ReturnConsumedCapacity": 'TOTAL'
					};
					request.RequestItems[table] = items;
					docClient.batchWrite(request, function(err, data) {
						if (err) {
							console.log(`All ${items.length} records failed`, err);
							done("could not write records", items, err);
						} else if (table in data.UnprocessedItems && Object.keys(data.UnprocessedItems[table]).length !== 0) {
							console.log(`Unprocessed ${data.UnprocessedItems[table].length} records`);
							done("unprocessed records", data.UnprocessedItems[table], "unprocessed records");
						} else {
							done(null, []);
						}
					});
				} else {
					done();
				}
			}, opts);
			return {
				put: function(item) {
					chunker.add({
						PutRequest: {
							Item: item
						}
					});
				},
				delete: function(key) {
					chunker.add({
						DeleteRequest: {
							Key: key
						}
					});
				},
				end: chunker.end
			};
		}
	};
};
