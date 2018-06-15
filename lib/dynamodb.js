"use strict";
var AWS = require('aws-sdk');
var https = require("https");
let extend = require("extend");
const async = require("async");
const chunk = require("../utils/chunker.js");

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
	var docClient = new AWS.DynamoDB.DocumentClient(configuration);
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
		update: function(table, key, set, opts = {}) {
			if (typeof key == "string") {
				key = {
					"id": key
				};
			}
			var sets = [];
			var names = {};
			var attributes = {};
			for (var k in set) {
				if (set[k] != undefined) {
					var fieldName = k.replace(/[^a-z]+/ig, "_");
					var fieldOpts = opts.fields && opts.fields[k] || {};
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

			var command = {
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
			var config = Object.assign({}, {
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
				var uniquemap = {};
				var results = [];
				var chunker = chunk(function(items, done) {
					if (items.length > 0) {
						var params = {
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

				for (var i = 0; i < keys.length; i++) {
					var identifier = JSON.stringify(keys[i]);
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
				var ret = {};
				ret[hashkey] = e;
				return ret;
			}), opts).then(results => {
				var result = {};
				for (var i = 0; i < results.length; i++) {
					var row = results[i];
					result[row[hashkey]] = row;
				}
				return result;
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

			var chunker = chunk(function(items, done) {
				if (opts.keys.length) {
					var hash = opts.keys[0];
					var range = opts.keys[1];

					var seen = {};
					//Process in reverse, so that the newest record goes through and so I can delete without readjusting keys
					for (var i = items.length - 1; i >= 0; i--) {
						var id = items[i].PutRequest.Item[hash] + "" + items[i].PutRequest.Item[range];
						if (id in seen) {
							items.splice(i, 1);
						} else {
							seen[id] = 1;
						}
					}
				}
				if (items.length > 0) {
					var request = {
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
