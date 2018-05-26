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
			return docClient.get({
				TableName: table,
				Key: {
					[opts.id || 'id']: id
				},
				ConsistentRead: true,
				"ReturnConsumedCapacity": 'TOTAL'
			}).promise().then(data => data.Item);
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
		}
	};
};
