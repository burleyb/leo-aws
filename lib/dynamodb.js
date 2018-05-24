"use strict";
var AWS = require('aws-sdk');
var https = require("https");
let extend = require("extend");
const async = require("async");

// AWS.config.logger = console;

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
		}
	};
};
