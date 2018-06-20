"use strict";
const aws = require('aws-sdk');

module.exports = function(configuration) {
	let sqs = new aws.SQS(configuration);

	return {
		_service: sqs,
		deleteMessage: function(params) {
			return sqs.deleteMessage(params).promise().then(data => data.ResponseMetadata.RequestId);
		},
		sendMessage: function(params) {
			return sqs.sendMessage(params).promise().then(data => data.MessageId);
		},
		receiveMessage: function(params) {
			return sqs.receiveMessage(params).promise().then(data => data.Messages);
		},
		sendMessageBatch: function(params) {
			return sqs.sendMessageBatch(params).promise().then(data => data.MessageId);
		}
	};
};
