"use strict";
const { SQS } = require('@aws-sdk/client-sqs');

module.exports = function(configuration) {
	let sqs = new SQS(configuration);

	return {
		_service: sqs,
		deleteMessage: function(params) {
			return sqs.deleteMessage(params).then(data => data.ResponseMetadata.RequestId);
		},
		sendMessage: function(params) {
			return sqs.sendMessage(params).then(data => data.MessageId);
		},
		receiveMessage: function(params) {
			return sqs.receiveMessage(params).then(data => data.Messages);
		},
		sendMessageBatch: function(params) {
			return sqs.sendMessageBatch(params).then(data => data);
		}
	};
};
