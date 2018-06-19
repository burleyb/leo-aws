"use strict";
var aws = require('aws-sdk');
module.exports = function(configuration) {
	let kms = new aws.KMS(configuration);
	return {
		_service: kms,
		decrypt: function(encryptedString) {
			return kms.decrypt({
				CiphertextBlob: new Buffer(encryptedString, 'base64')
			}).promise().then(data => data.Plaintext.toString('ascii'));
		},
		encrypt: function(key, value) {
			return kms.encrypt({
				KeyId: key,
				Plaintext: value
			}).promise().then(data => data.CiphertextBlob.toString("base64"));
		}
	}
};
