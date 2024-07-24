"use strict";
var { KMS } = require('@aws-sdk/client-kms');
module.exports = function(configuration) {
	let kms = new KMS(configuration);
	return {
		_service: kms,
		decrypt: function(encryptedString) {
			return kms.decrypt({
				CiphertextBlob: Buffer.from(encryptedString, 'base64')
			}).then(data => data.Plaintext.toString('ascii'));
		},
		encrypt: function(key, value) {
			return kms.encrypt({
				KeyId: key,
				Plaintext: value
			}).then(data => data.CiphertextBlob.toString("base64"));
		}
	}
};
