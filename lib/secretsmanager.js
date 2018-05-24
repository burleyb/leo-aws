"use strict";
const aws = require('aws-sdk');

let cache = {};
module.exports = function(configuration) {
	let secret = new aws.SecretsManager(configuration);
	return {
		_service: secret,
		getSecret: function(secretName, opts) {
			opts = Object.assign({
				cache: 1000 * 60 * 5
			}, opts || {});
			if (cache[secretName]) {
				if (cache[secretName].t + opts.cache > Date.now()) {
					return cache[secretName].data;
				} else {
					delete cache[secretName];
				}
			}
			return new Promise((resolve, reject) => {
				secret.getSecretValue({
					SecretId: secretName
				}).promise().then(data => {
					try {
						let r = JSON.parse(data.SecretString);
						cache[secretName] = {
							t: Date.now(),
							data: r
						};
						resolve(r);
					} catch (e) {
						reject("Invalid JSON");
					}
				}).catch(reject);
			});
		}
	};
};
