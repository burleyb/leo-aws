let cache = {};

const cloudformation = require('./lib/cloudformation');
const dynamodb = require("./lib/dynamodb");
const kms = require("./lib/kms");
const secrets = require("./lib/secretsmanager");

function build(configuration) {
	return {
		region: configuration.region,
		cloudformation: cloudformation(configuration),
		dynamodb: dynamodb(configuration),
		kms: kms(configuration),
		secrets: secrets(configuration),
		profile: configuration.profile
	};
}

module.exports = function(configuration, forceNew = false) {
	let c = JSON.stringify(configuration);

	if (!(c in cache) || forceNew) {
		cache[c] = build(configuration);
	}
	return cache[c];
};
