let cache = {};

const secrets = require("./lib/secretsmanager");
const dynamodb = require("./lib/dynamodb");
const cloudformation = require('./lib/cloudformation');

function build(configuration) {
	return {
		secrets: secrets(configuration),
		dynamodb: dynamodb(configuration),
		cloudformation: cloudformation(configuration)
	};
}

module.exports = function(configuration, forceNew = false) {
	let c = JSON.stringify(configuration);

	if (!(c in cache) || forceNew) {
		cache[c] = build(configuration);
	}
	return cache[c];
};
