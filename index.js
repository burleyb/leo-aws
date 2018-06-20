let config = require("leo-config");
let cache = {};

const cloudformation = require('./lib/cloudformation');
const dynamodb = require("./lib/dynamodb");
const kms = require("./lib/kms");
const secrets = require("./lib/secretsmanager");
const sqs = require("./lib/sqs");
const AWS = require('aws-sdk');

function build(configuration) {
	if (configuration.profile && !configuration.credentials) {
		configuration.credentials = new AWS.SharedIniFileCredentials({
			profile: configuration.profile,
			role: configuration.role
		});
	}

	return Object.assign((config) => {
		return new build(config)
	}, {
		region: configuration.region,
		cloudformation: cloudformation(configuration),
		dynamodb: dynamodb(configuration),
		kms: kms(configuration),
		secrets: secrets(configuration),
		profile: configuration.profile,
		config: configuration,
		sqs: sqs(configuration)
	});
}

module.exports = new build(config.leoaws);