let config = require("leo-config");
let cache = {};

const cloudformation = require('./lib/cloudformation');
const dynamodb = require("./lib/dynamodb");
const kms = require("./lib/kms");
const secrets = require("./lib/secretsmanager");
const sqs = require("./lib/sqs");
const { fromIni } = require("@aws-sdk/credential-providers");

/**
 *
 * @param configuration
 * @returns {(function(*=)) & {region, cloudformation: {_service, getStackResources, get, runChangeSet, run, describeStackResources, waitFor, createStack}, dynamodb: {_service, get, put, update, scan, query, smartQuery, batchGetTable, batchGetHashkey, writeToTableInChunks}, kms: {_service, decrypt, encrypt}, secrets: {_service, getSecret}, profile, config: *, sqs: {_service, deleteMessage, sendMessage, receiveMessage, sendMessageBatch}}}
 */
function build(configuration) {
	if (configuration.profile && !configuration.credentials) {
		configuration.credentials = fromIni({
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
