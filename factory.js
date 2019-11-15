'use strict';

const config = require('leo-config');
const AWS = require('aws-sdk');
const merge = require('lodash.merge');

const leoaws = {
	cloudformation: require('./lib/cloudformation'),
	dynamodb: require('./lib/dynamodb'),
	kms: require('./lib/kms'),
	secrets: require('./lib/secretsmanager'),
	sqs: require('./lib/sqs'),
};

function factory (service) {
	const configuration = config.leoaws;
	if (configuration && configuration.profile && !configuration.credentials) {
		configuration.credentials = new AWS.SharedIniFileCredentials({
			profile: configuration.profile,
			role: configuration.role
		});
	}

	const lowerService = service.toLowerCase();
	if (leoaws[lowerService]) {
		return leoaws[lowerService](configuration);
	} else {
		// return a configured AWS service
		return {
			_service: new AWS[service](configuration)
		};
	}
}

factory.injector = (dependencies = {}) => {
	merge(leoaws, dependencies);
};

module.exports = factory;
