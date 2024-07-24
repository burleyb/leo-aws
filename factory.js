'use strict';

const config = require('leo-config');
const { fromIni } = require("@aws-sdk/credential-providers");
const merge = require('lodash.merge');
const requireFn = typeof __webpack_require__ === "function" ? __non_webpack_require__ : require;


const leoaws = {
	cloudformation: require('./lib/cloudformation'),
	dynamodb: require('./lib/dynamodb'),
	kms: require('./lib/kms'),
	secrets: require('./lib/secretsmanager'),
	sqs: require('./lib/sqs'),
};

function factory(service, options) {
	const configuration = config.leoaws;
	if (configuration && configuration.profile && !configuration.credentials) {
		configuration.credentials = fromIni({
			profile: configuration.profile,
			role: configuration.role,
		});
	}

	if (options) {
		merge(configuration, options);
	}

	const lowerService = service.toLowerCase();
	if (leoaws[lowerService]) {
		return leoaws[lowerService](configuration);
	} else {
		// return a configured AWS service
		let serviceLib = requireFn("@aws-sdk/client-" + service.replace(/[A-Z]/g, (a) => "-" + a.toLowerCase()).replace(/^-/, ""));
		return {
			_service: new serviceLib[service](configuration),
		};
	}
}

factory.injector = (dependencies = {}) => {
	merge(leoaws, dependencies);
};

module.exports = factory;
