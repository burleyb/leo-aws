'use strict';

module.exports = async function (stackname, configuration) {
	const leoaws = require('../index')(configuration);

	let resources = await leoaws.cloudformation.getStackResources(stackname);

	let profile = {
		region: configuration.region,
		resources: {
			"Region": configuration.region
		}
	};
	Object.keys(resources).forEach((id) => {
		if (id == "LeoKinesisStream") {
			profile.kinesis = resources[id].id;
		} else if (id == "LeoFirehoseStream") {
			profile.firehose = resources[id].id;
		} else if (id == "LeoS3") {
			profile.s3 = resources[id].id;
		}
		if (resources[id].type.match(/Table|Bucket|DeliveryStream|Stream/)) {
			profile.resources[id] = resources[id].id;
		}
	});

	return profile;
};
