'use strict';

module.exports = async function(stackname, configuration) {
	const leoaws = require('../index')(configuration);

	let resources = await leoaws.cloudformation.getStackResources(stackname);

	let profile = {
		"Region": configuration.region
	};
	Object.keys(resources).forEach((id) => {
		if (resources[id].type.match(/Table|Bucket|DeliveryStream|Stream/)) {
			profile[id] = resources[id].id;
		}
	});

	return profile;
};
