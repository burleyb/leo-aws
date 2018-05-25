"use strict";
const aws = require('aws-sdk');

module.exports = function(configuration) {
	let service = new aws.CloudFormation(configuration);

	return {
		_service: service,
		getStackResources: function(stack) {
			return service.listStackResources({
				StackName: stack
			}).promise().then((data) => {
				console.log(data);
				if (data.NextToken) {
					console.log("We need to deal with next token");
				}
				var resources = {};
				data.StackResourceSummaries.map((resource) => {
					resources[resource.LogicalResourceId] = {
						type: resource.ResourceType,
						id: resource.PhysicalResourceId,
						name: resource.LogicalResourceId
					};
				});

				return resources;
			});
		}
	};
};
