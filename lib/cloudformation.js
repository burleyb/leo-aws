"use strict";
const aws = require('aws-sdk');
const readline = require('readline');

module.exports = function(configuration) {
	let service = new aws.CloudFormation(configuration);
	return {
		_service: service,
		getStackResources: function(stack) {
			return service.listStackResources({
				StackName: stack
			}).promise().then((data) => {
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
		},
		get: function(stack, opts) {
			return service.getTemplate(Object.assign({
				StackName: stack,
			}, opts)).promise().then(data => JSON.parse(data.TemplateBody));
		},
		runChangeSet: function(stack, file, opts) {
			let updateOpts = Object.assign({}, opts);
			if (updateOpts.Parameters) {
				updateOpts.Parameters = updateOpts.Parameters.map(param => ({
					ParameterKey: param.ParameterKey,
					UsePreviousValue: param.UsePreviousValue,
					ParameterValue: param.ParameterValue
				}));
			}
			let changeSetId = null;
			let changeSetName = "leo-cli-" + Date.now();
			let params = Object.assign({
				ChangeSetName: changeSetName,
				ChangeSetType: "UPDATE",
				StackName: stack,
				TemplateURL: file,
				Capabilities: [
					"CAPABILITY_IAM",
				]
			}, updateOpts);
			return service.createChangeSet(params).promise().then(data => {
				changeSetId = data.Id;
				service.api.waiters["changeSetCreateComplete"].delay = 5;
				return this.waitFor("changeSetCreateComplete", {
					ChangeSetName: changeSetId
				});
			}).then(data => {
				changeSetId = data.ChangeSetId;
				console.log(changeSetId);

				function rightPad(val, count) {
					return (val + " ".repeat(count)).slice(0, count) + "  ";
				}
				console.log(`${rightPad("Action",30)}${rightPad("Logical ID",30)}${rightPad("Physical ID",30)}${rightPad("Resource Type",30)}${rightPad("Replacement",30)}`);
				data.Changes.map(change => {
					change = change.ResourceChange;
					console.log(`${rightPad(change.Action,30)}${rightPad(change.LogicalResourceId,30)}${rightPad(change.PhysicalResourceId,30)}${rightPad(change.ResourceType,30)}${rightPad(change.Replacement,30)}`);
				});
				const rl = readline.createInterface({
					input: process.stdin,
					output: process.stdout
				});
				return new Promise((resolve) => {
					rl.question('Press [Enter] to execute change set', (data) => {
						rl.close();
						console.log("Executing Change Set");
						resolve(service.executeChangeSet({
							ChangeSetName: changeSetId
						}).promise().then(data => {
							service.api.waiters["stackUpdateComplete"].delay = 10;
							return this.waitFor("stackUpdateComplete", {
								StackName: stack
							});
						}));
					});
				})
			}).catch(err => {
				console.log(err);
				if (err.message.match(/^Stack.*does not exist/)) {
					return this.createStack(stack, file, updateOpts.Parameters, opts, true, true);
				} else {
					throw err;
				}
			});
		},
		run: function(stack, file, opts) {
			let updateOpts = Object.assign({}, opts);
			if (updateOpts.Parameters) {
				updateOpts.Parameters = updateOpts.Parameters.map(param => ({
					ParameterKey: param.ParameterKey,
					UsePreviousValue: param.UsePreviousValue,
					ParameterValue: param.ParameterValue
				}));
			}
			console.log(updateOpts);
			return service.updateStack(Object.assign({
				StackName: stack,
				TemplateURL: file,
				Capabilities: [
					"CAPABILITY_IAM",
				]
			}, updateOpts)).promise().then(data => {
				service.api.waiters["stackUpdateComplete"].delay = 10;
				return this.waitFor("stackUpdateComplete", {
					StackName: stack
				});
			}).catch(err => {
				if (err.message.match(/^Stack.*does not exist/)) {
					return this.createStack(stack, file, updateOpts.Parameters, opts, true, true);
				} else {
					throw err;
				}
			});
		},
		describeStackResources: function(stack) {
			return service.describeStackResources({
				StackName: stack
			}).promise().then(data => data.StackResources);
		},
		waitFor: function(action, params) {
			return service.waitFor(action, params).promise();
		},
		createStack: async function createStack(name, template, paramaters = [], waitFor = true, describeStack = true) {
			let templateBody;
			if (typeof template === "string") {
				templateBody = "TemplateURL";
			} else {
				templateBody = "TemplateBody";
				template = JSON.stringify(template);
			}

			let promise = service.createStack({
				StackName: name,
				Capabilities: [
					"CAPABILITY_IAM"
				],
				OnFailure: "DELETE",
				[templateBody]: template,
				Parameters: paramaters
			}).promise();
			if (waitFor || describeStack) {
				service.api.waiters["stackCreateComplete"].delay = 10;
				promise = promise.then(() => this.waitFor("stackCreateComplete", {
					StackName: name
				})).then(waitdata => ({
					stack: name,
					region: configuration.region,
					details: waitdata.Stacks[0]
				}));
			} else if (describeStack) {
				promise = promise.then(() => this.describeStackResources(name)).then(data => ({
					stack: name,
					region: configuration.region,
					details: {
						StackResources: data
					}
				}));
			}
			return promise;
		}
	};
};
