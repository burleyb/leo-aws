"use strict";
const { CloudFormation, waitUntilChangeSetCreateComplete } = require('@aws-sdk/client-cloudformation');
const readline = require('readline');

module.exports = function(configuration) {
	let service = new CloudFormation(configuration);
	return {
		_service: service,
		getStackResources: function(stack) {
			return service.listStackResources({
				StackName: stack
			}).then((data) => {
				if (data.NextToken) {
					console.log("We need to deal with next token");
				}
				var resources = {};
				data.StackResourceSummaries.map((resource) => {
					resources[resource.LogicalResourceId] = {
						Type: resource.ResourceType,
						Id: resource.PhysicalResourceId,
						Name: resource.LogicalResourceId
					};
				});

				return resources;
			});
		},
		get: function(stack, opts) {
			return service.getTemplate(Object.assign({
				StackName: stack,
			}, opts)).then(data => JSON.parse(data.TemplateBody));
		},
		/**
		 * Create a run a changeset
		 * @param stack
		 * @param file
		 * @param opts (Goes directly to AWS. Options must match AWS documentation)
		 * @param options (Options for this function)
		 * @returns {Promise<PromiseResult<CloudFormation.CreateChangeSetOutput, AWSError>>}
		 */
		runChangeSet: async function(stack, file, opts, options = {}) {
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
			return service.createChangeSet(params)
			.then(async data => {
		        const changeSetId = data.Id;
		        const stackId = data.StackId;
		
		        const waitConfig = {
		            minDelay: 5,  // Minimum delay in seconds between checks
		            maxDelay: 30, // Maximum delay in seconds between checks
		            maxWaitTime: 300 // Maximum time in seconds to wait for the operation to complete
		        };
		
		        try {
		            const result = await waitUntilChangeSetCreateComplete(
		                {
		                    client: service,  // Pass the CloudFormation client here
		                    maxWaitTime: waitConfig.maxWaitTime,
		                    minDelay: waitConfig.minDelay,
		                    maxDelay: waitConfig.maxDelay
		                },
		                {
		                    ChangeSetName: changeSetId, // Use the ChangeSet ID
		                    StackName: stackId, // Use the Stack ID
		                    IncludeNestedStacks: true // Optional, if you want nested stack details
		                }
		            );
		
		            console.log("--- ChangeSet creation complete ---");
		            return result;
		        } catch (err) {
		            console.error("Error waiting for ChangeSet creation to complete", err);
		            throw err;  // Re-throw the error to handle it later if needed
		        }
			}).then(data => {
				data = data.reason
				changeSetId = data.ChangeSetId;

				function rightPad(val, count) {
					return (val + " ".repeat(count)).slice(0, count) + "  ";
				}
				console.log(`${rightPad("Action", 30)}${rightPad("Logical ID", 30)}${rightPad("Physical ID", 30)}${rightPad("Resource Type", 30)}${rightPad("Replacement", 30)}`);
				data.Changes.map(change => {
					change = change.ResourceChange;
					console.log(`${rightPad(change.Action, 30)}${rightPad(change.LogicalResourceId, 30)}${rightPad(change.PhysicalResourceId, 30)}${rightPad(change.ResourceType, 30)}${rightPad(change.Replacement, 30)}`);
				});
				const rl = readline.createInterface({
					input: process.stdin,
					output: process.stdout
				});
				return new Promise((resolve, reject) => {

					if (options.forceDeploy) {
						resolve(this.executeChangeset(rl, service, changeSetId, stack));
					} else {
						if (options.progressInterval) {
							options.progressInterval.stop();
						}

						rl.question('Press [Enter] to execute change set', (data) => {
							if (options.progressInterval) {
								options.progressInterval.start();
							}
							resolve(this.executeChangeset(rl, service, changeSetId, stack))
						});
					}
				})
			}).catch(err => {
				if (err.message && err.message.match(/^Stack.*does not exist/)) {
					console.log("Stack does not exist, creating new stack");
					let createStart = Date.now();
					return this.createStack(stack, file, updateOpts.Parameters, opts, true, true);
				} else if (err.StatusReason) {
					throw new Error(err.StatusReason);
				} else {
					return service.describeChangeSet({
						ChangeSetName: changeSetId,
						StackName: stack
					}).then(cs => {
						throw cs;
					}).catch((err2) => {
						if (err2.StatusReason) {
							throw new Error(err2.StatusReason)
						} else {
							throw err;
						}
					});
				}
			});
		},
		/**
		 * Execute the created changeset
		 * @param rl
		 * @param service
		 * @param changeSetId
		 * @param stack
		 * @returns {Promise<any>}
		 */
		executeChangeset: function(rl, service, changeSetId, stack) {
			return new Promise(resolve => {
				rl.close();
				let start = Date.now();
				console.log("Executing Change Set");
				resolve(service.executeChangeSet({
					ChangeSetName: changeSetId
				}).then(data => {
					const waitConfig = {
					    minDelay: 5, // Minimum delay in seconds between checks
					    maxDelay: 30, // Maximum delay in seconds between checks
					    maxWaitTime: 900 // Maximum time in seconds to wait for the operation to complete
					};
					return waitUntilChangeSetCreateComplete(
	            		{ service, maxWaitTime: waitConfig.maxWaitTime }, // Waiter configuration
	            		{ ChangeSetName: changeSetId, StackName: stack } // Command input
	        		);					
					
				}).catch(err => {
					return service.describeStackEvents({
						StackName: stack
					}).then(data => {
						let messages = [];
						let addLinkMessage = false;
						for (let i = 0; i < data.StackEvents.length; i++) {
							let r = data.StackEvents[i];
							if (r.ResourceStatusReason && new Date(r.Timestamp).valueOf() > start && r.ResourceStatus && r.ResourceStatus.match(/(?:_FAILED|_ROLLBACK|DELETE_)/)) {
								addLinkMessage = addLinkMessage || !!r.ResourceStatusReason.match(/no export named/i);
								messages.push(`${r.LogicalResourceId} - ${r.ResourceStatusReason}`);
							}
						}
						if (addLinkMessage) {
							messages.push("Linked Stack(s) are missing export values.  Are your stack names correct?")
						}
						if (messages.length == 0) {
							messages.push("Unknown Error")
						}

						throw {
							StatusReason: messages.join(". ")
						};
					});
				}));
			});
		},
		getStackErrorStatus: function(stack, start) {
			return service.describeStackEvents({
				StackName: stack
			}).then(data => {
				let messages = [];
				let addLinkMessage = false;
				for (let i = 0; i < data.StackEvents.length; i++) {
					let r = data.StackEvents[i];
					if (r.ResourceStatusReason && new Date(r.Timestamp).valueOf() > start && r.ResourceStatus && r.ResourceStatus.match(/(?:_FAILED|_ROLLBACK|DELETE_)/)) {
						addLinkMessage = addLinkMessage || !!r.ResourceStatusReason.match(/no export named/i);
						messages.push(`${r.LogicalResourceId} - ${r.ResourceStatusReason}`);
					}
				}
				if (addLinkMessage) {
					messages.push("Linked Stack(s) are missing export values.  Are your stack names correct?")
				}
				if (messages.length == 0) {
					messages.push("Unknown Error")
				}

				throw {
					StatusReason: messages.join(". ")
				};
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
			let start = Date.now();
			return service.updateStack(Object.assign({
				StackName: stack,
				TemplateURL: file,
				Capabilities: [
					"CAPABILITY_IAM",
				]
			}, updateOpts)).then(data => {
				service.api.waiters["stackUpdateComplete"].delay = 10;
				return this.waitFor("stackUpdateComplete", {
					StackName: stack
				});
			}).catch(err => {
				if (err.message.match(/^Stack.*does not exist/)) {
					return this.createStack(stack, file, updateOpts.Parameters, opts, true, true);
				} else {
					return service.describeStackEvents({
						StackName: stack
					}).then(data => {
						let messages = [];
						let addLinkMessage = false;
						for (let i = 0; i < data.StackEvents.length; i++) {
							let r = data.StackEvents[i];
							if (r.ResourceStatusReason && new Date(r.Timestamp).valueOf() > start && r.ResourceStatus && r.ResourceStatus.match(/(?:_FAILED|_ROLLBACK)/)) {
								addLinkMessage = addLinkMessage || !!r.ResourceStatusReason.match(/no export named/i);
								messages.push(`${r.LogicalResourceId} - ${r.ResourceStatusReason}`);
							}
						}
						if (addLinkMessage) {
							messages.push("Linked Stack(s) are missing export values.  Are your stack names correct?")
						}
						if (messages.length == 0) {
							messages.push("Unknown Error")
						}
						throw {
							StatusReason: messages.join(", ")
						};
					}).catch((err2) => {
						if (err2.StatusReason) {
							throw new Error(err2.StatusReason)
						} else {
							throw err;
						}
					});
				}
			});
		},
		describeStackResources: function(stack) {
			return service.describeStackResources({
				StackName: stack
			}).then(data => data.StackResources);
		},
		waitFor: function(action, params) {
			return service.waitFor(action, params);
		},
		createStack: async function createStack(name, template, paramaters = [], waitFor = true, describeStack = true) {
			let templateBody;
			if (typeof template === "string") {
				templateBody = "TemplateURL";
			} else {
				templateBody = "TemplateBody";
				template = JSON.stringify(template);
			}

			let createInfo = {};
			let createStart = Date.now();
			let promise = service.createStack({
				StackName: name,
				Capabilities: [
					"CAPABILITY_IAM"
				],
				OnFailure: "DELETE",
				[templateBody]: template,
				Parameters: paramaters
			}).then(stackInfo => {
				createInfo = stackInfo;
				return stackInfo;
			});
			if (waitFor || describeStack) {
				service.api.waiters["stackCreateComplete"].delay = 10;
				promise = promise
				.then(() => {
					const waitConfig = {
					    minDelay: 5, // Minimum delay in seconds between checks
					    maxDelay: 30, // Maximum delay in seconds between checks
					    maxWaitTime: 300 // Maximum time in seconds to wait for the operation to complete
					};
					return waitUntilChangeSetCreateComplete(
	            		{ service, maxWaitTime: waitConfig.maxWaitTime }, // Waiter configuration
	            		{ StackName: name } // Command input
	        		);									

				}).then(waitdata => ({
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
			return promise.catch((waitErr) => this.getStackErrorStatus(createInfo.StackId || name, createStart).catch(err => {
				if (err.StatusReason)
					throw new Error(err.StatusReason);
				else
					throw waitErr;
			}));
		}
	};
};
