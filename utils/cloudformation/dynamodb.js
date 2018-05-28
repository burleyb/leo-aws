const autoscale = require("./autoscale");

module.exports = {
	table: function(logicalId, main, secondaryKeys = []) {
		let throughput = main.throughput;
		delete main.throughput;

		let shouldAutoscale = main.autoscale;
		delete main.autoscale;

		let attributes = {};
		Object.keys(main).forEach(name => {
			attributes[name] = {
				AttributeName: name,
				AttributeType: main[name]
			};
		});

		let cfSnippet = {
			[logicalId]: {
				Type: "AWS::DynamoDB::Table",
				Properties: {
					AttributeDefinitions: [{
						AttributeName: "id",
						AttributeType: "S"
					}],
					KeySchema: Object.keys(main).map((name, i) => {
						return {
							AttributeName: name,
							KeyType: i === 0 ? "HASH" : "RANGE"
						}
					}),
					ProvisionedThroughput: {
						"ReadCapacityUnits": throughput.read || 20,
						"WriteCapacityUnits": throughput.write || 20,
					}
				}
			}
		};

		function addScale(targetType, throughput, type) {
			let targetCapacity = throughput[`Target${type}Capacity`];
			if (typeof targetCapacity == "number") {
				targetCapacity = {
					TargetValue: targetCapacity
				};
			}
			let scalableTargetId = `${logicalId}${type}CapacityScalableTarget`;
			cfSnippet[scalableTargetId] = {
				Type: "AWS::ApplicationAutoScaling::ScalableTarget",
				Properties: {
					MaxCapacity: throughput[`Max${type}CapacityUnits`] || throughput[`${type}CapacityUnits`],
					MinCapacity: throughput[`Min${type}CapacityUnits`] || throughput[`${type}CapacityUnits`],
					ResourceId: {
						"Fn::Sub": `${targetType}/\${${logicalId}}`,
					},
					RoleARN: {
						"Fn::Sub": "${AutoScalingRole.Arn}"
					},
					"ScalableDimension": `dynamodb:${targetType}:${type}CapacityUnits`,
					"ServiceNamespace": "dynamodb"
				}
			};

			let policyId = `${logicalId}${type}AutoScalingPolicy`;
			cfSnippet[policyId] = {
				Type: "AWS::ApplicationAutoScaling::ScalingPolicy",
				Properties: {
					PolicyName: policyId,
					PolicyType: "TargetTrackingScaling",
					ScalingTargetId: {
						Ref: scalableTargetId
					},
					TargetTrackingScalingPolicyConfiguration: Object.assign({
						TargetValue: 70.0,
						PredefinedMetricSpecification: {
							PredefinedMetricType: `DynamoDB${type}CapacityUtilization`
						}
					}, targetCapacity)
				}
			};
		}

		let scaled = false;
		if (shouldAutoscale || throughput.TargetReadCapacity) {
			scaled = true;
			addScale("table", Object.assign({
				"MinReadCapacityUnits": throughput.read,
				"MaxReadCapacityUnits": throughput.read * 10,
				"TargetReadCapacity": 70,
			}, throughput), "Read");
		}
		if (shouldAutoscale || throughput.TargetWriteCapacity) {
			scaled = true;
			addScale("table", Object.assign({
				"MinWriteCapacityUnits": throughput.write,
				"MaxWriteCapacityUnits": throughput.write * 10,
				"TargetWriteCapacity": 70
			}, throughput), "Write");
		}

		if (scaled == true) {
			Object.assign(cfSnippet, autoscale.dynamodbrole());
		}
		return cfSnippet;
	}
};
