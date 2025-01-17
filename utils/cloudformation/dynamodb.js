const autoscale = require("./autoscale");

module.exports = {
    table: function(logicalId, main, globalIndexes = {}) {
        let throughput = main.throughput;
        let isOnDemand = main.onDemand || false;
        delete main.onDemand;

        if (!isOnDemand) {
            throughput.read = throughput.read || throughput.ReadCapacityUnits;
            throughput.write = throughput.write || throughput.WriteCapacityUnits;
        }
        delete main.throughput;

        let shouldAutoscale = main.autoscale;
        delete main.autoscale;

        let stream = main.stream;
        delete main.stream;

        let attributes = {};
        Object.keys(main).forEach(name => {
            attributes[name] = {
                AttributeName: name,
                AttributeType: main[name]
            };
        });

        Object.keys(globalIndexes).forEach(key => {
            let gIndex = globalIndexes[key];
            Object.keys(gIndex).forEach(name => {
                if (["autoscale", "throughput", "projection"].indexOf(name) === -1) {
                    attributes[name] = {
                        AttributeName: name,
                        AttributeType: gIndex[name]
                    };
                }
            })
        });

        let cfSnippet = {
            [logicalId]: {
                Type: "AWS::DynamoDB::Table",
                Properties: {
                    AttributeDefinitions: Object.keys(attributes).map(name => attributes[name]),
                    KeySchema: Object.keys(main).map((name, i) => {
                        return {
                            AttributeName: name,
                            KeyType: i === 0 ? "HASH" : "RANGE"
                        }
                    })
                }
            }
        };

        // Add billing mode based on isOnDemand flag
        if (isOnDemand) {
            cfSnippet[logicalId].Properties.BillingMode = "PAY_PER_REQUEST";
        } else {
            cfSnippet[logicalId].Properties.BillingMode = "PROVISIONED";
            cfSnippet[logicalId].Properties.ProvisionedThroughput = {
                "ReadCapacityUnits": throughput.read || 20,
                "WriteCapacityUnits": throughput.write || 20,
            };
        }

        // Handle Global Secondary Indexes
        if (Object.keys(globalIndexes).length > 0) {
            cfSnippet[logicalId].Properties.GlobalSecondaryIndexes = Object.keys(globalIndexes).map(key => {
                let gIndex = globalIndexes[key];
                let gsiConfig = {
                    IndexName: key,
                    KeySchema: Object.keys(gIndex).filter(key => ["autoscale", "throughput", "projection"].indexOf(key) === -1).map((name, i) => {
                        return {
                            AttributeName: name,
                            KeyType: i === 0 ? "HASH" : "RANGE"
                        };
                    }),
                    Projection: {
                        ProjectionType: gIndex.projection || 'ALL'
                    }
                };

                if (!isOnDemand) {
                    if (gIndex.throughput) {
                        gIndex.throughput.read = gIndex.throughput.read || gIndex.throughput.ReadCapacityUnits;
                        gIndex.throughput.write = gIndex.throughput.write || gIndex.throughput.WriteCapacityUnits;
                    }
                    let gThroughput = Object.assign({
                        read: throughput.read || 20,
                        write: throughput.write || 20
                    }, gIndex.throughput || {});

                    gsiConfig.ProvisionedThroughput = {
                        "ReadCapacityUnits": gThroughput.read,
                        "WriteCapacityUnits": gThroughput.write
                    };
                }

                return gsiConfig;
            });
        }

        if (stream) {
            cfSnippet[logicalId].Properties.StreamSpecification = {
                StreamViewType: stream
            };
        }

        // Only add autoscaling if not using on-demand
        if (!isOnDemand) {
            let scaled = false;
            if (shouldAutoscale || throughput.TargetReadCapacity) {
                scaled = true;
                addScale(`table/\${${logicalId}}`, "table", Object.assign({
                    "MinReadCapacityUnits": throughput.read,
                    "MaxReadCapacityUnits": throughput.read * 10,
                    "TargetReadCapacity": 70,
                }, throughput), "Read");
            }
            if (shouldAutoscale || throughput.TargetWriteCapacity) {
                scaled = true;
                addScale(`table/\${${logicalId}}`, "table", Object.assign({
                    "MinWriteCapacityUnits": throughput.write,
                    "MaxWriteCapacityUnits": throughput.write * 10,
                    "TargetWriteCapacity": 70
                }, throughput), "Write");
            }

            Object.keys(globalIndexes).forEach(key => {
                let gIndex = globalIndexes[key];
                let shouldAutoscale = gIndex.autoscale;
                let gThroughput = Object.assign({
                    read: throughput.read || 20,
                    write: throughput.write || 20
                }, gIndex.throughput || {});

                if (shouldAutoscale || gThroughput.TargetReadCapacity) {
                    scaled = true;
                    addScale(`table/\${${logicalId}}/index/${key}`, "index", Object.assign({
                        "MinReadCapacityUnits": gThroughput.read,
                        "MaxReadCapacityUnits": gThroughput.read * 10,
                        "TargetReadCapacity": 70,
                    }, gThroughput), "Read", key);
                }
                if (shouldAutoscale || gThroughput.TargetWriteCapacity) {
                    scaled = true;
                    addScale(`table/\${${logicalId}}/index/${key}`, "index", Object.assign({
                        "MinWriteCapacityUnits": gThroughput.write,
                        "MaxWriteCapacityUnits": gThroughput.write * 10,
                        "TargetWriteCapacity": 70
                    }, gThroughput), "Write", key);
                }
            });

            if (scaled == true) {
                Object.assign(cfSnippet, autoscale.dynamodbrole());
            }
        }

        return cfSnippet;
    },

    tableStaticName: function(logicalId, main, globalIndexes = {}) {
        let snippet = this.table(logicalId, main, globalIndexes);

        snippet[logicalId].Properties.TableName = {
            "Fn::Sub": "${AWS::StackName}-" + logicalId
        }

        return snippet;
    }
};

function addScale(target, targetType, throughput, type, name = "") {
    let targetCapacity = throughput[`Target${type}Capacity`];
    if (typeof targetCapacity == "number") {
        targetCapacity = {
            TargetValue: targetCapacity
        };
    }
    let scalableTargetId = `${logicalId}${name}${type}CapacityScalableTarget`;
    cfSnippet[scalableTargetId] = {
        Type: "AWS::ApplicationAutoScaling::ScalableTarget",
        Properties: {
            MaxCapacity: throughput[`Max${type}CapacityUnits`] || throughput[`${type}CapacityUnits`],
            MinCapacity: throughput[`Min${type}CapacityUnits`] || throughput[`${type}CapacityUnits`],
            ResourceId: {
                "Fn::Sub": `${target}`,
            },
            RoleARN: {
                "Fn::Sub": "${AutoScalingRole.Arn}"
            },
            "ScalableDimension": `dynamodb:${targetType}:${type}CapacityUnits`,
            "ServiceNamespace": "dynamodb"
        }
    };

    let policyId = `${logicalId}${name}${type}AutoScalingPolicy`;
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
