'use strict';

require('leo-config').bootstrap({
	_global: {
		leoaws: {
			region: 'us-east-1'
		}
	}
});

const leoaws = require('./../../');
const assert = require('assert');
const uniqid = require('uniqid');
const moment = require('moment');
const ls = require('leo-streams');
const stream = require('stream');

const entityTable = 'order-test-entity';
const settingsTable = 'TestBus-LeoSettings-YHQHOKWR337E';
const dynamodb = [
	{
		id: 'qatest1',
		uniqid: uniqid(),
		newuniqid: uniqid()
	},
	{
		id: 'qatest2',
		uniqid: uniqid(),
		newuniqid: uniqid()
	},
	{
		id: 'qatest3',
		uniqid: uniqid(),
		newuniqid: uniqid()
	}
];

describe('DynamoDB', function() {
	// beforeAll(() => {
	// 	console.log('Preparing to run tests');
	// });
	// afterAll(async () => {});
	//
	it('has the docClient service', () => {
		assert(leoaws.dynamodb._service);
	});

	it('All functions have tests', () => {
		assert(Object.keys(leoaws.dynamodb).length === 14);
	});

	test('Put', async () => {
		let result = await leoaws.dynamodb.put(settingsTable, dynamodb[0].id, { id: dynamodb[0].id, uniqid: dynamodb[0].uniqid });
		expect(result).toBe(true);
	});

	test('Merge', async () => {
		let result = await leoaws.dynamodb.merge(settingsTable, dynamodb[0].id, { newuniqid: dynamodb[0].newuniqid });
		expect(result).toBe(true);
	});

	test('Get', async () => {
		let result = await leoaws.dynamodb.get(settingsTable, dynamodb[0].id);

		expect(result.id).toBe(dynamodb[0].id);
		expect(result.uniqid).toBe(dynamodb[0].uniqid);
		expect(result.newuniqid).toBe(dynamodb[0].newuniqid);
	});

	test('Update', async () => {
		// update the uniqid to a new one
		await leoaws.dynamodb.update(settingsTable, dynamodb[0].id, { uniqid: dynamodb[1].uniqid }, '').then(data => {
			expect(data).toBe(true);
		});
	});

	test('MultiUpdate', async () => {
		let multiUpdate = [];
		dynamodb.forEach(item => {
			multiUpdate.push({
				table: settingsTable,
				key: item.id,
				set: {
					uniqid: item.uniqid,
					newuniqid: item.newuniqid
				}
			});
		});

		await leoaws.dynamodb.updateMulti(multiUpdate).then(data => {
			expect(data).toEqual([true, true, true]);
		}).catch(err => {
			assert(false);
		});
	});

	test('Scan', () => {
		leoaws.dynamodb.scan(settingsTable).then(data => {
			// this is only a few items in the table, but items that should exist.
			let requireItems = ['Leo_cron_last_shutdown_time', 'healthSNS_data', 'bus_to_s3', dynamodb[0].id, dynamodb[1].id, dynamodb[2].id];
			data.forEach(item => {
				expect(requireItems.indexOf(item.id)).not.toBe(false);
			});
		}).catch(err => {
			assert(false);
		});
	});

	test('Query', () => {
		leoaws.dynamodb.query({
			TableName: entityTable,
			KeyConditionExpression: `#partition = :partition and #id = :id`,
			ExpressionAttributeNames: {
				"#partition": "partition",
				"#id": "id"
			},
			ExpressionAttributeValues: {
				":partition": 'order-9',
				":id": "369"
			},
			Limit: 1
		}).then(data => {
			if (Object.keys(data[0].data).length) {
				assert(true);
			} else {
				assert(false);
			}
		}).catch(err => {
			assert(false);
		});
	});

	describe('SmartQuery', () => {
		test('SmartQuery with limit 5', () => {
			smartQuery(5).then(data => {
				assert(data._stats.mb = 1);
				assert(data._stats.count = 5);
			});
		});
		test('SmartQuery with count 5', () => {
			smartQuery(null, 5).then(data => {
				assert(data._stats.mb = 1);
				assert(data._stats.count = 5);
			});
		});
		test('SmartQuery with Limit 5, count 10', () => {
			smartQuery(5, 10).then(data => {
				assert(data._stats.mb = 2);
				assert(data._stats.count = 10);
			});
		});
		test('SmartQuery with Limit 10, count 5', () => {
			smartQuery(10, 5).then(data => {
				assert(data._stats.mb = 1);
				assert(data._stats.count = 10);
			});
		});
	});
	// @todo smartQuery with a scan

	test('BatchGetTable', () => {

		let keys = [279, 289, 369, 1479, 1529].map(id => {
			return {
				partition: 'order-' + (id % 10),
				id: id
			};
		});

		leoaws.dynamodb.batchGetTable(entityTable, keys).then(data => {
			assert(data.length === keys.length);
		}).catch(() => {
			assert(false);
		});
	});

	test('BatchGetHashkey', () => {
		let ids = dynamodb.map(item => {
			return item.id;
		});
		leoaws.dynamodb.batchGetHashkey(settingsTable, 'id', ids).then(data => {
			dynamodb.forEach(item => {
				expect(data[item.id]).toEqual(item);
			});
			expect(Object.keys(data).length).toEqual(dynamodb.length);
		}).catch((err) => {
			assert(false);
		});
	});

	test('writeToTableInChunks', () => {
		let stream = leoaws.dynamodb.writeToTableInChunks(entityTable);

		for (let i = 1; i <= 10; i++) {
			stream.put({
				partition: 'qatest-' + i,
				id: moment.now(),
				data: JSON.stringify({ "uniqid": uniqid() }),
				entity: 'qatest'
			});
		}

		stream.end((err) => {
			if (err) {
				assert(false);
			} else {
				assert(true);
			}
		});
	});

	test('batchTableWrite', () => {
		let records = [];
		for (let i = 1; i <= 10; i++) {
			records.push({
				PutRequest: {
					Item: {
						partition: 'qatest-' + i,
						id: moment.now(),
						data: JSON.stringify({ "uniqid": uniqid() }),
						entity: 'batchTableWrite'
					}
				}
			});
		}

		leoaws.dynamodb.batchTableWrite(entityTable, records).then(() => {
			assert(true);
		}).catch(() => {
			assert(false);
		});
	});

	test('streamToTable', async (done) => {

		let transform = ls.through((obj, done) => {
			// console.log('obj', obj);
			done(null, obj);
		});

		for (let i = 1; i <= 10; i++) {
			transform.write({
				partition: 'streamToTable-' + i,
				id: moment.now(),
				data: JSON.stringify({ "uniqid": uniqid() }),
				entity: 'streamToTable'
			});
		}

		transform.end();

		ls.pipe(transform
			, leoaws.dynamodb.streamToTable(entityTable)
			, (err, data) => {
				if (err) {
					assert(false);
				} else {
					assert(true);
				}
				done();
			});
	});

	test('testingDelete`', async (done) => {
		let settingsStream = leoaws.dynamodb.writeToTableInChunks(settingsTable);

		dynamodb.forEach(item => {
			settingsStream.delete({ id: item.id });
		});
		settingsStream.end((err) => {
			if (err) {
				console.log('error while deleting');
				assert(false);
			} else {
				console.log('cleaned up settings table');
				assert(true);
			}
			done();
		});

		// delete records from EntityTable
		let entityStream = leoaws.dynamodb.writeToTableInChunks(entityTable);

		for (let i = 1; i <= 10; i++) {
			await findAndDelete(i, entityStream);
		}

		entityStream.end((err) => {
			console.log('cleaned up entity table');
			if (err) {
				assert(false);
			} else {
				assert(true);
			}
			done();
		});
	});
});

async function findAndDelete(i, stream) {
	return new Promise((resolve, reject) => {
		// query for all of the test records we inserted, and delete them
		leoaws.dynamodb.query({
			TableName: entityTable,
			KeyConditionExpression: `#partition = :partition`,
			ExpressionAttributeNames: {
				"#partition": "partition"
			},
			ExpressionAttributeValues: {
				":partition": `qatest-${i}`
			}
		}).then(data => {
			data.forEach(item => {
				stream.delete({
					partition: item.partition,
					id: item.id
				});
			});
			resolve('done');
		}).catch(err => {
			assert(false);
			stream.end((err) => { });
			reject("Error");
		});
	});
}

function smartQuery(limit = null, count = null) {
	let params = {
		TableName: entityTable,
		KeyConditionExpression: `#partition = :partition`,
		ExpressionAttributeNames: {
			"#partition": "partition"
		},
		ExpressionAttributeValues: {
			":partition": 'order-9'
		}
	};

	if (limit != undefined) {
		params.Limit = limit;
	}

	let configuration = {};
	if (count != undefined) {
		configuration.count = count;
	}

	return new Promise((resolve, reject) => {
		leoaws.dynamodb.smartQuery(params, configuration)
			.then(data => {
				resolve(data);
			})
			.catch(err => {
				assert(false);
				reject(err);
			});
	});
}

// example for begins_with, contains, etc…
// let result = await leoaws.dynamodb.scan({
// 	TableName: config.aggregationTableName,
// 	FilterExpression: 'begins_with(#id, :id)',
// 	ExpressionAttributeNames: {
// 		"#id": "id"
// 	},
// 	ExpressionAttributeValues: {
// 		":id": 'weather'
// 	}
