'use strict';
const cache = require("../../utils/cache.js");
const assert = require('assert');

describe.only('Cache', function() {
	it('should cache the value', (done) => {
		let count = 0;

		let value = () => {
			return Promise.resolve(++count);
		}
		cache.get("mycachename", value, 100).then(v => {
			expect(v).toEqual(1);
			cache.get("mycachename", value, 100).then(v => {
				expect(v).toEqual(1);
				setTimeout(() => {
					cache.get("mycachename", value, 100).then(v => {
						expect(v).toEqual(1);
						setTimeout(() => {
							cache.get("mycachename", value, 100).then(v => {
								expect(v).toEqual(2);
								done();
							});
						}, 50);
					});
				}, 50);
			});
		});

	});
});
