const merge = require("lodash.merge");
module.exports = function(data) {
	let cf = {};
	if (data) {
		merge(cf, data);
	}
	if (!('Resources' in cf)) {
		cf.Resources = {};
	}
	return {
		dynamodb: require("./cloudformation/dynamodb"),
		extend: function(data) {
			merge(cf, data);
			return this;
		},
		add: function(resources) {
			merge(cf.Resources, resources);
			return this;
		},
		export: () => {
			return cf
		}
	};
};