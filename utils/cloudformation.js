module.exports = function() {
	let cf = {
		Resources: {}
	};
	return {
		dynamodb: require("./cloudformation/dynamodb"),
		extend: function(section, data) {
			if (!cf[section]) {
				cf[section] = {};
			}

			Object.assign(cf[section], data);
			return this;
		},
		add: function(resources) {
			Object.assign(cf.Resources, resources);
			return this;
		},
		export: () => {
			return cf
		}
	};
};
