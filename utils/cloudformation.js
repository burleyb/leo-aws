module.exports = function() {
	let cf = {};
	return {
		dynamodb: require("./cloudformation/dynamodb"),
		extend: function(resources) {
			Object.assign(cf, resources);
			return this;
		},
		add: function(resources) {
			Object.assign(cf, resources);
			return this;
		},
		export: () => {
			return cf
		}
	};
};
