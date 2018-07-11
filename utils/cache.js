let cache = {};
const expireDefault = 1 * 60 * 1000;

async function refresh(name, func) {
	let c = cache[name];
	if (c.onRefresh) { //someone is already trying to refresh it
		return new Promise((resolve, reject) => {
			c.onRefresh.listeners.push(() => {
				resolve(true);
			});
		});
	} else {
		c.onRefresh = {
			start: Date.now(),
			listeners: []
		};
		let v = await func(name);

		if (c.onRefresh && c.onRefresh.listeners.length) {
			c.onRefresh.listeners.map(f => f());
		}
		cache[name] = {
			ts: Date.now(),
			v: v,
			shouldRefresh: false,
			refreshing: []
		};
		return true;
	}
}

function getFromCache(name, expireDuration, overlyStaleDuration) {
	//Does it exist
	if (!(name in cache)) {
		cache[name] = {
			ts: 0,
			v: null,
			shouldRefresh: false,
			refreshing: []
		};
		return false;
	} else {
		let c = cache[name];
		let staleDuration = Date.now() - c.ts;

		let isExpired = c.ts === 0 || staleDuration >= expireDuration;
		console.log(isExpired, " is expired");
		if (isExpired) {
			let isOverlyStale = c.ts === 0 || staleDuration >= overlyStaleDuration;
			console.log(isOverlyStale, " is overlystale");
			if (isOverlyStale) {
				return false;
			} else {
				c.shouldRefresh = true;
				return c;
			}
		} else {
			return c
		}
	}
}

module.exports = {
	get: async function(name, func, expireDuration = minute, opts = {}) {
		opts = Object.assign({
			allowedStalePeriod: 0,
			maxRefreshWait: 10 * 1000
		}, opts);
		let overlyStaleDuration = expireDuration + opts.allowedStalePeriod;
		let r = getFromCache(name, expireDuration, overlyStaleDuration);

		//No cache
		if (r === false) {
			await refresh(name, func);
		} else if (r.shouldRefresh == true) { //It is there but getting stale
			refresh(name, func);
		}
		return cache[name].v;
	}
};
