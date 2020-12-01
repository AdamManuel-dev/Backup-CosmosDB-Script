const { Dao } = require('./Dao');
const { Observable, from, defer } = require('rxjs');
const { take, tap } = require('rxjs/operators');

/**
 * Transforms a promise into an observable
 * @param prom Promise to transform into observable
 */
function fromPromise(prom) {
	const obs = defer(() => from(prom));
	return obs;
}

/**
 * Transforms an observable into a promise
 * @param obs Observable to transform into promise
 */
async function toPromise(obs) {
	return obs.pipe(take(1)).toPromise();
}

class TypeClient {
	constructor() {
		this.DAO = new Dao({
			secrets: {
				endpoint: 'xxx',
				masterKey: 'xxx',
				database: 'xxx'
			},
			databaseId: 'xxx',
			containerId: 'xxx'
		});
	}

	async getAllTypes() {
		await this.DAO.init();
		return this.DAO.query({
			query: 'SELECT * from c',
			parameters: [],
			partition: 'xxx'
		});
	}

	async getTypesNames() {
		await this.DAO.init();
		return this.DAO.query({
			query: 'SELECT DISTINCT VALUE c.id from c order by c.id asc',
			parameters: [],
			partition: 'xxx'
		});
	}

	async getTypeUpdates(unixTime = 0) {
		await this.DAO.init();
		const time = (unixTime - unixTime % 1000) / 1000;
		return this.DAO.query({
			query: 'SELECT c.id, c._ts from c where c._ts > @ts',
			parameters: [
				{
					name: '@ts',
					value: time
				}
			],
			partition: 'xxx'
		});
	}

	async createType(name, definition, project = 'xxx', src = undefined) {
		await this.DAO.init();
		const response = await this.DAO.create({
			id: name,
			definition,
			_partitionKey: project,
			...(!!src ? { src } : {})
		});
		return response.resource;
	}

	async updateType(name, definition, project = 'xxx') {
		await this.DAO.init();
		const response = await this.DAO.create({
			id: name,
			definition,
			_partitionKey: project
		});
		return response.resource;
	}

	async textQuery(query, project = 'xxx') {
		await this.DAO.init();
		const response = await this.DAO.query({
			query,
			parameters: [],
			partition: project
		});
		return response.map((item) => {
			const entries = Object.entries(item);
			return entries.filter(([ key, value ]) => !key.includes('_')).reduce((prev, cur) => {
				const value = { [cur[0]]: cur[1] };
				return { ...prev, ...value };
			}, {});
		});
	}

	async dirtyTextQuery(query, project = 'xxx') {
		await this.DAO.init();
		const response = await this.DAO.query({
			query,
			parameters: [],
			partition: project
		});
		return response;
	}

	async run() {
		const types = await this.getTypes();
	}
}

if (process.argv[2] === 'out') {
	const RUN = new TypeClient();
	RUN.run();
}

module.exports = {
	fromPromise,
	toPromise,
	TypeClient
};
