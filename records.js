const { TypeClient, fromPromise, toPromise } = require('./shared/TypeClient');
const _ = require('lodash');
const fs = require('fs-extra');

async function run() {
	const client = new TypeClient();
	async function listRecords(skip = 0, limit = 1000, partitionKey = 'Appointment') {
		const query = `SELECT * FROM c OFFSET ${skip} LIMIT ${limit}`;
		return client.textQuery(query, partitionKey).catch((err) => {
			console.log('⛔ ERROR:', err);
		});
	}
	async function listTotal(partitionKey = 'Appointment') {
		const query = `SELECT COUNT(1) as total FROM c `;
		return (await client.textQuery(query, partitionKey).catch((err) => {
			console.log('⛔ ERROR:', err);
		}))[0].total;
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//  USE `SELECT DISTINCT c._partitionKey as partition FROM c`                                               //
	//                                                                                                          //
	//  Then map to an array of strings                                                                         //
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	const allPartitions = [
		{
			partition: 'Residence'
		},
		{
			partition: 'Form'
		},
		{
			partition: 'icapture'
		},
		{
			partition: 'Role'
		},
		{
			partition: 'Permission'
		},
		{
			partition: 'Lead'
		},
		{
			partition: 'Appointment'
		},
		{
			partition: 'Interest'
		},
		{
			partition: 'Aggregate'
		},
		{
			partition: 'Scorecard'
		},
		{
			partition: 'FormDraft'
		},
		{
			partition: 'MarketSharp'
		},
		{
			partition: 'LeadMarketsharp'
		},
		{
			partition: 'AppointmentMarketsharp'
		},
		{
			partition: 'Measure'
		},
		{
			partition: 'ScreenShot'
		},
		{
			partition: 'Order'
		},
		{
			partition: 'User'
		},
		{
			partition: 'Twillio'
		},
		{
			partition: 'Timeclock'
		},
		{
			partition: 'Payment'
		},
		{
			partition: 'userlocation'
		},
		{
			partition: 'workflow'
		},
		{
			partition: 'id'
		},
		{
			partition: 'Contract'
		},
		{
			partition: 'Corporation'
		},
		{
			partition: 'Invoice'
		},
		{
			partition: 'Message'
		},
		{
			partition: 'PaidTimeOff'
		},
		{
			partition: 'PayrollTotals'
		},
		{
			partition: 'Phonecall'
		},
		{
			partition: 'Product'
		},
		{
			partition: 'Task'
		},
		{
			partition: 'Timeoff'
		},
		{
			partition: 'UserLocation'
		},
		{
			partition: 'botTask'
		},
		{
			partition: 'Customers'
		},
		{
			partition: 'Customer'
		},
		{
			partition: 'BugReport'
		},
		{
			partition: 'FormResponse'
		},
		{
			partition: 'UserAggregate'
		},
		{
			partition: 'LeadAggregate'
		},
		{
			partition: 'Note'
		},
		{
			partition: 'workerUser'
		},
		{
			partition: null
		},
		{},
		{
			partition: 'UserActivity|anonymous'
		},
		{
			partition: 'LeadActivity|dbda5a27-b0c5-46e8-a4c0-ea64662a47c4'
		},
		{
			partition: 'UserActivity|4c16a94e-2a4a-41cc-b81e-832aac0d02a0'
		},
		{
			partition: 'action'
		},
		{
			partition: 'Residenceid'
		}
	];
	const partitions = allPartitions.map((i) => i.partition).filter((i) => !!i);

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Or use this hardcoded partitions                                                                         //
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// const partitions = [ 'Customer', 'Residence' ];

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//                                                                                                          //
	//   Below runs the parallel requests                                                                       //
	//                                                                                                          //
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	if (fs.existsSync('./output')) {
		fs.removeSync('./output');
	}
	fs.mkdirSync('./output', {
		recursive: true
	});
	partitions.map(async (partition) => {
		const total = await listTotal(partition);
		const totalQueries = 15;
		const pageSize = 1 + (total - total % totalQueries) / totalQueries;
		const response = _.repeat(' _', totalQueries).split(' _');
		response.pop();
		const skips = response.map((__, i) => {
			return pageSize * i;
		});
		const requests = await Promise.all(skips.map(async (skip) => listRecords(skip, pageSize, partition)));

		// Logging Status
		let ttl = 0;
		requests.forEach((i, j) => {
			ttl += i.length;
			console.log(i.length, i.length * j);
		});
		console.timeLog('query', ttl);

		// Reducing to single array
		const all = _.flatten(requests);

		// Verifying total length
		console.log(all.length);

		const path = `${process.cwd()}/output/${partition}.json`;

		fs.writeJSONSync(path.replace(/\|/g, '_').replace(/\-/g, '_'), all);

		console.log(`✅ Saved to "${path}" with all ${partition} records!`);
	});

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//                                                                                                          //
	//         FIN                                                                                              //
	//                                                                                                          //
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
}

run();
