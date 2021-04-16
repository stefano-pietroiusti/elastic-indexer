const { Client } = require('@elastic/elasticsearch');
const R = require('ramda');

const orgs = require('./data/organization.json');
const consumptions = require('./data/consumption.json');

require('env2')('./elastic_env.json');

const { user, password, esUrl, port, indexName } = process.env;
const idxName = indexName || 'tao-msp-clients';

console.time('new Client');
const client = new Client({
  node: `https://${user}:${password}@${esUrl}:${port}`
});
console.timeEnd('new Client');

/**
 * Ping ES server
 */
const ping = () => {
  client.ping({}, function(error) {
    if (error) {
      console.error('elasticsearch cluster is down!', error);
    } else {
      console.log('Everything is ok');
    }
  });
};

const genMockMspClient = (org, consumption) => {
  const mspIds = [
    'ac_314581ce-b79d-4e16-b8bb-905d5e667bf5',
    'ac_79da71e9-93e1-4a06-bae1-55af04f6d3b2',
    'ac_8d281cea-d5c7-46ed-bf6c-167bc1da0134',
    'ac_ccd5d2ce-8b63-4b31-a662-c8f5281a4b86',
    'ac_37fcda94-936a-4233-b440-5c2c7d184997'
  ];

  const getRandomInt = max => {
    return Math.floor(Math.random() * Math.floor(max));
  };

  const tidyOrgId = orgId => 'acc_' + orgId.replace('ip_', '');

  const orgData = R.pick(['name', 'status'], org);
  const consumptionData = R.pipe(
    R.pick(['authState', 'lastUpdatedDate']),
    R.mergeLeft({
      totalActivePaidUser: getRandomInt(1000),
      totalLicenses: getRandomInt(2000),
      availableLicenses: getRandomInt(100),
      totalSpend: getRandomInt(100000) - 0.1
    })
  )(consumption.office365);
  const created = R.pathOr(
    new Date().toISOString(),
    ['users', 0, 'created'],
    org
  );
  return R.mergeAll([
    { id: tidyOrgId(org.id) },
    { countryCode: R.propOr('us', 'countryCode', org)},
    orgData,
    {
      office365: consumptionData
    },
    {
      mspId: mspIds[getRandomInt(5)],
      email: R.pathOr(
        R.path(['invites', 0, 'email'], org),
        ['users', 0, 'email'],
        org
      ),
      created,
      lastUpdated: new Date(org._ts).toISOString()
    }
  ]);
};

const insertDocument = async mspClient => {
  try {
    const doc = await client.index({
      index: idxName,
      id: mspClient.id,
      body: mspClient
    });
    return doc.body;
  } catch (e) {
    console.error(mspClient.id, e);
    throw e;
  }
};

const indexToES = () => {
  console.log('org count', orgs.length);
  console.log('consumptions count', consumptions.length);
  for (let i = 0; i < orgs.length - 1; i++) {
    const org = orgs[i];
    const consumption = consumptions[i];
    insertDocument(genMockMspClient(org, consumption))
      .then(data => console.log(`indexing ${i}: ${org.id} is successful`))
      .catch(() => console.log(`indexing ${i}: ${org.id} is failed`));
  }
};

indexToES();

// promise API
// const result = await client.search({
//   index: 'my-index',
//   body: { foo: 'bar' }
// });

// callback API
// client.search(
//   {
//     index: 'my-index',
//     body: { foo: 'bar' }
//   },
//   (err, result) => {
//     if (err) console.log(err);
//   }
// );
