const { Client } = require('@elastic/elasticsearch');
const R = require('ramda');
const path = require('path');
const fs = require('fs');

const { ping } = require('../utils/esHelper');

require('env2')('./elastic_env.json');

const { user, password, esUrl, port, indexName } = process.env;

console.log(user, password, esUrl, port);

console.time('new Client');
const client = new Client({
  node: `https://${user}:${password}@${esUrl}:${port}`
});
console.timeEnd('new Client');

/**
 * Get files from provide folder and sub-folders
 */
const getFilesFromDir = (onFileFound, ...subFolder) => {
  // joining path of directory
  const directoryPath = path.join(...subFolder);
  const files = fs.readdirSync(directoryPath);

  //listing all files
  files.forEach(function(file) {
    // Do whatever you want to do with the file
    // console.log(file);
    if (fs.lstatSync(path.join(...subFolder, file)).isDirectory()) {
      getFilesFromDir(onFileFound, ...subFolder, file);
    } else {
      if (onFileFound) {
        onFileFound(path.join(...subFolder, file));
      }
    }
  });
};

const filePostfix = {
  office365DashboardView: 'office365DashboardView',
  office365RecommendationsView: 'office365RecommendationsView',
  office365UsersView: 'office365UsersView'
};

/**
 * Get OrgId from filePath.
 * eg:  /elastic-indexer/consumption/data/ip_00172f6b-0464-4fce-92bd-01a8a4dc32af/5922736e-c304-3683-b6dd-3e683bb6fe09-office365DashboardView.json
 * return ip_00172f6b-0464-4fce-92bd-01a8a4dc32af
 * @param filePath
 * @returns {*|string}
 */
const getOrgId = filePath => {
  const paths = filePath.split('/');
  return paths[paths.length - 2];
};

/**
 * Get unique view files into an object with three maps
 * @returns {
 *    {
 *    office365DashboardViewFinal: Map<any, any>,
 *    office365UsersViewFinal: Map<any, any>,
 *    office365RecommendationsViewFinal: Map<any, any>
 *    }
 *  }
 */
const getUniqueViews = () => {
  /**
   * View files name arrays
   * @type {Array}
   */
  const office365DashboardView = [];
  const office365RecommendationsView = [];
  const office365UsersView = [];

  const processFileName = fileName => {
    // console.log(fileName);
    if (fileName.includes(filePostfix.office365DashboardView)) {
      office365DashboardView.push(fileName);
      return;
    }
    if (fileName.includes(filePostfix.office365RecommendationsView)) {
      office365RecommendationsView.push(fileName);
      return;
    }
    if (fileName.includes(filePostfix.office365UsersView)) {
      office365UsersView.push(fileName);
    }
  };

  /**
   *
   * @param fileNames Array of file names
   */
  const processUniqueFiles = fileNames => {
    const results = new Map();
    fileNames.forEach(fileName => {
      const orgId = getOrgId(fileName);
      if (!results.has(orgId)) {
        results.set(orgId, fileName);
      }
    });
    return results;
  };

  getFilesFromDir(processFileName, __dirname, 'data');

  // console.log(
  //   'office365DashboardView.length',
  //   office365DashboardView.length,
  //   // office365DashboardView[0]
  // );
  // console.log(
  //   'office365RecommendationsView.length',
  //   office365RecommendationsView.length,
  //   // office365RecommendationsView[0]
  // );
  // console.log(
  //   'office365UsersView.length',
  //   office365UsersView.length,
  //   // office365UsersView[0]
  // );

  const office365DashboardViewFinal = processUniqueFiles(
    office365DashboardView
  );
  console.log('office365DashboardViewFinal', office365DashboardViewFinal.size);

  const office365RecommendationsViewFinal = processUniqueFiles(
    office365RecommendationsView
  );
  console.log(
    'office365RecommendationsViewFinal',
    office365RecommendationsViewFinal.size
  );

  const office365UsersViewFinal = processUniqueFiles(office365UsersView);
  console.log('office365UsersViewFinal', office365UsersViewFinal.size);

  return {
    office365DashboardViewFinal,
    office365RecommendationsViewFinal,
    office365UsersViewFinal
  };
};

const mspClientId = 'ac_e5d168d0-d9df-457d-94bf-8384a167cc91';

/**
 * Insert consumption doc to ES V1
 * @param office365DashboardView
 * @param office365RecommendationsView
 * @param office365UsersView
 * @returns {Promise<any>}
 */
const insertDocument = async ({
  office365DashboardView,
  office365RecommendationsView,
  office365UsersView
}) => {
  const idxName = indexName || 'tao-msp-consumptions';

  const office365DashboardViewJson = require(office365DashboardView);
  const office365RecommendationsViewJson = require(office365RecommendationsView);
  const office365UsersViewJson = require(office365UsersView);

  // console.log(
  //   `${office365DashboardViewJson.currency}, ${
  //     office365RecommendationsViewJson.id
  //   }, ${office365UsersViewJson.users.length}`
  // );

  const doc = {
    id: office365RecommendationsViewJson.id,
    mspClientId,
    office365UsersView: office365UsersViewJson,
    office365RecommendationsView: office365RecommendationsViewJson,
    office365DashboardView: office365DashboardViewJson
  };

  try {
    const result = await client.index({
      index: idxName,
      id: doc.id,
      body: doc
    });
    return result.body;
  } catch (e) {
    console.error(doc.id, e);
    throw e;
  }
};

/**
 * Insert consumption doc to ES V2
 * @param office365DashboardView
 * @param office365RecommendationsView
 * @param office365UsersView
 * @returns {Promise<any>}
 */
const insertDocumentV2 = async ({
  office365DashboardView,
  office365RecommendationsView,
  office365UsersView
}) => {
  const idxName = indexName || 'tao-msp-consumptions-v2';

  const { birthtime } = fs.lstatSync(office365DashboardView);
  // console.log(`birthtime: ${birthtime.toISOString()}`);

  const office365DashboardViewJson = require(office365DashboardView);
  const office365RecommendationsViewJson = require(office365RecommendationsView);
  const office365UsersViewJson = require(office365UsersView);

  // console.log(
  //   `${office365DashboardViewJson.currency}, ${
  //     office365RecommendationsViewJson.id
  //   }, ${office365UsersViewJson.users.length}`
  // );

  const { currency, users } = office365UsersViewJson;

  const doc = {
    id: office365RecommendationsViewJson.id,
    mspClientId,
    office365: {
      createdDate: birthtime.toISOString(),
      currency,
      users,
      recommendations: R.omit(['id', 'currency'], office365RecommendationsViewJson),
      dashboard: R.omit(['currency'], office365DashboardViewJson)
    }
  };

  try {
    const result = await client.index({
      index: idxName,
      id: doc.id,
      body: doc
    });
    return result.body;
  } catch (e) {
    console.error(doc.id, e);
    throw e;
  }
};

/**
 * Deduplicate all three views from the data S3 folder.
 * So that each org only have 3 files:
 * office365DashboardView, office365RecommendationsView and office365UsersView
 * then put these view to ES index
 */
const genConsumptionIndex = () => {
  const result = getUniqueViews();

  result.office365DashboardViewFinal.forEach((office365DashboardView, key) => {
    const office365RecommendationsView = result.office365RecommendationsViewFinal.get(
      key
    );
    const office365UsersView = result.office365UsersViewFinal.get(key);

    // console.log(`${key}: ${office365DashboardView},  ${office365RecommendationsView}, ${office365UsersView}`);

    insertDocumentV2({
      office365DashboardView,
      office365RecommendationsView,
      office365UsersView
    })
      .then(data => console.log(`indexing: ${key} is successful`))
      .catch(() => console.log(`indexing: ${key} is failed`));
  });
};

genConsumptionIndex();

// ping(client);
