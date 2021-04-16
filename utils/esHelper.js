/**
 * Ping ES server
 */
const ping = (client) => {
  client.ping({}, function(error) {
    if (error) {
      console.error('elasticsearch cluster is down!', error);
    } else {
      console.log('Everything is ok');
    }
  });
};

module.exports = {
  ping
};
