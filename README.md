# ElasticSearch Organization and Consumption Mock Data Indexer

## environment file - elastic_env.json

```json
{
  "user": "user",
  "password": "password",
  "esUrl": "xxx.us-west-2.aws.found.io",
  "port": 9243
}
```
# How to

### generate tao-msp-clients mock data

```javascript
node ./index.js
```

### generate tao-msp-consumptions mock data

Make sure you download the consumption data file from S3 [bucket](https://s3.console.aws.amazon.com/s3/buckets/accordo-consumption/prod/?region=us-west-2&tab=overview)

```
node ./consumption/index.js
```



## Requirements

* [@elastic/elasticsearch](https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html)
