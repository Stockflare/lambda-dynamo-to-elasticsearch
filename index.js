// dependencies
var aws = require('aws-sdk');
var _ = require('underscore');
var path = require('path');
var elasticsearch = require('elasticsearch');
var http_aws_es = require('http-aws-es');
var when = require('when');
var table = 'stock';
var region = 'us-east-1';
var es_domain = 'stockflare-staging';

exports.handler = function(event, context) {


  //  Log out the entire invocation
  // console.log(JSON.stringify(event, null, 2));
  var aws_es = new aws.ES({
    region: region
  });


  when.promise(function(resolve, reject, notify){
    aws_es.describeElasticsearchDomains({
      DomainNames: [ es_domain ]
    }, function(err, data){
      if (err) {
        console.log("describeElasticsearchDomains error:", err, data);
        reject(err);
      } else {
        resolve({domain: _.first(data.DomainStatusList)});
      }
    });
  }).then(function(result){
    // Create the Index if needed
    var promise = when.promise(function(resolve, reject, notify){
      var myCredentials = new aws.EnvironmentCredentials('AWS');
      var es = elasticsearch.Client({
        hosts: result.domain.Endpoint,
        connectionClass: http_aws_es,
        amazonES: {
          region: region,
          credentials: myCredentials
        }

      });

      es.indices.exists({
        index: table
      },function(err, response, status){
        console.log('Looking for Index');
        console.log(err, response, status);
        if (status == 200) {
          console.log('Index Exists');
          resolve({es: es, domain: result.domain});
        } else if (status == 404) {
          createIndex(es, table, function(){
            resolve({es: es, domain: result.domain});
          });
        } else {
          reject(err);
        }
      });
    });
    return promise;
  }).then(function(result){
    console.log('Index is ready');
    // Create promises for every record that needs to be processed
    var records = _.map(event.Records, function(record, index, all_records){
      return when.promise(function(resolve, reject, notify){
        // First get the record
        recordExists(result.es, table, record).then(function(exists){
          // Now create or update the record.
          return putRecord(result.es, table, record, exists);
        }).then(function(record){
          resolve(record);
        }, function(reason){
          console.log(reason);
          reject(reason);
        });
      });
    });

    return when.all(records);
  }).done(function(records){
    console.log("Processed all records");
    context.succeed("Successfully processed " + records.length + " records.");
  }, function(reason){
    context.fail("Failed to process records " + reason);
  });


};

var createIndex = function(es, table, callback) {
  console.log('createIndex', table);
  es.indices.create({
    index: table
  }, function(err, response, status){
    if (err) {
      console.log("Index could not be created", err, response, status);
    } else {
      console.log("Index has been created");
    }
  });

};

var recordExists = function(es, table, record) {
  return when.promise(function(resolve, reject, notify){

    es.get({
      index: table,
      id: record.dynamodb.NewImage.sic.S,
      type: '_all'
    }, function(err, response, status){
      if (status == 200) {
        console.log('Document Exists');
        resolve(true);
      } else if (status == 404) {
        resolve(false);
      } else {
        reject(err);
      }
    });
  });
};

var putRecord = function(es, table, record, exists) {
  console.log('putRecord:', record.dynamodb.NewImage.sic.S);
  return when.promise(function(resolve, reject, notify){
    var params = {
      index: table,
      id: record.dynamodb.NewImage.sic.S,
      body: esBody(record),
      type: 'stock',
      versionType: 'force',
      version: getPricingDate(record)
    };
    var handler = function(err, response, status) {
      if (status == 200 || status == 201) {
        console.log('Document written');
        resolve(record);
      } else {
        console.log(err, response, status);
        reject(err);
      }
    };

    if (exists) {
      params.body = {
        doc: esBody(record)
      };
      es.update(params, handler);
    } else {
      params.body = esBody(record);
      es.create(params, handler);
    }
  });
};


var esBody = function(record){
  var values = record.dynamodb.NewImage;
  var body = _.mapObject(values, function(val, key){
    var tuple = _.pairs(val)[0];
    var new_val = tuple[1];
    switch (tuple[0]) {
      case 'N':
      new_val = parseFloat(tuple[1]);
      break;
      case 'NULL':
      new_val = null;
      break;
    }
    return new_val;
  });
  return body;
};

var getPricingDate = function(record) {
  var date = new Date();
  date.setHours(0,0,0,0);
  var pricing_date = Math.round(date.getTime() / 1000);
  if (!_.isUndefined(record.dynamodb.NewImage.updated_at)) {
    pricing_date = record.dynamodb.NewImage.updated_at.N;
  }
  return pricing_date;
};
