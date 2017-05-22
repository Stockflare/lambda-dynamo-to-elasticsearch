// dependencies
var aws = require('aws-sdk');
var _ = require('underscore');
var path = require('path');
var elasticsearch = require('elasticsearch');
var http_aws_es = require('http-aws-es');
var when = require('when');
var moment = require('moment');
var table = 'stock';
var region = 'us-east-1';
var es_domain = 'stockflare-production';
var es_endpoint = 'https://search-stockflare-production-34le5cz4pso7iiztz7pqnkpks4.us-east-1.es.amazonaws.com';
var rest = require('restler');

exports.handler = function(event, context) {


  //  Log out the entire invocation
  // console.log(JSON.stringify(event, null, 2));
  var aws_es = new aws.ES({
    region: region
  });

  // Promise - Describe the ES Domain in order to get the endpoint url
  when.promise(function(resolve, reject, notify){
    // aws_es.describeElasticsearchDomains({
    //   DomainNames: [ es_domain ]
    // }, function(err, data){
    //   if (err) {
    //     console.log("describeElasticsearchDomains error:", err, data);
    //     reject(err);
    //   } else {
    //     resolve({domain: _.first(data.DomainStatusList)});
    //   }
    // });
    resolve({
      domain: {
        Endpoint: es_endpoint
      }
    });
  }).then(function(result){
    // Promise - Create the Index if needed - resolve if it is already there
    // or create and then resolve
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

      // es.indices.exists({
      //   index: table
      // },function(err, response, status){
      //   // console.log('Looking for Index');
      //   // console.log(err, response, status);
      //   if (status == 200) {
      //     // console.log('Index Exists');
      //     resolve({es: es, domain: result.domain});
      //   } else if (status == 404) {
      //     createIndex(es, table, function(){
      //       resolve({es: es, domain: result.domain});
      //     });
      //   } else {
      //     reject(err);
      //   }
      // });
      resolve({es: es, domain: result.domain})
    });
    return promise;
  }).then(function(result){
    // console.log('Index is ready');
    // Create promises for every record that needs to be processed
    // resolve as each successful callback comes in
    // console.log(event.Records);
    var records = _.map(event.Records, function(record, index, all_records){
      return when.promise(function(resolve, reject, notify){
        if (record.eventName == 'REMOVE') {
          resolve(record);
        } else {
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
        }
      });
    });

    // return a promise array of all records
    return when.all(records);
  }).done(function(records){
    // Succeed the context if all records have been created / updated
    // console.log("Processed all records");
    context.succeed("Successfully processed " + records.length + " records.");
  }, function(reason){
    context.fail("Failed to process records " + reason);
  });


};

var createIndex = function(es, table, callback) {
  // console.log('createIndex', table);
  es.indices.create({
    index: table
  }, function(err, response, status){
    if (err) {
      // console.log("Index could not be created", err, response, status);
    } else {
      // console.log("Index has been created");
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
        // console.log('Document Exists');
        resolve(true);
      } else if (status == 404) {
        resolve(false);
      } else {
        console.log(err);
        reject(err);
      }
    });
  });
};

var putRecord = function(es, table, record, exists) {
  // console.log('putRecord:', record.dynamodb.NewImage.sic.S);
  return when.promise(function(resolve, reject, notify){
    two_days_ago = (moment().utc().subtract(5, 'days').valueOf()) / 1000;
    if (_.isUndefined(record.dynamodb.NewImage.updated_at) || record.dynamodb.NewImage.updated_at.N >= two_days_ago) {
      // console.log('Either no updated date or date within two days');
      // console.log(two_days_ago);
      if (!_.isUndefined(record.dynamodb.NewImage.updated_at)) {
        // console.log(record.dynamodb.NewImage.updated_at.N);
      }

      // If the record is an instrument then get the Sector Code Name
      //
      when.promise(function(resolve, reject, notify){
        if (record.dynamodb.NewImage.sector_code) {
          var params = {
            codes: [record.dynamodb.NewImage.sector_code.S]
          }
          // console.log(params)
          rest.putJson('http://sectors.internal-stockflare.com', params)
          .on('success', function(data, response){
            if (data.length > 0) {
              // console.log('Got Sector Code: ' + data[0].name);
              record.dynamodb.NewImage.sector_code_name = {
                S: data[0].name
              }
              resolve(record);

            } else {
              resolve(record)
            }
          }).on('fail', function(data, response){
            console.log('Could not get Sector Code:');
            resolve(record);
          });
        } else {
          resolve(record)
        }
      }).done(function() {
        // Now save the record
        var params = {
          index: table,
          id: record.dynamodb.NewImage.sic.S,
          body: esBody(record),
          type: 'stock'
        };
        var handler = function(err, response, status) {
          if (status == 200 || status == 201) {
            // console.log('Document written');
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
      }, function(err) {
        reject(err)
      })
    } else {
      // console.log('Not saving record because it is too old');
      resolve(record);
    }
  });
};

// Deal with Floats and Nulls coming in on the Stream
// in order to created a valid ES Body
// Otherwise just reformat into ES body
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
