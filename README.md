# Lambda DynamoDB to Elasticsearch

A Lambda function to post Instrument and History records to ElasticSearch

This function will have two DynamoDB Stream event sources one from each of these projects
* https://github.com/Stockflare/api-instrument
* https://github.com/Stockflare/api-historical

The purpose of this function is to combine the records from both these streams into a single ElasticSearch index called `stock`

Both the Instrument and Historical tables have a primary key of `sic`, therefore a record from either of the event sources for the same `sic` will create or update the same document in ElasticSearch where the document's `id` is the `sic`.

Due to limitations in the information sent in the DynamoDB stream records we have had to hard-code a few parameters.

### Hard-Coded parameters
| Variable  | Value              | Description                  |
|:----------|:-------------------|:-----------------------------|
| region    | us-east-1          | AWS Region                   |
| es_domain | stockflare-staging | The AWS Elasticsearch domain |

## Processing of records
The processing of an event is as follows:
* Describe the ElasticSearch domain in order to get the endpoint url; then
* Create the `stock` index if it does not already exist; then
* For each `Record` in the event:
  * Search for an existing document with `id` of `sic`
  * Convert the `Record` into the format of an ElasticSearch document
  * Create or update the document

In this way the documents in ElasticSearch will contain all fields from both Instrument and Historical tables
