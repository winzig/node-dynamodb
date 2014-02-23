// Copyright Teleportd Ltd. and other Contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var fwk = require('fwk');
var events = require('events');

/**
 * The DynamoDb Object
 * @param spec {accessKeyId, secretAccessKey, region, endpoint, apiVersion, sslEnabled}
 * Now uses official AWS SDK for Node.JS: https://github.com/aws/aws-sdk-js
 */
var ddb = function(spec, my) {
  var _super = {};
  my = my || {};

  my.consumedCapacity = my.consumedCapacity || 0;

  my.schemaTypes = {
    number: 'N',
    string: 'S',
    number_array: 'NS',
    string_array: 'SS'
  };

  // public
  var createTable;
  var listTables;
  var describeTable;
  var deleteTable;
  var updateTable;

  var getItem;
  var putItem;
  var deleteItem;
  var updateItem;
  var query;
  var scan;
  var batchGetItem;
  var batchWriteItem;

  // private
  var scToDDB;
  var objToDDB;
  var objFromDDB;
  var arrFromDDB;

  aws = my.aws || require('aws-sdk');
  var dynamo = new aws.DynamoDB(spec);

  var that = new events.EventEmitter();
  that.setMaxListeners(0);


  /**
   * The CreateTable operation adds a new table to your account.
   * It returns details of the table.
   * @param table the name of the table
   * @param keySchema {hash: [attribute, type]} or {hash: [attribute, type], range: [attribute, type]}
   * @param localSecondaryIndexes {indexName:{AttributeName: attribute, KeyType: type, Projection:[]}
   * @param provisionedThroughput {write: X, read: Y}
   * @param cb callback(err, tableDetails) err is set if an error occured
   */
  createTable = function(table, keySchema, localSecondaryIndexes, provisionedThroughput, cb) {
    var data = {};
    data.TableName = table;
    data.KeySchema = [];
    data.AttributeDefinitions = [];
    data.ProvisionedThroughput = {};
    if(keySchema.hash && keySchema.hash.length == 2) {
        data.AttributeDefinitions.push({
            AttributeName: keySchema.hash[0],
            AttributeType: keySchema.hash[1] });
        data.KeySchema.push({
            AttributeName: keySchema.hash[0],
            KeyType: "HASH"
        });
    }else{
        //TODO raise parameter error, hash must be defined
    }
    if(keySchema.range && keySchema.range.length == 2) {
        data.AttributeDefinitions.push({
            AttributeName: keySchema.range[0],
            AttributeType: keySchema.range[1] });
        data.KeySchema.push({
            AttributeName: keySchema.range[0],
            KeyType: "RANGE"
        });
    }
    if(localSecondaryIndexes){
        data.LocalSecondaryIndexes = [];
        for(var key in localSecondaryIndexes){
            var thisLSI = localSecondaryIndexes[key];
            // first add to the attribute definitions
            data.AttributeDefinitions.push({
                AttributeName: thisLSI.AttributeName,
                AttributeType: thisLSI.AttributeType });
            var formatedLSI = {
                IndexName: key,
                KeySchema: [data.KeySchema[0],{
                    AttributeName: thisLSI.AttributeName,
                    KeyType: "RANGE"
                }],
                Projection: {
                    ProjectionType: 'KEYS_ONLY'
                }
            };
            if(thisLSI.Projection){
                if(Array.isArray(thisLSI.Projection)){
                    formatedLSI.Projection.ProjectionType = 'INCLUDE';
                    for(var index in thisLSI.Projection){
                        formatedLSI.Projection.NonKeyAttributes = thisLSI.Projection;
                    }
                }else if(typeof thisLSI.Projection === 'string' &&
                    thisLSI.Projection.toUpperCase() == 'ALL'){
                    formatedLSI.Projection.ProjectionType = 'ALL';
                }
            }
            data.LocalSecondaryIndexes.push(formatedLSI);
        }
        
    };
    if(provisionedThroughput) {
      if(provisionedThroughput.read)
        data.ProvisionedThroughput.ReadCapacityUnits = provisionedThroughput.read;
      if(provisionedThroughput.write)
        data.ProvisionedThroughput.WriteCapacityUnits = provisionedThroughput.write;
    }
    dynamo.createTable(data, function(err, res) {
      if(err) { cb(err) }
      else {
        cb(null, res.TableDescription);
      }
    });
  };


  /**
   * Updates the provisioned throughput for the given table.
   * It returns details of the table.
   * @param table the name of the table
   * @param provisionedThroughput {write: X, read: Y}
   * @param cb callback(err, tableDetails) err is set if an error occured
   */
  updateTable = function(table, provisionedThroughput, cb) {
    var data = {};
    data.TableName = table;
    data.ProvisionedThroughput = {};
    if(provisionedThroughput) {
      if(provisionedThroughput.read)
        data.ProvisionedThroughput.ReadCapacityUnits = provisionedThroughput.read;
      if(provisionedThroughput.write)
        data.ProvisionedThroughput.WriteCapacityUnits = provisionedThroughput.write;
    }
    dynamo.updateTable(data, function(err, res) {
      if(err) { cb(err) }
      else {
        cb(null, res.TableDescription);
      }
    });
  };


  /**
   * The DeleteTable operation deletes a table and all of its items
   * It returns details of the table
   * @param table the name of the table
   * @param cb callback(err, tableDetails) err is set if an error occured
   */
  deleteTable = function(table, cb) {
    var data = {};
    data.TableName = table;
    dynamo.deleteTable(data, function(err, res) {
      if(err) { cb(err) }
      else {
        cb(null, res.TableDescription);
      }
    });
  };


  /**
   * returns an array of all the tables associated with the current account and endpoint
   * @param options {limit, exclusiveStartTableName}
   * @param cb callback(err, tables) err is set if an error occured
   */
  listTables = function(options, cb) {
    var data = {};
    if(options.limit)
      data.Limit = options.limit;
    if(options.exclusiveStartTableName)
      data.ExclusiveStartTableName = options.exclusiveStartTableName;
    dynamo.listTables(data, function(err, res) {
      if(err) { cb(err) }
      else {
        cb(null, res.TableNames);
      }
    });
  };


  /**
   * returns information about the table, including the current status of the table,
   * the primary key schema and when the table was created
   * @param table the table name
   * @param cb callback(err, tables) err is set if an error occured
   */
  describeTable = function(table, cb) {
    var data = {};
    data.TableName = table;
    dynamo.describeTable(data, function(err, res) {
      if(err) { cb(err) }
      else {
        cb(null, res.Table);
      }
    });
  };

  /**
   * returns a set of Attributes for an item that matches the primary key.
   * @param table the tableName
   * @param key dictionary of hash and range attributeNames to values
   * @param options {attributesToGet, consistentRead}
   * @param cb callback(err, tables) err is set if an error occured
   */
  getItem = function(table, key, options, cb) {
    var data = {};
    try {
      data.TableName = table;
      data.Key = objToDDB(key);
      if(options.attributesToGet) {
        data.AttributesToGet = options.attributesToGet;
      }
      if(options.consistentRead) {
        data.ConsistentRead = options.consistentRead;
      }
      data.ReturnConsumedCapacity = 'TOTAL';
    }
    catch(err) {
      cb(err);
      return;
    }
    dynamo.getItem(data, function(err, res) {
      if(err) { cb(err) }
      else {
        my.consumedCapacity += res.ConsumedCapacity.CapacityUnits;
        try {
          var item = res.Item ? objFromDDB(res.Item) : null;
        }
        catch(err) {
          cb(err);
          return;
        }
        cb(null, item, res.ConsumedCapacity.CapacityUnits);
      }
    });
  };


  /**
   * Creates a new item, or replaces an old item with a new item
   * (including all the attributes). If an item already exists in the
   * specified table with the same primary key, the new item completely
   * replaces the existing item.
   * putItem expects a dictionary (item) containing only strings and numbers
   * This object is automatically converted into the expected Amazon JSON
   * format for convenience.
   * @param table the tableName
   * @param item the item to put (string/number/string array dictionary)
   * @param options {expected, returnValues}
   * @param cb callback(err, attrs, consumedCapUnits) err is set if an error occured
   */
  putItem = function(table, item, options, cb) {
    var data = {};
    try {
      data.TableName = table;
      data.Item = objToDDB(item);
      if(options.expected) {
        data.Expected = {};
        for(var i in options.expected) {
          data.Expected[i] = {};
          if(typeof options.expected[i].exists === 'boolean') {
            data.Expected[i].Exists = options.expected[i].exists;
          }
          if(typeof options.expected[i].value !== 'undefined') {
            data.Expected[i].Value = scToDDB(options.expected[i].value);
          }
        }
      }
      if(options.returnValues) {
        data.ReturnValues = options.returnValues;
      }
      data.ReturnConsumedCapacity = 'TOTAL';
    }
    catch(err) {
      cb(err);
      return;
    }
    dynamo.putItem(data, function(err, res) {
      if(err) { cb(err) }
      else {
        my.consumedCapacity += res.ConsumedCapacity.CapacityUnits;
        try {
          var attr = res.Attributes ? objFromDDB(res.Attributes) : null;
        }
        catch(err) {
          cb(err);
          return;
        }
        cb(null, attr, res.ConsumedCapacity.CapacityUnits);
      }
    });
  };


  /**
   * deletes a single item in a table by primary key. You can perform a conditional
   * delete operation that deletes the item if it exists, or if it has an expected
   * attribute value.
   * @param table the tableName
   * @param hash the hashKey
   * @param range the rangeKey. NOT USED! The latest api does not support the range key.
   * @param options {expected, returnValues}
   * @param cb callback(err, attrs, consumedCapUnits) err is set if an error occured
   */
  deleteItem = function(table, hash, range, options, cb) {
    var data = {};
    try {
      data.TableName = table;
      // fix to be compatible with '2012-08-10' API version
      var key = hash;
      // range key is not used in '2012-08-10' API
      data.Key = objToDDB(key);
      if(options.expected) {
        data.Expected = {};
        for(var i in options.expected) {
          data.Expected[i] = {};
          if(typeof options.expected[i].exists === 'boolean') {
            data.Expected[i].Exists = options.expected[i].exists;
          }
          if(typeof options.expected[i].value !== 'undefined') {
            data.Expected[i].Value = scToDDB(options.expected[i].value);
          }
        }
      }
      if(options.returnValues)
        data.ReturnValues = options.returnValues;
    }
    catch(err) {
      cb(err);
      return;
    }
    dynamo.deleteItem(data, function(err, res) {
      if(err) { cb(err) }
      else {
        my.consumedCapacity += res.ConsumedCapacityUnits;
        try {
          var attr = objFromDDB(res.Attributes);
        }
        catch(err) {
          cb(err);
          return;
        }
        cb(null, attr, res.ConsumedCapacityUnits);
      }
    });
  };


  /**
   * Updates an item with the supplied update orders.
   * @param table the tableName
   * @param key dictionary of hash and range attributeNames to values
   * @param updates dictionary of attributeNames to { value: XXX, action: 'PUT|ADD|DELETE' }
   * @param options {expected, returnValues}
   * @param cb callback(err, attrs, consumedCapUnits) err is set if an error occured
   */
  updateItem = function(table, key, updates, options, cb) {
    var data = {};
    try {
      data.TableName = table;
      data.Key = objToDDB(key);
      if(options.expected) {
        data.Expected = {};
        for(var i in options.expected) {
          data.Expected[i] = {};
          if(typeof options.expected[i].exists === 'boolean') {
            data.Expected[i].Exists = options.expected[i].exists;
          }
          if(typeof options.expected[i].value !== 'undefined') {
            data.Expected[i].Value = scToDDB(options.expected[i].value);
          }
        }
      }
      if(typeof updates === 'object') {
        data.AttributeUpdates = {};
        for(var attr in updates) {
          if(updates.hasOwnProperty(attr)) {
            data.AttributeUpdates[attr] = {};
            if(typeof updates[attr].action === 'string')
              data.AttributeUpdates[attr]["Action"] = updates[attr].action;
            if(typeof updates[attr].value !== 'undefined')
              data.AttributeUpdates[attr]["Value"] = scToDDB(updates[attr].value);
          }
        }
      }
      if(options.returnValues) {
        data.ReturnValues = options.returnValues;
      }
      data.ReturnConsumedCapacity = 'TOTAL';
    }
    catch(err) {
      cb(err);
      return;
    }
    dynamo.updateItem(data, function(err, res) {
      if(err) { cb(err) }
      else {
        my.consumedCapacity += res.ConsumedCapacity.CapacityUnits;
        try {
          var attr = res.Attributes ? objFromDDB(res.Attributes) : null;
        }
        catch(err) {
          cb(err);
          return;
        }
        cb(null, attr, res.ConsumedCapacity.CapacityUnits);
      }
    });
  };


  /**
   * An object representing a table query, or an array of such objects
   * { 'table': { keys: [1, 2, 3], attributesToGet: ['user', 'status'] } }
   *           or keys: [['id', 'range'], ['id2', 'range2']] 
   * @param cb callback(err, tables) err is set if an error occured
   */
  batchGetItem = function(request, cb) {
    var data = {};
    try {
      data.RequestItems = {};
      for(var table in request) {
        if(request.hasOwnProperty(table)) {
          var parts = Array.isArray(request[table]) ? request[table] : [request[table]];
          
          for(var i = 0; i < parts.length; ++i) {
            var part = parts[i];
            var tableData = {Keys: []};
            var hasRange = Array.isArray(part.keys[0]);

            for(var j = 0; j < part.keys.length; j++) {
              var key = part.keys[j];
              var keyData = hasRange ? {"HashKeyElement": scToDDB(key[0]), "RangeKeyElement": scToDDB(key[1])} : {"HashKeyElement": scToDDB(key)};
              tableData.Keys.push(keyData);
            }

            if (part.attributesToGet) {
              tableData.AttributesToGet = part.attributesToGet;
            }
            data.RequestItems[table] = tableData;
          }
        }
      }
    }
    catch(err) {
      cb(err);
      return;
    }
    dynamo.batchGetItem(data, function(err, res) {
      if(err) { cb(err) }
      else {
        var consumedCapacity = 0;
        for(var table in res.Responses) {
          var part = res.Responses[table];
          var cap = part.ConsumedCapacityUnits;
          if (cap) {
            consumedCapacity += cap;
          }
          if (part.Items) {
            try {
              part.items = arrFromDDB(part.Items);
            }
            catch(err) {
              cb(err);
              return;
            }
            delete part.Items;
          }
          if (res.UnprocessedKeys[table]) {
            part.UnprocessedKeys = res.UnprocessedKeys[table];
          }
        }
        my.consumedCapacity += consumedCapacity;
        if (parts.length == 1) {
          var smartResponse = res.Responses[table];
          cb(null, smartResponse, consumedCapacity);
        }
        else {
          cb(null, res.Responses, consumedCapacity);
        }
      }
    });
  };


  /**
   * Put or delete several items across multiple tables
   * @param putRequest dictionnary { 'table': [item1, item2, item3], 'table2': item }
   * @param deleteRequest dictionnary { 'table': [key1, key2, key3], 'table2': [[id1, range1], [id2, range2]] }
   * @param cb callback(err, res, cap) err is set if an error occured
   */
  batchWriteItem = function(putRequest, deleteRequest, cb) {
    var data = {};
    try {
      data.RequestItems = {};

      for(var table in putRequest) {
        if(putRequest.hasOwnProperty(table)) {
          var items = (Array.isArray(putRequest[table]) ? putRequest[table] : [putRequest[table]]);

          for(var i = 0; i < items.length; i++) {
            data.RequestItems[table] = data.RequestItems[table] || [];
            data.RequestItems[table].push( { "PutRequest": { "Item": objToDDB(items[i]) }} );
          }
        }
      }
   
      for(var table in deleteRequest) {
        if(deleteRequest.hasOwnProperty(table)) {
          var parts = (Array.isArray(deleteRequest[table]) ? deleteRequest[table] : [deleteRequest[table]]);
          
          for(var i = 0; i < parts.length; i++) {
            var part = parts[i];
            var hasRange = Array.isArray(part);
            
            var keyData = hasRange ? {"HashKeyElement": scToDDB(part[0]), "RangeKeyElement": scToDDB(part[1])} : {"HashKeyElement": scToDDB(part)};
            
            data.RequestItems[table] = data.RequestItems[table] || [];
            data.RequestItems[table].push( { "DeleteRequest": { "Key" : keyData }} );
          }
        }
      }

      dynamo.batchWriteItem(data, function(err, res) {
        if(err)
          cb(err);
        else {
          var consumedCapacity = 0;
          for(var table in res.Responses) {
            if(res.Responses.hasOwnProperty(table)) {
              var part = res.Responses[table];
              var cap = part.ConsumedCapacityUnits;
              if (cap) {
                consumedCapacity += cap;
              }
            }
          }
          my.consumedCapacity += consumedCapacity;
          cb(null, res.UnprocessedItems, consumedCapacity);
        }
      });
    }
    catch(err) {
      cb(err) 
    }     
  };


  /**
   * returns a set of Attributes for an item that matches the query
   * @param table the tableName
   * @param hash the hashKey as an object with attribute names and values (or 
   *             comparison operator and value i.e. {'GT':10})
   * @param options {attributesToGet, limit, consistentRead, count, 
   *                 scanIndexForward, exclusiveStartKey, indexName}
   * @param cb callback(err, tables) err is set if an error occured
   */
  query = function(table, hash, options, cb) {
    var data = {};
    try {
      data.TableName = table;
      data.KeyConditions = {};
      for(var keyName in hash) {
        var hashValue = hash[keyName];
        if(typeof hashValue === 'object'){ // handle comparisons other than equal
            for(var op in hashValue){
                data.KeyConditions[keyName] = {
                  "AttributeValueList": [],
                  "ComparisonOperator": op
                };
                if(Array.isArray(hashValue[op])){ // handle 'between' cases
                    for(var index in hashValue[op]){
                        var casted = scToDDB(hashValue[op][index]);
                        data.KeyConditions[keyName].AttributeValueList.push(casted);
                    }
                }else{
                    var casted = scToDDB(hashValue[op]);
                    data.KeyConditions[keyName].AttributeValueList.push(casted);
                }
            }
        }else{ // just a value specified
            data.KeyConditions[keyName] = {
              "AttributeValueList": [scToDDB(hashValue)],
              "ComparisonOperator": 'EQ'
            };
        }
      }
      if(options.attributesToGet) {
        data.AttributesToGet = options.attributesToGet;
      }
      if(options.limit) {
        data.Limit = options.limit;
      }
      if(options.consistentRead) {
        data.ConsistentRead = options.consistentRead;
      }
      if(options.count && !options.attributesToGet) {
        data.Count = options.count;
      }
      if(options.scanIndexForward === false) {
        data.ScanIndexForward = false;
      }
      if(options.exclusiveStartKey && options.exclusiveStartKey.hash) {
        data.ExclusiveStartKey = { HashKeyElement: scToDDB(options.exclusiveStartKey.hash) };
        if(options.exclusiveStartKey.range)
          data.ExclusiveStartKey.RangeKeyElement = scToDDB(options.exclusiveStartKey.range);      
      }
      if(options.indexName) {
        data.IndexName = options.indexName;
      }
    }
    catch(err) {
      cb(err);
      return;
    }
    dynamo.query(data, function(err, res) {
      if(err) { cb(err) }
      else {
        my.consumedCapacity += res.ConsumedCapacityUnits;
        var r = { count: res.Count,
                  items: [],
                  lastEvaluatedKey: {}};
        try {
          if (res.Items) {
            r.items = arrFromDDB(res.Items);
          }
          if(res.LastEvaluatedKey) {
            var key = objFromDDB(res.LastEvaluatedKey);
            r.lastEvaluatedKey = { hash: key.HashKeyElement,
                                   range: key.RangeKeyElement };
          }
        }
        catch(err) {
          cb(err);
          return;
        }
        cb(null, r, res.ConsumedCapacityUnits);
      }
    });
  };


  /**
   * returns one or more items and its attributes by performing a full scan of a table.
   * @param table the tableName
   * @param options {attributesToGet, limit, count, scanFilter, exclusiveStartKey}
   * @param cb callback(err, {count, items, lastEvaluatedKey}) err is set if an error occured
   */
  scan = function(table, options, cb) {
    var data = {};
    try {
      data.TableName = table;
      if(options.attributesToGet) {
        data.AttributesToGet = options.attributesToGet;
      }
      if(options.limit) {
        data.Limit = options.limit;
      }
      if(options.count && !options.attributesToGet) {
        data.Count = options.count;
      }
      if(options.exclusiveStartKey && options.exclusiveStartKey.hash) {
        data.ExclusiveStartKey = { HashKeyElement: scToDDB(options.exclusiveStartKey.hash) };
        if(options.exclusiveStartKey.range)
          data.ExclusiveStartKey.RangeKeyElement = scToDDB(options.exclusiveStartKey.range);      
      }
      if(options.filter) {
        data.ScanFilter = {};
        for(var attr in options.filter) {
          if(options.filter.hasOwnProperty(attr)) {
            for(var op in options.filter[attr]) { // supposed to be only one
              if(typeof op === 'string') {
                data.ScanFilter[attr] = {"AttributeValueList":[],"ComparisonOperator": op.toUpperCase()};
                if(op === 'not_null' || op === 'null') {
                  // nothing ot do
                }
                else if((op == 'between' || op == 'in') &&
                        Array.isArray(options.filter[attr][op]) &&
                        options.filter[attr][op].length > 1) {
                  for (var i = 0; i < options.filter[attr][op].length; ++i) {
                    data.ScanFilter[attr].AttributeValueList.push(scToDDB(options.filter[attr][op][i]));
                  }
                }
                else {
                  data.ScanFilter[attr].AttributeValueList.push(scToDDB(options.filter[attr][op]));
                }
              }
            }
          }
        }
      }
    }
    catch(err) {
      cb(err);
      return;
    }
    dynamo.scan(data, function(err, res) {
      if(err) { cb(err) }
      else {
        my.consumedCapacity += res.ConsumedCapacityUnits;
        var r = { count: res.Count,
                  items: [],
                  lastEvaluatedKey: {},
                  scannedCount: res.ScannedCount };          
        try {
          if(Array.isArray(res.Items)) {
            r.items = arrFromDDB(res.Items);
          }
          if(res.LastEvaluatedKey) {
            var key = objFromDDB(res.LastEvaluatedKey);
            r.lastEvaluatedKey = { hash: key.HashKeyElement,
                                   range: key.RangeKeyElement };
          }
        }
        catch(err) {
          cb(err);
          return;
        }
        cb(null, r, res.ConsumedCapacityUnits);
      }
    });
  };  


  //-- INTERNALS --//

  /**
   * converts a JSON object (dictionary of values) to an amazon DynamoDB 
   * compatible JSON object
   * @param json the JSON object
   * @throws an error if input object is not compatible
   * @return res the converted object
   */
  objToDDB = function(json) {
    if(typeof json === 'object') {
      var res = {};
      for(var i in json) {
        if(json.hasOwnProperty(i) && json[i] !== null) {
          res[i] = scToDDB(json[i]);
        }
      }
      return res;
    }
    else {
      return json;
    }
  };


  /**
   * converts a string, string array, number or number array (scalar)
   * JSON object to an amazon DynamoDB compatible JSON object
   * @param json the JSON scalar object
   * @throws an error if input object is not compatible
   * @return res the converted object
   */
  scToDDB = function(value) {
    if (typeof value === 'number') {
      return { "N": value.toString() };
    }
    if (typeof value === 'string') {
      return { "S": value };
    }
    if (Array.isArray(value)) {
      var arr = [];
      var length = value.length;
      var isSS = false;
      for(var i = 0; i < length; ++i) {
        if(typeof value[i] === 'string') {
          arr[i] = value[i];
          isSS = true;
        }
        else if(typeof value[i] === 'number') {
          arr[i] = value[i].toString();
        }
      }
      return isSS ? {'SS': arr} : {'NS': arr};
    }
    throw new Error('Non Compatible Field [not string|number|string array|number array]: ' + value);
  }


  /**
   * converts a DynamoDB compatible JSON object into
   * a native JSON object
   * @param ddb the ddb JSON object
   * @throws an error if input object is not compatible
   * @return res the converted object
   */
  objFromDDB = function(ddb) {
    if(typeof ddb === 'object') {
      var res = {};
      for(var i in ddb) {
        if(ddb.hasOwnProperty(i)) {
          if(ddb[i]['S'])
            res[i] = ddb[i]['S'];
          else if(ddb[i]['SS'])
            res[i] = ddb[i]['SS'];
          else if(ddb[i]['N'])
            res[i] = parseFloat(ddb[i]['N']);
          else if(ddb[i]['NS']) {
            res[i] = [];
            for(var j = 0; j < ddb[i]['NS'].length; j ++) {
              res[i][j] = parseFloat(ddb[i]['NS'][j]);
            }
          }
          else
            throw new Error('Non Compatible Field [not "S"|"N"|"NS"|"SS"]: ' + i);
        }
      }
      return res;
    }
    else {
      return ddb;
    }
  };


  /**
   * converts an array of DynamoDB compatible JSON object into
   * an array of native JSON object
   * @param arr the array of ddb  objects to convert
   * @throws an error if input object is not compatible
   * @return res the converted object
   */
  arrFromDDB = function(arr) {
    var length = arr.length;
    for(var i = 0; i < length; ++i) {
      arr[i] = objFromDDB(arr[i]);
    }
    return arr;
  };


  fwk.method(that, 'createTable', createTable, _super);
  fwk.method(that, 'listTables', listTables, _super);
  fwk.method(that, 'describeTable', describeTable, _super);
  fwk.method(that, 'updateTable', updateTable, _super);
  fwk.method(that, 'deleteTable', deleteTable, _super);

  fwk.method(that, 'putItem', putItem, _super);
  fwk.method(that, 'getItem', getItem, _super);
  fwk.method(that, 'deleteItem', deleteItem, _super);
  fwk.method(that, 'updateItem', updateItem, _super);
  fwk.method(that, 'query', query, _super);
  fwk.method(that, 'batchGetItem', batchGetItem, _super);
  fwk.method(that, 'batchWriteItem', batchWriteItem, _super);
  fwk.method(that, 'scan', scan, _super);

  // for testing purpose
  fwk.method(that, 'objToDDB', objToDDB, _super);
  fwk.method(that, 'scToDDB', scToDDB, _super);
  fwk.method(that, 'objFromDDB', objFromDDB, _super);
  fwk.method(that, 'arrFromDDB', arrFromDDB, _super);

  fwk.getter(that, 'consumedCapacity', my, 'consumedCapacity');
  fwk.getter(that, 'schemaTypes', my, 'schemaTypes');

  return that;
};

exports.ddb = ddb;
