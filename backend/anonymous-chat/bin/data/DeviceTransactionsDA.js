"use strict";

const mongoDB = require("./MongoDB")();
const Rx = require("rxjs");
const CollectionName = "deviceTransactions";
const { CustomError } = require('../tools/customError');

class DeviceTransactionsDA {

  /**
   * Updates a document in the collection
   * @param {object} filter The selection criteria for the update. The same query selectors as in the find() method are available.
   * @param {object} update The modifications to apply.
   * @param {object} options Optional update query
   */
  static insertDeviceTransaction$(deviceTransaction, update, options) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection.insertOne(deviceTransaction));
  }

  /**
   * Updates a document in the collection
   * @param {object} filter The selection criteria for the update. The same query selectors as in the find() method are available.
   * @param {object} update The modifications to apply.
   * @param {object} options Optional update query
   */
  static handleDeviceMainAppErrsTranspCountReported$(filter, update, options) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection.updateOne(filter, update, options));
  }

  /**
   * Gets the devices transactions group by group name and time intervals of 10 minutes between the specified dates
   * @param {*} startDate Start date
   * @param {*} endDate  End date
   */
  static getCuencaNamesWithSuccessTransactionsOnInterval$(startDate, endDate) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection
    .aggregate(
      [
        { $match: { 
            $and: [ 
                { timestamp: { $gte: startDate, $lt: endDate } } ,
                { groupName: { $ne: null } }
            ]} 
        },
        {
            "$project": {
                "date": { "$add": [new Date(0), "$timestamp"] },
                "timestamp": 1,
                "value": 1,
                "success": 1,
                "groupName": 1
            }
        },
        { 
            "$group": {
                "_id": {
                    "interval": {"$add": ["$timestamp",     {"$subtract": [{"$multiply": [{"$subtract": [ 10,{ "$mod": [{ "$minute": "$date"}, 10 ]}]}, 60000]},{"$add": [( {"$multiply": [{"$second": "$date"}, 1000]}), {"$millisecond": "$date"}]}]}    ]},
                    "groupName": "$groupName"
                },
                "transactions": { "$sum": { "$cond": [ { "$eq": ["$success", true] }, "$value", 0] }},
                "errors": { "$sum": { "$cond": [ { "$eq": ["$success", false] }, "$value", 0] }}
            }
        },
        {
            "$project":{
                "interval": "$_id.interval",
                "groupName": "$_id.groupName",
                "transactions": 1,
                "errors": 1
            }
        },
        {
            $group: {
                _id: "$groupName"
            }
        },
        {
            $project : {
                _id: 0,
                name: "$_id"
            }
        }
    ]
    )
    .toArray());
  }

    /**
   * Gets the devices transactions group by intervals of 10 minutes between the specified dates
   * @param {*} startDate Start date
   * @param {*} endDate  End date
   */
  static getDeviceTransactionGroupByTimeInterval$(startDate, endDate, cuenca) {
    const collection = mongoDB.db.collection(CollectionName);
    let matchCriteria = { $match: { timestamp: { $gte: startDate, $lt: endDate }} }
    if(cuenca){
        matchCriteria = { $match: { timestamp: { $gte: startDate, $lt: endDate }, groupName: cuenca  } }
    }
    // console.log(startDate, endDate, cuenca, matchCriteria);
    return Rx.Observable.defer(() => collection
    .aggregate(
      [
        matchCriteria,
        {
            "$project": {
                "date": { "$add": [new Date(0), "$timestamp"] },
                "timestamp": 1,
                "value": 1,
                "success": 1
            }
        },
        { 
            "$group": {
                "_id": {
                    "interval": {"$add": ["$timestamp",     {"$subtract": [{"$multiply": [{"$subtract": [ 10,{ "$mod": [{ "$minute": "$date"}, 10 ]}]}, 60000]},{"$add": [( {"$multiply": [{"$second": "$date"}, 1000]}), {"$millisecond": "$date"}]}]}    ]}
                },
                "transactions": { "$sum": { "$cond": [ { "$eq": ["$success", true] }, "$value", 0] }},
                "errors": { "$sum": { "$cond": [ { "$eq": ["$success", false] }, "$value", 0] }}
            }
        },
        {
            "$project":{
                "interval": "$_id.interval",
                "transactions": 1,
                "errors": 1
            }
        }
    ]
    )
    .toArray());
    // .do(val => console.log('getDeviceTransactionGroupByTimeInterval RESULT ===========> ', val));
  }

  static getDeviceTransactionGroupByGroupName$(evt){
      return Rx.Observable.forkJoin(
        DeviceTransactionsDA.getDeviceTransactionGroupByGroupNameInInterval$(evt.timeRanges[0], evt.endTimeLimit, evt.timeRangesLabel[0]),
        DeviceTransactionsDA.getDeviceTransactionGroupByGroupNameInInterval$(evt.timeRanges[1], evt.endTimeLimit, evt.timeRangesLabel[1]),
        DeviceTransactionsDA.getDeviceTransactionGroupByGroupNameInInterval$(evt.timeRanges[2], evt.endTimeLimit, evt.timeRangesLabel[2])
      );
  }


  static getDeviceTransactionGroupByGroupNameInInterval$(startDate, endTime, timeInterval){
    //   console.log(startDate, endTime, timeInterval )
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() =>
    collection.aggregate([
        { $match: { timestamp: { $gte: startDate, $lte: endTime }, success: true } },
        {
          $project: {
            groupName: 1,
            transactions: 1,
            value: 1
          }
        },
        {
            $group: {
              _id: { cuenca: "$groupName" },
              value: { $sum: "$value" }   
            }            
        },
        {
            $project: {
                _id: 0,
                name: "$_id.cuenca",
                value: 1
            }
        },
        { $sort: { _id: 1 } }
      ])
      .toArray()
    )
    .map(result => {
        return {
            timeRange: timeInterval,
            data: result
        }
    })
   
  }

    static removeObsoleteTransactions$(obsoleteThreshold) {
        const collection = mongoDB.db.collection(CollectionName);
        return Rx.Observable.defer(() =>
            collection.remove({ timestamp: { $lt: obsoleteThreshold } }))
            .map(r => r.result)
    }
  
}

module.exports = DeviceTransactionsDA;
