"use strict";

const mongoDB = require("./MongoDB")();
const Rx = require("rxjs");
const CollectionName = "deviceState";
const { CustomError } = require('../tools/customError');

class DeviceStatusDA {

  /**
   * gets DashboardDeviceStatus by sn
   * @param {string} type
   */
  static getDeviceStatusByID$(deviceId, projection) {
    const collection = mongoDB.db.collection(CollectionName);
    let observ = null;
    if(projection){
      observ = Rx.Observable.defer(() => collection.findOne({ deviceId }, projection) );
    }else{
      observ = Rx.Observable.defer(() => collection.findOne({ deviceId }));
    }
    return observ;
  }

  /**
   * 
   */
  static getDevicesTotalAccount$(){
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection.find().count());
  }

  /**
   * Updates the device state in DB and gets the new data for front-end chart interested
   * @param {String} sn device sn to update the state in DB
   */
  static onDeviceOnlineReported(evt) {
    return this.updateOne$(
      { deviceId: evt.aid },
      { $set: { online: evt.data.connected } },
      { upsert: true}
    );
  }
  
  /**
   * Search and using aggregate counts the total devices per cuenca with
   *  networkState condition
   */
  static getTotalDeviceByCuencaAndNetworkState$() {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection
    .aggregate([
      // { $match: { active: true } },
      {
        $group: {
          _id: { cuenca: "$groupName", online: "$online" },
          value: { $sum: 1 }   
        }            
      },
      { $sort: { _id: 1 } }
    ])
    .toArray())
    .map(results => results.filter(result => result._id.cuenca))
  }
  /**
   * Updates a document in the collection
   * @param {object} filter The selection criteria for the update. The same query selectors as in the find() method are available.
   * @param {object} update The modifications to apply.
   * @param {object} options Optional update query
   */
  static updateOne$(filter, update, options) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection.updateOne(filter, update, options));
  }


  static generateDevices__RANDOM__$() {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection.insertOne(
      {
        active: true,
        deviceId: `sn0000-000${Math.floor((Math.random() * 10))}-TEST`,
        hostname: `ABC${Math.floor((Math.random() * 999) + 100)}`,
        groupName: `Cuenca ${Math.floor((Math.random() * 5) + 1)}`,
        online: true,
        ramMemoryAlert: false,
        cpuAlert: false,
        temperatureAlert: false,
        voltageAlert: false
      }
    ))
  }

/**
 * On device state Report event:
 * update new values on collection
 */
static onDeviceStateReportedEvent$(info){
  return this.updateOne$(
    { deviceId: info.sn },
    { $set :  {...info}  },
    { upsert: true }
  )
}
  
}

module.exports = DeviceStatusDA;
