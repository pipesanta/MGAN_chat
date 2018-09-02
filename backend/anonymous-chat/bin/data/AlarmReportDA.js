'use strict'

const mongoDB = require('./MongoDB')();
const Rx = require('rxjs');
const CollectionName = "DeviceAlarmReports";
const { CustomError } = require('../tools/customError');


class AlarmReportDA {
  /**
   * gets DashBoardDevicesAlarmReport by type
   * @param {string} type
   */
  static getDashBoardDevicesAlarmReport$(evt) {    
    // if(evt.alarmType === "CPU_USAGE"){   
    //     // return Rx.Observable.throw(
    //     //   new CustomError("DashboardDA", "AlarmReportDA.getDashBoardDevicesAlarmReport$", undefined, "Custom error")
    //     // );
    //     console.log(dfer);
    // }    
    return Rx.Observable.forkJoin(
      AlarmReportDA.getAlarmsInRangeOfTime(evt.timeRanges[0], evt.alarmType),
      AlarmReportDA.getAlarmsInRangeOfTime(evt.timeRanges[1], evt.alarmType),
      AlarmReportDA.getAlarmsInRangeOfTime(evt.timeRanges[2], evt.alarmType)
    );
  }

  /**
   * Save the alarm document in mongo
   * @param {Object} evt 
   */
  static onDeviceAlarmActivated$(evt) {
    const collection = mongoDB.db.collection(CollectionName);
    const alarmType = evt.alarmType;
    return Rx.Observable.defer(() => collection.insertOne({
      timestamp: evt.data.timestamp,
      type: alarmType,
      deviceId: evt.aid,
      deviceHostname: evt.device.hostname,
      value: evt.data.value,
      unit: evt.data.value
    }))
    // get the information of alerts in frames for lastHour, lastTwohours, lastThreeHours
    .mergeMap(() =>
        Rx.Observable.forkJoin(
          AlarmReportDA.getAlarmsInRangeOfTime(evt.timeRanges[0], alarmType),
          AlarmReportDA.getAlarmsInRangeOfTime(evt.timeRanges[1], alarmType),
          AlarmReportDA.getAlarmsInRangeOfTime(evt.timeRanges[2], alarmType)
        )
      )
  }
  /**
   * get the number of alerts
   * @param {number} limit lowest date in millis to fecth alerts
   * @param {string} type of alert
   */
  static getAlarmsInRangeOfTime(lowestLimit, alarmType) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection
      .aggregate([
        { $match: { timestamp: { $gte: lowestLimit }, type: alarmType } },
        {
          $group: {
            _id: "$type",
            value: { $sum: 1 }
          }
        }
      ])
      .toArray())
      .mergeMap(result => this.getDistinctDevicesOnAlarm$(result, lowestLimit, alarmType))
      .map(item => {
        item[0].startTime = lowestLimit;
        item[0].alarmType = alarmType;
        return item;
      })
  }


  static getDistinctDevicesOnAlarm$(result, lowestLimit, alarmType){
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection
    .aggregate([
      { $match: { timestamp: { $gte: lowestLimit }, type: alarmType } },
      {
        $group: {
          _id: "$deviceId",
        }
      }
    ])
    .toArray())
    .map(res => {
      // if in the last hour there are not alarms it will create a empty registry 
      if(result[0] == undefined){
        result[0] = {
           _id: alarmType, 
           value: 0 
        }
      }
      result[0].informers = res.length
      return result;
    })
  }

  /**
   * 
   * @param {Object} timeRanges contains the information to show in the card about just one frame hour 
   * @param {int} topLimit Top devices  limit
   */
  static getTopAlarmDevices$(timeRanges) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.from(timeRanges)
      .mergeMap(timeRange => {      
          return Rx.Observable.defer(() => collection
            .aggregate([
              { $match: { timestamp: { $gte: timeRange[0].startTime }, type: timeRange[0].alarmType } },
              {
                $group: {
                  _id: { type: "$type", deviceId: "$deviceId" },
                  value: { $sum: 1 },
                  hostname: { $first: "$deviceHostname" }
                }
              },
              { $sort: { "value": -1 } }
            ]).toArray())
            .map((aggregateResult) => {
              const result = [];
              aggregateResult.forEach((item, index) => {
                if (item._id.deviceId == null) { return }
                result.push({
                  id: item._id.deviceId,
                  sn: item._id.deviceId,
                  hostname: item.hostname ? item.hostname : "NULL hostname",
                  alarmsCount: item.value,
                  deviceDetailLink: ''
                })
              });
              return result;
            })
            .map(aggregateResult => {
              timeRange[0].topDevices = aggregateResult;
              return timeRange;
            })
      })
      .toArray()
      .map(timeranges => timeRanges)
  }

  static generateAlarms__RANDOM__() {
    const collection = mongoDB.db.collection(CollectionName);
    const types = ["VOLTAGE", "TEMPERATURE", "CPU_USAGE", "RAM_MEMORY"];
    const units = ["V", "C", "%", "%"];
    const selection = Math.floor(Math.random() * 4);
    const id = `sn0000-000${Math.floor(Math.random() * 10)}-TEST`;
    return Rx.Observable.defer(() => collection.insertOne({
      timestamp: Date.now(),
      type: types[selection],
      deviceId: id,
      value: Math.floor(Math.random() * 100 + 5),
      unit: units[selection]
    }));    
  }

  static calculateTimeRanges$(evt) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection
    .aggregate([
      { $match: { type: evt.data.alarmType } },
      {
        $project: {
          date: { $add: [new Date(0), "$timestamp"] },
          timestamp: 1,
          deviceId: 1
        }
      },
      {
        $group: {
          _id: {
            year: { $year: { date: "$date", timezone: "-0500" } },
            month: { $month: { date: "$date", timezone: "-0500" } },
            interval: {
              $subtract: [
                { $minute: { date: "$date", timezone: "-0500" } },
                { $mod: [{ $minute: new Date(1524779294) }, 40] }
              ]
            }
          },
          grouped_data: {
            $push: { timestamp: "$timestamp", value: "$deviceId" }
          }
        }
      },
      { $limit: 1 }
    ])
    .toArray());
  }

/**
 * Remove all documents before the obsolete Threshold
 * @param {double} obsoleteThreshold 
 */
  static removeObsoleteAlarmsReports$(obsoleteThreshold){
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => 
    collection.remove({ timestamp: { $lt: obsoleteThreshold } }))
    .map(r => r.result)
  }
}

module.exports =  AlarmReportDA 