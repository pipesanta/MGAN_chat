"use strict";
const Rx = require("rxjs");
const eventSourcing = require("../../tools/EventSourcing")();
const dashBoardDevices = require("../../domain/DashBoardDevices")();
const mbeKey = "ms-dashboard-devices_mbe_dashboard-devices";

let instance;

class EventStoreService {
  constructor() {
    this.functionMap = this.generateFunctionMap();
    this.subscriptions = [];
    this.aggregateEventsArray = this.generateAggregateEventsArray();
  }

  generateFunctionMap() {
    return {
      DeviceConnected: {
        fn: dashBoardDevices.handleDeviceConnectedEvent$,
        obj: dashBoardDevices
      },
      DeviceCpuUsageAlarmActivated: {
        fn: dashBoardDevices.DeviceCpuUsageAlarmActivated$,
        obj: dashBoardDevices
      },
      DeviceRamuUsageAlarmActivated: {
        fn: dashBoardDevices.onDeviceRamuUsageAlarmActivated$,
        obj: dashBoardDevices
      },
      DeviceTemperatureAlarmActivated: {
        fn: dashBoardDevices.onDeviceTemperatureAlarmActivated$,
        obj: dashBoardDevices
      },
      DeviceLowVoltageAlarmReported: {
        fn: dashBoardDevices.handleDeviceLowVoltageAlarmEvent$,
        obj: dashBoardDevices
      },
      DeviceHighVoltageAlarmReported: {
        fn: dashBoardDevices.handleDeviceHighVoltageAlarmEvent$,
        obj: dashBoardDevices
      },
      DeviceDeviceStateReported: {
        fn: dashBoardDevices.handleDeviceStateReportedEvent$,
        obj: dashBoardDevices
      },
      DeviceMainAppUsosTranspCountReported: {
        fn: dashBoardDevices.persistSuccessDeviceTransaction$,
        obj: dashBoardDevices
      },
      DeviceMainAppErrsTranspCountReported: {
        fn: dashBoardDevices.persistFailedDeviceTransaction$,
        obj: dashBoardDevices
      },
      CleanDashBoardDevicesHistoryJobTriggered: {
        fn: dashBoardDevices.removeAllObsoleteMongoDocuments$,
        obj: dashBoardDevices
      }
    };
  }

  /**
   * Starts listening to the EventStore
   * Returns observable that resolves to each subscribe agregate/event
   *    emit value: { aggregateType, eventType, handlerName}
   */
  start$() {
    ////##################################################################
    ///######## SOLO PARA GENERAR  REGISTROS DE ALARMAS##############
    ////##################################################################

    // Rx.Observable.interval(5000).subscribe(() => {
    //   dashBoardDevices.generateAlarms__RANDOM__$()
    //   .subscribe(r => {});
    // });

    // Rx.Observable.interval(3000).subscribe(() => {
    //   dashBoardDevices.generateDevices__RANDOM__$()
    //   .subscribe(
    //     r => {},
    //     e => {}
    //   );
    // });

    // Rx.Observable.interval(3000).subscribe(() => {
    //   dashBoardDevices
    //     .persistFailedDeviceTransaction$({
    //       et: "DeviceMainAppUsosTranspCountReported",
    //       etv: 1,
    //       at: "Device",
    //       aid: `sn0000-000${Math.floor(Math.random() * 10)}-TEST`,
    //       data: {
    //         count: Math.floor(Math.random() * 5 + 1),
    //         timestamp: Date.now()
    //       },
    //       user: "SYSTEM.DevicesReport.devices-report-handler",
    //       timestamp: Date.now(),
    //       av: 174,
    //       _id: "5ad7cd023a8ce443f84f8f8c"
    //     })
    //     .subscribe(r => {}, e => {});
    // });

    // Rx.Observable.interval(3000).subscribe(() => {
    //   dashBoardDevices.persistSuccessDeviceTransaction$({
    //     et: "DeviceMainAppUsosTranspCountReported",
    //     etv: 1,
    //     at: "Device",
    //     aid: `sn0000-000${Math.floor(Math.random() * 10)}-TEST`,
    //     data: {
    //       count: Math.floor(Math.random() * 11 + 1),
    //       timestamp: Date.now()
    //     },
    //     user: "SYSTEM.DevicesReport.devices-report-handler",
    //     timestamp: Date.now(),
    //     av: 174,
    //     _id: "5ad7cd023a8ce443f84f8f8c"
    //   })
    //   .subscribe(r => {}, e => {});
    // });

    ////##################################################################
    ////##################################################################

    //default error handler
    const onErrorHandler = error => {
      console.error("Error handling  EventStore incoming event", error);
      process.exit(1);
    };
    //default onComplete handler
    const onCompleteHandler = () => {
      () => console.log("EventStore incoming event subscription completed");
    };
    console.log("EventStoreService starting ...");

    return Rx.Observable.from(this.aggregateEventsArray)
            .map(aggregateEvent => { return { ...aggregateEvent, onErrorHandler, onCompleteHandler } })
            .map(params => this.subscribeEventHandler(params));
  }

  /**
   * Stops listening to the Event store
   * Returns observable that resolves to each unsubscribed subscription as string
   */
  stop$() {
    return Rx.Observable.from(this.subscriptions).map(subscription => {
      subscription.subscription.unsubscribe();
      return `Unsubscribed: aggregateType=${aggregateType}, eventType=${eventType}, handlerName=${handlerName}`;
    });
  }

  /**
     * Create a subscrition to the event store and returns the subscription info     
     * @param {{aggregateType, eventType, onErrorHandler, onCompleteHandler}} params
     * @return { aggregateType, eventType, handlerName  }
     */
    subscribeEventHandler({ aggregateType, eventType, onErrorHandler, onCompleteHandler }) {
      const handler = this.functionMap[eventType];
      const subscription =
          //MANDATORY:  AVOIDS ACK REGISTRY DUPLICATIONS
          eventSourcing.eventStore.ensureAcknowledgeRegistry$(aggregateType)
              .mergeMap(() => eventSourcing.eventStore.getEventListener$(aggregateType,mbeKey))
              .filter(evt => evt.et === eventType)
              .mergeMap(evt => Rx.Observable.concat(
                  handler.fn.call(handler.obj, evt),
                  //MANDATORY:  ACKWOWLEDGE THIS EVENT WAS PROCESSED
                  eventSourcing.eventStore.acknowledgeEvent$(evt, mbeKey),
              ))
              .subscribe(
                  (evt) => {
                    // console.log(`EventStoreService: ${eventType} process: ${evt}`);
                  },
                  onErrorHandler,
                  onCompleteHandler
              );
      this.subscriptions.push({ aggregateType, eventType, handlerName: handler.fn.name, subscription });
      return { aggregateType, eventType, handlerName: `${handler.obj.name}.${handler.fn.name}` };
  }

  /**
  * Starts listening to the EventStore
  * Returns observable that resolves to each subscribe agregate/event
  *    emit value: { aggregateType, eventType, handlerName}
  */
  syncState$() {
      return Rx.Observable.from(this.aggregateEventsArray)
          .concatMap(params => this.subscribeEventRetrieval$(params))
  }



  /**
   * Create a subscrition to the event store and returns the subscription info     
   * @param {{aggregateType, eventType, onErrorHandler, onCompleteHandler}} params
   * @return { aggregateType, eventType, handlerName  }
   */
  subscribeEventRetrieval$({ aggregateType, eventType }) {
      const handler = this.functionMap[eventType];
      //MANDATORY:  AVOIDS ACK REGISTRY DUPLICATIONS
      return eventSourcing.eventStore.ensureAcknowledgeRegistry$(aggregateType)
          .switchMap(() => eventSourcing.eventStore.retrieveUnacknowledgedEvents$(aggregateType, mbeKey))
          .filter(evt => evt.et === eventType)
          .concatMap(evt => Rx.Observable.concat(
              handler.fn.call(handler.obj, evt),
              //MANDATORY:  ACKWOWLEDGE THIS EVENT WAS PROCESSED
              eventSourcing.eventStore.acknowledgeEvent$(evt, mbeKey)
          ));
  }

   /**
   * Generates a map that assocs each AggretateType withs its events
   */
  generateAggregateEventsArray() {
    return [
      {
        aggregateType: "Device",
        eventType: "DeviceConnected"
      },
      // {
      //   aggregateType: "Device",
      //   eventType: "DeviceDisconnected"
      // },
      {
        aggregateType: "Device",
        eventType: "DeviceDeviceStateReported"
      },
      {
        aggregateType: "Device",
        eventType: "DeviceCpuUsageAlarmActivated"
      },
      {
        aggregateType: "Device",
        eventType: "DeviceRamuUsageAlarmActivated"
      },
      {
        aggregateType: "Device",
        eventType: "DeviceTemperatureAlarmActivated"
      },
      {
        aggregateType: "Device",
        eventType: "DeviceLowVoltageAlarmReported"
      },
      {
        aggregateType: "Device",
        eventType: "DeviceHighVoltageAlarmReported"
      },
      {
        aggregateType: "Device",
        eventType: "DeviceMainAppUsosTranspCountReported"
      },
      {
        aggregateType: "Device",
        eventType: "DeviceMainAppErrsTranspCountReported"
      },
      {
        aggregateType: "Cronjob",
        eventType: "CleanDashBoardDevicesHistoryJobTriggered"
      }
    ]
  }
}

 

module.exports = () => {
  if (!instance) {
    instance = new EventStoreService();
    console.log("NEW  EventStore instance  !!");
  }
  return instance;
};

