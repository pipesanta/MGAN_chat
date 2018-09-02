"use strict";

const dashBoardDevices = require("../../domain/DashBoardDevices")();
const broker = require("../../tools/broker/BrokerFactory")();
const Rx = require("rxjs");
const jsonwebtoken = require("jsonwebtoken");
const jwtPublicKey = process.env.JWT_PUBLIC_KEY.replace(/\\n/g, "\n");

let instance;

class GraphQlService {
  constructor() {
    this.functionMap = this.generateFunctionMap();
    this.subscriptions = [];
  }

  generateFunctionMap() {
    return {
      "gateway.graphql.query.getDashBoardDevicesAlarmReport": {
        fn: dashBoardDevices.getDashBoardDevicesAlarmReport$,
        obj: dashBoardDevices
      },
      "gateway.graphql.query.getDashBoardDevicesCurrentNetworkStatus": {
        fn: dashBoardDevices.getDashBoardDevicesCurrentNetworkStatus$,
        obj: dashBoardDevices
      },
      "gateway.graphql.query.getDeviceTransactionsGroupByTimeInterval": {
        fn: dashBoardDevices.getDeviceTransactionsGroupByTimeInterval$,
        obj: dashBoardDevices
      },
      "gateway.graphql.query.getCuencaNamesWithSuccessTransactionsOnInterval": {
        fn: dashBoardDevices.getCuencaNamesWithSuccessTransactionsOnInterval$,
        obj: dashBoardDevices
      },
      "gateway.graphql.query.getDeviceTransactionsGroupByGroupName": {
        fn: dashBoardDevices.getDeviceTransactionsGroupByGroupName$,
        obj: dashBoardDevices
      },
      "gateway.graphql.query.getDeviceDashBoardTotalAccount": {
        fn: dashBoardDevices.getDeviceDashBoardTotalAccount$,
        obj: dashBoardDevices
      }
    };
  }

  start$() {
    const onErrorHandler = error => {
      console.error("Error handling  GraphQl incoming event", error);
      process.exit(1);
    };

    //default onComplete handler
    const onCompleteHandler = () => {
      () => console.log("GraphQlService incoming event subscription completed");
    };
    console.log("GraphQl Service starting ...");

    return Rx.Observable.from([
      {
        aggregateType: "Device",
        messageType: "gateway.graphql.query.getDashBoardDevicesAlarmReport",
        onErrorHandler,
        onCompleteHandler
      },
      {
        aggregateType: "Device",
        messageType:
          "gateway.graphql.query.getDashBoardDevicesCurrentNetworkStatus",
        onErrorHandler,
        onCompleteHandler
      },
      {
        aggregateType: "Device",
        messageType:
          "gateway.graphql.query.getDeviceTransactionsGroupByTimeInterval",
        onErrorHandler,
        onCompleteHandler
      },
      {
        aggregateType: "Device",
        messageType:
          "gateway.graphql.query.getCuencaNamesWithSuccessTransactionsOnInterval",
        onErrorHandler,
        onCompleteHandler
      },
      {
        aggregateType: "Device",
        messageType:
          "gateway.graphql.query.getDeviceTransactionsGroupByGroupName",
        onErrorHandler,
        onCompleteHandler
      },
      {
        aggregateType: "Device",
        messageType: "gateway.graphql.query.getDeviceDashBoardTotalAccount",
        onErrorHandler,
        onCompleteHandler
      }
    ]).map(params => this.subscribeEventHandler(params));
  }

  subscribeEventHandler({
    aggregateType,
    messageType,
    onErrorHandler,
    onCompleteHandler
  }) {
    const handler = this.functionMap[messageType];
    const subscription = broker
      .getMessageListener$([aggregateType], [messageType])
      //decode and verify the jwt token
      .map(message => {
        return {
          authToken: jsonwebtoken.verify(message.data.jwt, jwtPublicKey),
          message
        };
      })
      //ROUTE MESSAGE TO RESOLVER
      .mergeMap(({ authToken, message }) =>
        handler.fn
          .call(handler.obj, message.data, authToken)
          // .do(r => console.log("############################", r))
          .map(response => {
            return {
              response,
              correlationId: message.id,
              replyTo: message.attributes.replyTo
            };
          })
      )
      //send response back if neccesary
      .mergeMap(({ response, correlationId, replyTo }) => {
        if (replyTo) {
          return broker.send$(
            replyTo,
            "gateway.graphql.Query.response",
            response,
            { correlationId }
          );
        } else {
          return Rx.Observable.of(undefined);
        }
      })
      .subscribe(
        msg => {
          // console.log(`GraphQlService: ${messageType} process: ${msg}`);
        },
        onErrorHandler,
        onCompleteHandler
      );
    this.subscriptions.push({
      aggregateType,
      messageType,
      handlerName: handler.fn.name,
      subscription
    });
    return {
      aggregateType,
      messageType,
      handlerName: `${handler.obj.name}.${handler.fn.name}`
    };
  }

  stop$() {
    Rx.Observable.from(this.subscriptions).map(subscription => {
      subscription.subscription.unsubscribe();
      return `Unsubscribed: aggregateType=${aggregateType}, eventType=${eventType}, handlerName=${handlerName}`;
    });
  }
}

module.exports = () => {
  if (!instance) {
    instance = new GraphQlService();
    console.log("NEW instance GraphQlService  !!");
  }
  return instance;
};
