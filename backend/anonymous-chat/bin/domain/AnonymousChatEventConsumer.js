"use strict";

const Rx = require("rxjs");
const broker = require("../tools/broker/BrokerFactory")();
const MATERIALIZED_VIEW_TOPIC = "materialized-view-updates";
const anonymousChatDA = require("../data/AnonymousChatDA")

/**
 * Singleton instance
 */
let instance;

class UserEventConsumer {
    constructor() { }

    handleAnonymousMessageArrived$(evt){
        console.log("handleAnonymousMessageArrived", evt.data.msg);
        return anonymousChatDA.saveMessageSent$(evt.data.msg, evt.timestamp)
        .mergeMap(() => broker.send$(MATERIALIZED_VIEW_TOPIC, 'onNewMsgArrived', evt.data.msg))
    }
}

module.exports = () => {
    if (!instance) {
        instance = new UserEventConsumer();
        console.log(`${instance.constructor.name} Singleton created`);
    }
    return instance;
};