"use strict";

const Rx = require("rxjs");
const eventSourcing = require("../tools/EventSourcing")();
const Event = require("@nebulae/event-store").Event;
const anonymousChatDA = require("../data/AnonymousChatDA");
const broker = require("../tools/broker/BrokerFactory")();
const {
  PERMISSION_DENIED_ERROR
} = require("../tools/ErrorCodes");
const MATERIALIZED_VIEW_TOPIC = "materialized-view-updates";
const {
  CustomError,
  DefaultError
} = require("../tools/customError");


/**
 * Singleton instance
 */
let instance;

class AnonymousChat {
  constructor() {
    // this.initHelloWorldEventGenerator();
  }


  // initHelloWorldEventGenerator(){
  //   Rx.Observable.interval(1000)
  //   .take(512)
  //   .map(id => "Felipe Santa" + Math.floor(Math.random() *10) )    
  //   .mergeMap(evt => {
  //     return broker.send$(MATERIALIZED_VIEW_TOPIC, 'onNewMsgArrived', evt);
  //   }).subscribe(
  //     (evt) => {},
  //     (err) => console.error('Gateway GraphQL sample event sent ERROR, please remove'),
  //     () => console.log('Gateway GraphQL sample event sending STOPPED, please remove'),
  //   );
  // }
  
  getMessages$({ args, jwt }, authToken){
    return anonymousChatDA.searchAllMessages$()
    .mergeMap(mongoArrayResul => {
      return Rx.Observable.from(mongoArrayResul)
      .map(doc => doc.body)
    })
    .toArray()
    .do(r => console.log(r))
    .mergeMap(rawResponse => this.buildSuccessResponse$(rawResponse))
    .catch(err => this.errorHandler$(err));
  }


  sendMessage$({ args, jwt }, authToken) {    
    console.log("sendMessage ====>", args);
    return eventSourcing.eventStore.emitEvent$(
      new Event({
        eventType: "anonymousMessageArrived",
        eventTypeVersion: 1,
        aggregateType: "ChatMessage",
        aggregateId: Date.now(),
        data: args,
        user: authToken.preferred_username
      })
    )    
      .map(r => {
        return "Message sent"
      })
      .mergeMap(rawResponse => this.buildSuccessResponse$(rawResponse))
      .catch(err => this.errorHandler$(err));
  }



  //#region  mappers for API responses
  errorHandler$(err) {
    return Rx.Observable.of(err)
      .map(err => {
        const exception = { data: null, result: {} };
        const isCustomError = err instanceof CustomError;
        if(!isCustomError){
          err = new DefaultError(err)
        }
        exception.result = {
            code: err.code,
            error: {...err.getContent()}
          }
        return exception;
      });
  }

  
  buildSuccessResponse$(rawRespponse) {
    return Rx.Observable.of(rawRespponse)
      .map(resp => {
        return {
          data: resp,
          result: {
            code: 200
          }
        }
      });
  }

  //#endregion


}

module.exports = () => {
  if (!instance) {
    instance = new AnonymousChat();
    console.log(`${instance.constructor.name} Singleton created`);
  }
  return instance;
};
