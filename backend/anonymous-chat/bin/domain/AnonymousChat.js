"use strict";

const Rx = require("rxjs");
const eventSourcing = require("../tools/EventSourcing")();
const Event = require("@nebulae/event-store").Event;
const anonymousChatDA = require("../data/AnonymousChatDA");


/**
 * Singleton instance
 */
let instance;

class AnonymousChat {
  constructor() {
  }

  
  getMessages$({ args, jwt }, authToken){
    console.log('Query by all messages');
    return anonymousChatDA.searchAllMessages$()
    .mergeMap(mongoArrayResul => {
      return Rx.Observable.from(mongoArrayResul)
      .map(doc => doc.body)
    })
    .toArray()
    .mergeMap(rawResponse => this.buildSuccessResponse$(rawResponse))
    .catch(err => this.errorHandler$(err));
  }


  sendMessage$({ args, jwt }, authToken) {    
    console.log("Sending Message ====>", args);
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
