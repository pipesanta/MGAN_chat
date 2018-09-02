const withFilter = require("graphql-subscriptions").withFilter;
const PubSub = require("graphql-subscriptions").PubSub;
const pubsub = new PubSub();
const Rx = require("rxjs");
const broker = require("../../broker/BrokerFactory")();

function getResponseFromBackEnd$(response) {
  return Rx.Observable.of(response).map(resp => {
    if (resp.result.code != 200) {
      const err = new Error();
      err.name = "Error";
      err.message = resp.result.error;
      // this[Symbol()] = resp.result.error;
      Error.captureStackTrace(err, "Error");
      throw err;
    }
    return resp.data;
  });
}

module.exports = {
  //// QUERY ///////

  Query: {
    getMessages(root, args, context) {
      console.log(root, args);
      return broker.forwardAndGetReply$(
          "ChatMessage",
          "gateway.graphql.query.getMessages",
          { root, args, jwt: context.encodedToken },
          2000
      )
      // return Rx.Observable.of({ result: { code: 200 }, data: ['mensaje_1', 'mensajae 2'] })
        .mergeMap(response => getResponseFromBackEnd$(response))
        .toPromise();
    }
  },

  //// MUTATIONS ///////
  Mutation: {
    sendMessage(root, args, context) {
      return context.broker.forwardAndGetReply$(
        "ChatMessage",
        "gateway.graphql.mutation.sendMessage",
        { root, args, jwt: context.encodedToken },
        2000
      )
      // return Rx.Observable.of({ result: { code: 200 }, data: 'texto de confirmacion' })
        // .catch(err => handleError$(err, "persistBasicInfoTag"))
        .mergeMap(response => getResponseFromBackEnd$(response))
        .toPromise();
    }
  },
  // SUBSCRIPTIONS ///////
  Subscription: {
    onNewMsgArrived: {
      subscribe: withFilter(
        (payload, variables, context, info) => {
          return pubsub.asyncIterator("onNewMsgArrived");
        },
        (payload, variables, context, info) => {
          return true;
        }
      )
    }
  }
};

//// SUBSCRIPTIONS SOURCES ////
const eventDescriptors = [
  {
    backendEventName: 'onNewMsgArrived',
    gqlSubscriptionName: 'onNewMsgArrived',
    //dataExtractor: (evt) => evt.data,// OPTIONAL, only use if needed
    //onError: (error, descriptor) => console.log(`Error processing ${descriptor.backendEventName}`),// OPTIONAL, only use if needed
    //onEvent: (evt, descriptor) => console.log(`Event of type  ${descriptor.backendEventName} arraived: ${JSON.stringify(evt)}`),// OPTIONAL, only use if needed
  },

];

/**
 * Connects every backend event to the right GQL subscription
 */
eventDescriptors.forEach(descriptor => {
  broker.getMaterializedViewsUpdates$([descriptor.backendEventName]).subscribe(
    evt => {
      if (descriptor.onEvent) {
        descriptor.onEvent(evt, descriptor);
      }
      const payload = {};
      payload[descriptor.gqlSubscriptionName] = descriptor.dataExtractor
        ? descriptor.dataExtractor(evt)
        : evt.data;
      pubsub.publish(descriptor.gqlSubscriptionName, payload);
    },

    error => {
      if (descriptor.onError) {
        descriptor.onError(error, descriptor);
      }
      console.error(`Error listening ${descriptor.gqlSubscriptionName}`, error);
    },

    () => console.log(`${descriptor.gqlSubscriptionName} listener STOPED`)
  );
});
