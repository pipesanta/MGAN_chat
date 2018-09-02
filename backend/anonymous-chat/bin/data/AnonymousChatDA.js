'use strict'

let mongoDB = undefined;
// const mongoDB = require('./MongoDB')();
const Rx = require('rxjs');
const CollectionName = "AnonymousChatMsg"; //please change


class AnonymousChatDA {

  static start$(mongoDbInstance) {
    return Rx.Observable.create((observer) => {
      if (mongoDbInstance) {
        mongoDB = mongoDbInstance;
        observer.next('using given mongo instance');
      } else {
        mongoDB = require('./MongoDB').singleton();
        observer.next('using singleton system-wide mongo instance');
      }
      observer.complete();
    });
  }

  static searchAllMessages$() {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection.find()
      .sort({ 'timestamp': 1 }).toArray());
  }

  static saveMessageSent$(msg, timestamp) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection.insert({
      body: msg,
      timestamp: timestamp
    })
    );
  }
}

module.exports = AnonymousChatDA 