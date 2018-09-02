"use strict";

const mongoDB = require("./MongoDB")();
const Rx = require("rxjs");
const CollectionName = "CommonVariables";

class CommonVarsDA {
  static getVarValue$(keyVar) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection.findOne({ key: keyVar }, {value: 1}));
  }

  static updateVarValue$(key, value){
    return this.updateOne$(
        { key: key },
        { $set: { value: value } },
        { upsert: true }
    );
  }


  static updateOne$(filter, update, options) {
    const collection = mongoDB.db.collection(CollectionName);
    return Rx.Observable.defer(() => collection.updateOne(filter, update, options));
  }
}

module.exports = CommonVarsDA;
