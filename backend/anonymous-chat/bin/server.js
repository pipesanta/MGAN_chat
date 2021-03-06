'use strict'

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').load();
}

const eventSourcing = require('./tools/EventSourcing')();
const eventStoreService = require('./services/event-store/EventStoreService')();
const mongoDB = require('./data/MongoDB').singleton();
const anonymousChatDA = require('./data/AnonymousChatDA');
const graphQlService = require('./services/gateway/GraphQlService')();
const Rx = require('rxjs');

const start = () => {
    Rx.Observable.concat(
        eventSourcing.eventStore.start$(),
        eventStoreService.start$(),
        mongoDB.start$(),
        anonymousChatDA.start$(),
        graphQlService.start$()
    ).subscribe(
        (evt) => {
        },
        (error) => {
            console.error('Failed to start', error);
            process.exit(1);
        },
        () => console.log('anonymous-chat started')
    );
};

start();



