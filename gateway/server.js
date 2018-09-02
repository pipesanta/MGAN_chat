'use strict'

require('dotenv').load();

const Rx = require('rxjs');
const express = require('express');
const bodyParser = require('body-parser');
const graphqlServer = require('apollo-server-express');
const graphqlExpress = graphqlServer.graphqlExpress;
const graphiqlExpress = graphqlServer.graphiqlExpress;
const graphqlTools = require('graphql-tools');
const gqlSchema = require('./graphql/index');
const broker = require('./broker/BrokerFactory')();
const cors = require('cors');
const graphql = require('graphql');
const execute = graphql.execute;
const subscribe = graphql.subscribe;
const http = require('http');
const SubscriptionServer = require('subscriptions-transport-ws').SubscriptionServer;
//This lib is the easiest way to validate through http using express
const expressJwt = require('express-jwt');


//Service Port
const PORT = process.env.GRAPHQL_END_POINT_PORT || 3000;
//graphql types compendium
const typeDefs = gqlSchema.types;
//graphql resolvers compendium
const resolvers = gqlSchema.resolvers;
//graphql schema = join types & resolvers
const schema = graphqlTools.makeExecutableSchema({ typeDefs, resolvers });

//Express Server
const server = express();
// bodyParser is needed just for POST.
server.use(cors());


//Validate JWT token throug Express HTTP
server.use(
    process.env.GRAPHQL_HTTP_END_POINT,
    bodyParser.json(),
    graphqlExpress(req => ({
        schema,
        context: {
            broker,
        }
    })));

// Expose GraphiQl interface
server.use(process.env.GRAPHIQL_HTTP_END_POINT, graphiqlExpress(
    {
        endpointURL: process.env.GRAPHQL_HTTP_END_POINT,
        subscriptionsEndpoint: `ws://${process.env.GRAPHQL_END_POINT_HOST}:${process.env.GRAPHQL_END_POINT_PORT}${process.env.GRAPHQL_WS_END_POINT}`
    }
));


// Wrap the Express server and combined with WebSockets
const ws = http.createServer(server);
ws.listen(PORT, () => {
    new SubscriptionServer(
        {
            execute,
            subscribe,
            schema,
            onConnect: async (connectionParams, webSocket, connectionContext) => {
                console.log(`GraphQL_WS.onConnect: origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url}`);
   
                return { broker, webSocket };
            },
            onDisconnect: (webSocket, connectionContext) => {
                if(webSocket.onUnSubscribe){
                    webSocket.onUnSubscribe.subscribe(
                        (evt) => console.log(`webSocket.onUnSubscribe: ${JSON.stringify({evt})};  origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url};`),
                        error => console.error(`GraphQL_WS.onDisconnect + onUnSubscribe; origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url}; Error: ${error.message}`, error),
                        () => console.log(`GraphQL_WS.onDisconnect + onUnSubscribe: Completed OK; origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url};`)
                    );
                }else{
                    console.log(`GraphQL_WS.onDisconnect; origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url}; WARN: no onUnSubscribe callback found`);
                }                
            },
            onOperationDone: webSocket => {
                console.log(`GraphQL_WS.onOperationDone ==================  ${Object.keys(webSocket)}`);
            },
        },
        {
            server: ws,
            path: process.env.GRAPHQL_WS_END_POINT,
        });
    console.log(`Apollo Server is now running on http://localhost:${PORT}`);
    console.log(`HTTP END POINT: http://${process.env.GRAPHQL_END_POINT_HOST}:${process.env.GRAPHQL_END_POINT_PORT}${process.env.GRAPHQL_HTTP_END_POINT}`);
    console.log(`WEBSOCKET END POINT: ws://${process.env.GRAPHQL_END_POINT_HOST}:${process.env.GRAPHQL_END_POINT_PORT}${process.env.GRAPHQL_WS_END_POINT}`);
    console.log(`GRAPHIQL PAGE: http://${process.env.GRAPHQL_END_POINT_HOST}:${process.env.GRAPHQL_END_POINT_PORT}${process.env.GRAPHIQL_HTTP_END_POINT}`);
});
