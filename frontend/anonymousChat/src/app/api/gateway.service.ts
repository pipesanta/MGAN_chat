import { Injectable } from '@angular/core';
import { Apollo } from 'apollo-angular';
import { HttpLink } from 'apollo-angular-link-http';
import { setContext } from 'apollo-link-context';
import { InMemoryCache } from 'apollo-cache-inmemory';
import { WebSocketLink } from 'apollo-link-ws';
import { split } from 'apollo-link';
import { getMainDefinition } from 'apollo-utilities';

@Injectable()
export class GatewayService {

  constructor(
    public apollo: Apollo,
    private httpLink: HttpLink,
  ) {

    // HTTP end-point
    const http = httpLink.create({ uri: 'http://localhost:3000/api/gateway/graphql/http' });




      // Add the JWT token in every request
      const auth = setContext((request, previousContext) => ({
        authorization: 'false'
      }));

      // Create a WebSocket link:
      const ws = new WebSocketLink({
        uri: "ws://localhost:3000/api/gateway/graphql/ws",
        options: {
          reconnect: true,
          connectionParams: {
            authToken: 'false',
          },
        }
      });



      // using the ability to split links, you can send data to each link
      // depending on what kind of operation is being sent
      const link = split(
        // split based on operation type
        ({ query }) => {
          const definition = getMainDefinition(query);
          return definition.kind === 'OperationDefinition' && definition.operation === 'subscription';
        },
        ws,
        auth.concat(http),
      );


      // Create Apollo client
      this.apollo.create({
        link,
        cache: new InMemoryCache()
      });

  }
}
