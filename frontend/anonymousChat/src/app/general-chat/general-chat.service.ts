import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import * as Rx from 'rxjs';
import { GatewayService } from '../api/gateway.service';
import {
  getAllMessages,
  sendMessage
} from './gql/anonymousChat';

@Injectable()
export class GeneralChatService {

  constructor(
     private gateway: GatewayService
  ) {

   }

   getAllMessages$() {
    return this.gateway.apollo
      .query<any>({
        query: getAllMessages,
        fetchPolicy: 'network-only',
        errorPolicy: 'all'
      });
  }

  sendMessage$(message: string){
    return this.gateway.apollo
    .mutate<any>({
      mutation: sendMessage,
      variables: {
        msg: message
      },
      errorPolicy: 'all'
    });
  }


}
