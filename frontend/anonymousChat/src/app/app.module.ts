import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { AppComponent } from './app.component';
import { MaterialModule } from './material.module';
import { GeneralChatComponent } from './general-chat/general-chat.component';
import { GeneralChatService } from './general-chat/general-chat.service';
import { GatewayService } from './api/gateway.service';
import { Apollo, ApolloModule } from 'apollo-angular';
import { HttpLink, HttpLinkModule } from 'apollo-angular-link-http';
import { HttpClient, HttpHandler, HttpClientModule } from '@angular/common/http';


@NgModule({
  declarations: [
    AppComponent,
    GeneralChatComponent,
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    HttpClientModule,
    MaterialModule,
    ApolloModule,
    HttpLinkModule,
  ],
  providers: [GeneralChatService, GatewayService],
  bootstrap: [AppComponent]
})
export class AppModule { }
