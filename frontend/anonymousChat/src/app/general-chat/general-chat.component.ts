import { Component, OnInit } from '@angular/core';
import { ViewChild } from '@angular/core';
import { ElementRef } from '@angular/core';
import { GeneralChatService } from './general-chat.service';
import { map } from 'rxjs/operators';
import * as Rx from 'rxjs/Rx';

@Component({
  selector: 'app-general-chat',
  templateUrl: './general-chat.component.html',
  styleUrls: ['./general-chat.component.css']
})
export class GeneralChatComponent implements OnInit {
  @ViewChild("input") inputText: ElementRef;
  @ViewChild("msgList") msgList: ElementRef;
  
  messages: string[] = [
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',
    'HELLO',

  ];

  constructor(private generalChatService: GeneralChatService) { 
    
  }

  ngOnInit() {
    this.generalChatService.getAllMessages$()
    .pipe(
      map(r => r.data.getMessages)
    )
    .subscribe(
      (messages: string[]) => {
        messages.forEach(msg => {
          this.messages.push(msg)
        })
      },
      error => console.log(),
      () => console.log('Stream completed')
    )
  }





  sendMsg() {
    const input = this.inputText.nativeElement.value;
    if (input && input !== '') {
      const msgContainer = this.msgList.nativeElement;

      this.generalChatService.sendMessage$(input)
        .pipe(
          map(r => r.data.sendMessage)
        )
        .subscribe(
          response => {
            console.log(response);
            this.messages.push(input);
            this.inputText.nativeElement.value = '';
            console.log(msgContainer.scrollHeight, msgContainer.scrollHeight - 500);
            msgContainer.scrollTo(0, msgContainer.scrollHeight - 500);

          },
          error => console.log(error),
          () => console.log('send message stream finished')
        )
      // msgContainer.scrollIntoView(false);
    }
  }

  onKeyenter(evt: any){
    if(evt  && evt.key === 'Enter'){
      this.sendMsg();
    }
  }

}
