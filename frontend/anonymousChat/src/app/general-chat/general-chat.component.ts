import { Component, OnInit } from '@angular/core';
import { ViewChild } from '@angular/core';
import { ElementRef } from '@angular/core';

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

  constructor() { }

  ngOnInit() {
  }

  sendMsg(){
    const input = this.inputText.nativeElement.value; 
    if (input && input !== ''){
      const msgContainer = this.msgList.nativeElement;
      this.messages.push(input);
      this.inputText.nativeElement.value ='';      
      console.log( msgContainer.scrollHeight, msgContainer.scrollHeight - 500 ) ;
      msgContainer.scrollTo(0, msgContainer.scrollHeight - 500);
      // msgContainer.scrollIntoView(false);
    }
  }

  onKeyenter(evt: any){
    if(evt  && evt.key === 'Enter'){
      this.sendMsg();
    }
  }

}
