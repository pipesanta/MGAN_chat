import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { GeneralChatComponent } from './general-chat/general-chat.component';

const routes: Routes = [
  {
    path: '', component: GeneralChatComponent
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
