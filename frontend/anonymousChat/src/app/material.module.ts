import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatButtonModule } from '@angular/material';
import {MatIconModule} from '@angular/material/icon';

const MODULES = [MatButtonModule, MatIconModule];

@NgModule({
  imports: MODULES,
  exports: MODULES,
})
export class MaterialModule { }