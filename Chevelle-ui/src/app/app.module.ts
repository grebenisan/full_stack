import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms'; // <-- NgModel lives here

import { DatePipe } from '@angular/common';

import { HttpClientModule } from '@angular/common/http';

import { MenubarModule } from 'primeng/menubar';
import { MenuModule } from 'primeng/menu';
import { TableModule } from 'primeng/table';
import { ProgressBarModule } from 'primeng/progressbar';
import { DialogModule } from 'primeng/dialog';
import { ButtonModule } from 'primeng/button';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { DgSensiListComponent } from './dg-sensi-list/dg-sensi-list.component';
import { DgSensiMainComponent } from './dg-sensi-main/dg-sensi-main.component';
import { DgSensiRecordComponent } from './dg-sensi-record/dg-sensi-record.component';
import { DgSensiSearchComponent } from './dg-sensi-search/dg-sensi-search.component';
import { MenuMainComponent } from './menu-main/menu-main.component';
import { SuspectCriteriaEditComponent } from './suspect-criteria-edit/suspect-criteria-edit.component';
import { SuspectCriteriaListComponent } from './suspect-criteria-list/suspect-criteria-list.component';
import { SuspectCriteriaMainComponent } from './suspect-criteria-main/suspect-criteria-main.component';
import { SuspectCriteriaSearchComponent } from './suspect-criteria-search/suspect-criteria-search.component';
import { WelcomeComponent } from './welcome/welcome.component';
import { AuthComponent } from './auth/auth.component';

@NgModule({
  declarations: [
    AppComponent,
    DgSensiListComponent,
    DgSensiMainComponent,
    DgSensiRecordComponent,
    DgSensiSearchComponent,
    MenuMainComponent,
    SuspectCriteriaEditComponent,
    SuspectCriteriaListComponent,
    SuspectCriteriaMainComponent,
    SuspectCriteriaSearchComponent,
    WelcomeComponent,
    AuthComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    FormsModule,
    HttpClientModule,
    MenubarModule,
    MenuModule,
    TableModule,
    ProgressBarModule,
    DialogModule,
    ButtonModule,
    AppRoutingModule
  ],
  providers: [ DatePipe// no need to place any providers due to the `providedIn` flag...
            ],
  bootstrap: [AppComponent]
})
export class AppModule { }
