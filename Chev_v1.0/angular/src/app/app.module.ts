import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MenubarModule } from 'primeng/menubar';
import { MenuModule } from 'primeng/menu';
import { ProgressBarModule } from 'primeng/progressbar';

import { DataTableModule } from 'primeng/datatable';
import { SharedModule } from 'primeng/shared';
import { TableModule } from 'primeng/table';

import { HttpClientModule } from '@angular/common/http';

import { AppComponent } from './app.component';
import { WelcomeComponent } from './welcome/welcome.component';
import { AboutComponent } from './about/about.component';
import { MainmenuComponent } from './mainmenu/mainmenu.component';
import { SearchInitComponent } from './search-init/search-init.component';
import { ColRegexpComponent } from './col-regexp/col-regexp.component';
import { EntityDefComponent } from './entity-def/entity-def.component';
import { EntityListComponent } from './entity-list/entity-list.component';
import { MainSearchComponent } from './main-search/main-search.component';

import { SelectionService } from './selection.service';

import { AppRoutingModule } from './/app-routing.module';


import { TableDetailComponent } from './table-detail/table-detail.component';
import { ColregDetailComponent } from './colreg-detail/colreg-detail.component';
import { ProfResultComponent } from './prof-result/prof-result.component';
import { AssociateComponent } from './associate/associate.component';
import { MainAssociateComponent } from './main-associate/main-associate.component';
import { RegexpListComponent } from './regexp-list/regexp-list.component';
import { GetSampleComponent } from './get-sample/get-sample.component';
import { MainSampleComponent } from './main-sample/main-sample.component';
import { SampleDataComponent } from './sample-data/sample-data.component';
import { ValidateRegexpComponent } from './validate-regexp/validate-regexp.component';


@NgModule({
  declarations: [
    AppComponent,
    WelcomeComponent,
    AboutComponent,
    MainmenuComponent,
    SearchInitComponent,
    ColRegexpComponent,
    EntityDefComponent,
    EntityListComponent,
    MainSearchComponent,
    TableDetailComponent,
    ColregDetailComponent,
    ProfResultComponent,
    AssociateComponent,
    MainAssociateComponent,
    RegexpListComponent,
    GetSampleComponent,
    MainSampleComponent,
    SampleDataComponent,
    ValidateRegexpComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    MenubarModule,
    MenuModule,
    ProgressBarModule,
    DataTableModule,
    SharedModule,
    TableModule,
    HttpClientModule,
    AppRoutingModule
  ],
  providers: [SelectionService],
  bootstrap: [AppComponent]
})
export class AppModule { }
