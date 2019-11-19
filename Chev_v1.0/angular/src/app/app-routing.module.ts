import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Routes } from '@angular/router';

import { WelcomeComponent } from './welcome/welcome.component';
import { AboutComponent } from './about/about.component';
import { SearchInitComponent } from './search-init/search-init.component';
import { MainSearchComponent } from './main-search/main-search.component';
import { MainAssociateComponent } from './main-associate/main-associate.component';
import { MainmenuComponent } from './mainmenu/mainmenu.component';
import { ColRegexpComponent } from './col-regexp/col-regexp.component';
import { EntityDefComponent } from './entity-def/entity-def.component';
import { EntityListComponent } from './entity-list/entity-list.component';
import { ProfResultComponent } from './prof-result/prof-result.component';
import { AssociateComponent } from './associate/associate.component';

import { GetSampleComponent } from './get-sample/get-sample.component';
import { SampleDataComponent } from './sample-data/sample-data.component';
import { MainSampleComponent } from './main-sample/main-sample.component';
import { ValidateRegexpComponent } from './validate-regexp/validate-regexp.component';

const routes: Routes = [
  { path: '', redirectTo: '/welcome', pathMatch: 'full' },
  { path: 'welcome', component: WelcomeComponent },
  { path: 'about', component: AboutComponent },
  { path: 'searchinit', component: SearchInitComponent  },
  { path: 'mainsearch', component: MainSearchComponent },
  { path: 'entitylist', component: EntityListComponent },
  { path: 'mainassoc', component: MainAssociateComponent },
  { path: 'associate', component: AssociateComponent },
  { path: 'viewsample', component: MainSampleComponent },  // just for testing
  { path: 'validateregexp', component: ValidateRegexpComponent }

];

@NgModule({
  imports: [
    CommonModule,
    RouterModule.forRoot(routes)
  ],
  exports: [ RouterModule ],
  declarations: []
})
export class AppRoutingModule { }
