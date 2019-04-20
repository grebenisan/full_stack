import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { DgSensiMainComponent } from './dg-sensi-main/dg-sensi-main.component';
import { SuspectCriteriaMainComponent } from './suspect-criteria-main/suspect-criteria-main.component';
import { WelcomeComponent } from './welcome/welcome.component';
import { AuthComponent } from './auth/auth.component';
import { AuthGuardService } from './auth-guard.service';

const routes: Routes = [
  { path: '', redirectTo: 'welcome', pathMatch: 'full' },
  { path: 'welcome', component: WelcomeComponent },
  { path: 'dg_sensitive', component: DgSensiMainComponent },
  { path: 'suspect_criteria', component: SuspectCriteriaMainComponent },
  { path: 'auth', component: AuthComponent }
];


@NgModule({
  imports: [RouterModule.forRoot(routes)], // , { useHash: true }
  exports: [RouterModule]
})
export class AppRoutingModule { }
