import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { Home } from './home/home';
import { Dashboard } from './dashboard/dashboard';
import { HistoricalData } from './historical-data/historical-data';
import { OptionsData } from './options-data/options-data';

const routes: Routes = [{
    path: '',
    children: [
      { path: '', component: Home },
      { path: 'dashboard', component: Dashboard },
      { path: 'HistoricalData', component: HistoricalData },
      { path: 'OptionsData', component: OptionsData},
    ]
  }];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
