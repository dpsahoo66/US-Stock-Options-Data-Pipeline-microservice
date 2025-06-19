import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { Layout } from './layout/layout';
import { Dashboard } from './dashboard/dashboard';
import { HistoricalData } from './historical-data/historical-data';
import { OptionsData } from './options-data/options-data';

const routes: Routes = [{
    path: '',
    component: Layout,
    children: [
      { path: '', component: Dashboard },
      { path: 'HistoricalData', component: HistoricalData },
      { path: 'OptionsData', component: OptionsData},
    ]
  }];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
