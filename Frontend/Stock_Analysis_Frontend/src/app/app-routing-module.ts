import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { HistoricalData } from './historical-data/historical-data';

const routes: Routes = [
  { path: '', component: HistoricalData },
  { path: 'HistoricalData', component: HistoricalData },
  { path: '**', redirectTo: '' }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
