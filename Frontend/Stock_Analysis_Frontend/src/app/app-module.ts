import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { HttpClientModule } from '@angular/common/http';
import { AppRoutingModule } from './app-routing-module';
import { App } from './app';
import { Layout } from './layout/layout';
import { Navbar } from './layout/navbar/navbar';
import { SideNav } from './layout/side-nav/side-nav';
import { Dashboard } from './dashboard/dashboard';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatSidenavModule } from '@angular/material/sidenav';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatListModule } from '@angular/material/list';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatSelectModule } from '@angular/material/select';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatFormFieldModule } from '@angular/material/form-field';
import { HistoricalData } from './historical-data/historical-data';
import { FormsModule } from '@angular/forms';
import { OptionsData } from './options-data/options-data';
import { HighchartsChartModule } from 'highcharts-angular';

@NgModule({
  declarations: [
    App,
    Layout,
    Navbar,
    SideNav,
    Dashboard,
    HistoricalData,
    OptionsData
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    HttpClientModule,
    AppRoutingModule,
    MatToolbarModule,
    MatSidenavModule,
    MatCardModule,
    MatIconModule,
    MatListModule,
    MatInputModule,
    MatButtonModule,
    MatSelectModule,
    MatProgressSpinnerModule,
    MatFormFieldModule,
    FormsModule,
    HighchartsChartModule
  ],
  providers: [],
  bootstrap: [App]
})
export class AppModule { }
