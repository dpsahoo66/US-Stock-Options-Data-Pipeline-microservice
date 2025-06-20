import { NgModule, provideBrowserGlobalErrorListeners } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
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
import { HistoricalData } from './historical-data/historical-data';
import { FormsModule } from '@angular/forms';
import { OptionsData } from './options-data/options-data';

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
    AppRoutingModule,
    MatToolbarModule,
    MatSidenavModule,
    MatCardModule,
    MatIconModule,
    MatListModule,
    MatInputModule,
    MatButtonModule,
    FormsModule
  ],
  providers: [
    provideBrowserGlobalErrorListeners()
  ],
  bootstrap: [App]
})
export class AppModule { }
