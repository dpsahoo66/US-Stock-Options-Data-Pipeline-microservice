import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HistoricalData } from './historical-data';

describe('HistoricalData', () => {
  let component: HistoricalData;
  let fixture: ComponentFixture<HistoricalData>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [HistoricalData]
    })
    .compileComponents();

    fixture = TestBed.createComponent(HistoricalData);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
