import { ComponentFixture, TestBed } from '@angular/core/testing';

import { OptionsData } from './options-data';

describe('OptionsData', () => {
  let component: OptionsData;
  let fixture: ComponentFixture<OptionsData>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [OptionsData]
    })
    .compileComponents();

    fixture = TestBed.createComponent(OptionsData);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
