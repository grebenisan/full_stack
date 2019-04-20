import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SuspectCriteriaMainComponent } from './suspect-criteria-main.component';

describe('SuspectCriteriaMainComponent', () => {
  let component: SuspectCriteriaMainComponent;
  let fixture: ComponentFixture<SuspectCriteriaMainComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SuspectCriteriaMainComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SuspectCriteriaMainComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
