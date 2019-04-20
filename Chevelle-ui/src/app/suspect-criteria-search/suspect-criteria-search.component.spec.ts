import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SuspectCriteriaSearchComponent } from './suspect-criteria-search.component';

describe('SuspectCriteriaSearchComponent', () => {
  let component: SuspectCriteriaSearchComponent;
  let fixture: ComponentFixture<SuspectCriteriaSearchComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SuspectCriteriaSearchComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SuspectCriteriaSearchComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
