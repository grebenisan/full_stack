import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SuspectCriteriaListComponent } from './suspect-criteria-list.component';

describe('SuspectCriteriaListComponent', () => {
  let component: SuspectCriteriaListComponent;
  let fixture: ComponentFixture<SuspectCriteriaListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SuspectCriteriaListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SuspectCriteriaListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
