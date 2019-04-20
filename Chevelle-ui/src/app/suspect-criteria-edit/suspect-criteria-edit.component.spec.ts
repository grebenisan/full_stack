import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SuspectCriteriaEditComponent } from './suspect-criteria-edit.component';

describe('SuspectCriteriaEditComponent', () => {
  let component: SuspectCriteriaEditComponent;
  let fixture: ComponentFixture<SuspectCriteriaEditComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SuspectCriteriaEditComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SuspectCriteriaEditComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
