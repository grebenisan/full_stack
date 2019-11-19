import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ValidateRegexpComponent } from './validate-regexp.component';

describe('ValidateRegexpComponent', () => {
  let component: ValidateRegexpComponent;
  let fixture: ComponentFixture<ValidateRegexpComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ValidateRegexpComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ValidateRegexpComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
