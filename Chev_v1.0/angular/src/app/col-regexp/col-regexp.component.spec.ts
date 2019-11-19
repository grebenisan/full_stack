import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ColRegexpComponent } from './col-regexp.component';

describe('ColRegexpComponent', () => {
  let component: ColRegexpComponent;
  let fixture: ComponentFixture<ColRegexpComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ColRegexpComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ColRegexpComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
