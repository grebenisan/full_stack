import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ColregDetailComponent } from './colreg-detail.component';

describe('ColregDetailComponent', () => {
  let component: ColregDetailComponent;
  let fixture: ComponentFixture<ColregDetailComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ColregDetailComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ColregDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
