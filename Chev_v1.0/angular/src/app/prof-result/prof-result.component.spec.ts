import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProfResultComponent } from './prof-result.component';

describe('ProfResultComponent', () => {
  let component: ProfResultComponent;
  let fixture: ComponentFixture<ProfResultComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProfResultComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProfResultComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
