import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DgSensiRecordComponent } from './dg-sensi-record.component';

describe('DgSensiRecordComponent', () => {
  let component: DgSensiRecordComponent;
  let fixture: ComponentFixture<DgSensiRecordComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DgSensiRecordComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DgSensiRecordComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
