import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DgSensiListComponent } from './dg-sensi-list.component';

describe('DgSensiListComponent', () => {
  let component: DgSensiListComponent;
  let fixture: ComponentFixture<DgSensiListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DgSensiListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DgSensiListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
