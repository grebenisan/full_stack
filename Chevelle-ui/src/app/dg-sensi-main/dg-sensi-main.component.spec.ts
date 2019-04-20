import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DgSensiMainComponent } from './dg-sensi-main.component';

describe('DgSensiMainComponent', () => {
  let component: DgSensiMainComponent;
  let fixture: ComponentFixture<DgSensiMainComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DgSensiMainComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DgSensiMainComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
