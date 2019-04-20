import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DgSensiSearchComponent } from './dg-sensi-search.component';

describe('DgSensiSearchComponent', () => {
  let component: DgSensiSearchComponent;
  let fixture: ComponentFixture<DgSensiSearchComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DgSensiSearchComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DgSensiSearchComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
