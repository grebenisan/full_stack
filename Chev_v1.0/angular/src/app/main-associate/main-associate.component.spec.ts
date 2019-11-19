import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MainAssociateComponent } from './main-associate.component';

describe('MainAssociateComponent', () => {
  let component: MainAssociateComponent;
  let fixture: ComponentFixture<MainAssociateComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MainAssociateComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MainAssociateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
