import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RegexpListComponent } from './regexp-list.component';

describe('RegexpListComponent', () => {
  let component: RegexpListComponent;
  let fixture: ComponentFixture<RegexpListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RegexpListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RegexpListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
