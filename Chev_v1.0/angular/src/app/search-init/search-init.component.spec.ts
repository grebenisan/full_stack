import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SearchInitComponent } from './search-init.component';

describe('SearchInitComponent', () => {
  let component: SearchInitComponent;
  let fixture: ComponentFixture<SearchInitComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SearchInitComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SearchInitComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
