import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GetSampleComponent } from './get-sample.component';

describe('GetSampleComponent', () => {
  let component: GetSampleComponent;
  let fixture: ComponentFixture<GetSampleComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GetSampleComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GetSampleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
