import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EntityDefComponent } from './entity-def.component';

describe('EntityDefComponent', () => {
  let component: EntityDefComponent;
  let fixture: ComponentFixture<EntityDefComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EntityDefComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EntityDefComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
