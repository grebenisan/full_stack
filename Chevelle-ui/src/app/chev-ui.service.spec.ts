import { TestBed } from '@angular/core/testing';

import { ChevUiService } from './chev-ui.service';

describe('ChevUiService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: ChevUiService = TestBed.get(ChevUiService);
    expect(service).toBeTruthy();
  });
});
