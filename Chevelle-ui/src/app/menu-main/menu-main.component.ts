import { Component, OnInit } from '@angular/core';
import { MenuItem } from 'primeng/api';

@Component({
  selector: 'app-menu-main',
  templateUrl: './menu-main.component.html',
  styleUrls: ['./menu-main.component.css']
})
export class MenuMainComponent implements OnInit {

  items: MenuItem[];
  
  constructor() { }

  ngOnInit()
  {
    this.items =
    [
        {
            label: 'Home', routerLink: ['/welcome']
        }
        ,
        {
            label: 'Data Governance',
            items:
            [
                {
                    label: 'Sensitive columns', icon: 'pi pi-fw pi-star', routerLink: ['/dg_sensitive']
                },
                {
                    label: 'Suspect criteria', icon: 'pi pi-fw pi-search', routerLink: ['/suspect_criteria']
                }
            ]
        }
        ,
        {
            label: 'Help', command: (event) => {
                //event.originalEvent: Browser event
                //event.item: menuitem metadata
                alert('This screen will be implemented on a future release!');
            }
        }        
    ];
  }

}
