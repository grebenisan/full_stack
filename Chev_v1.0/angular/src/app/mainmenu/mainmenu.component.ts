import { Component, OnInit } from '@angular/core';
import { MenubarModule } from 'primeng/menubar';
import { MenuModule } from 'primeng/menu';
import { MenuItem } from 'primeng/api';

@Component({
  selector: 'app-mainmenu',
  templateUrl: './mainmenu.component.html',
  styleUrls: ['./mainmenu.component.css']
})
export class MainmenuComponent implements OnInit {

  theitems: MenuItem[];
  constructor() { }

  ngOnInit() {
    this.theitems  = [
    {
      label: 'Data',
      icon: 'fa-database',
      items: [
          {label: 'Browser', icon: 'fa-book', routerLink: ['/welcome']},
          {label: 'Search', icon: 'fa-search', routerLink: ['/mainsearch']},
          {label: 'Associate RegExp', icon: 'fa-link', routerLink: '/mainassoc'},
          {label: 'View Sample', icon: 'fa-list-alt', routerLink: '/viewsample'}
             ]
    },
    {
      label: 'Runner',
      icon: 'fa-car',
      items: [
          {label: 'Run', icon: 'fa-motorcycle'},
          {label: 'Status', icon: 'fa-dashboard', command: (event) => {
            alert('Click event on ' + event.item.label);
          }}
             ]
    },
    {
    label: 'Scheduler',
    icon: 'fa-calendar',
    items: [
        {label: 'Create', icon: 'fa-edit'},
        {label: 'Status', icon: 'fa-clock-o'}
           ]
    },
    {
        label: 'RegExp',
        icon: 'fa-cubes',
        items: [
            {label: 'Build', icon: 'fa-building-o'},
            {label: 'Validate', icon: 'fa-check', routerLink: '/validateregexp'},
            {label: 'Associate', icon: 'fa-link', routerLink: '/mainassoc'}
               ]
    },
    {
        label: 'Tools',
        icon: 'fa-wrench',
        items: [
            {label: 'Security', icon: 'fa-lock'},
            {label: 'Options', icon: 'fa-gears'}
               ]
    },
    {
        label: 'Help',
        icon: 'fa-question',
        items: [
            {label: 'Tutorial', icon: 'fa-mortar-board'},
            {label: 'API', icon: 'fa-newspaper-o'},
            {label: 'Team', icon: 'fa-users'},
            {label: 'About', icon: 'fa-list-alt'}
               ]
    }
];
}

}
