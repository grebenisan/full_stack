import { Colregexp, ColReg } from './colregexp';

export const FINCAPRVMSTR: Colregexp[] = [ 
{ "column_name": "EMAIL_ADDR", 
  "regexp_name": "email address", 
  "col_regexp_desc": "email address for email_addr", 
  "regexp": "^(?=[a-zA-Z0-9._-]*@).*(?=[^a-z0-9A-Z._-]*[@]).*(?<![.])@.*", 
  "col_id": "24335128", 
  "reg_exp_id": "2", 
  "dt_cq_id": "233" 
}, 
{ "column_name": "account_nbr", 
  "regexp_name": "Account Number", 
  "col_regexp_desc": 
  "Account Number for account_nbr", 
  "regexp": "^[A-Z0-9<]{6,26}( +)|^[A-Z0-9<]{6,26}$", 
  "col_id": "25134704", 
  "reg_exp_id": "13", 
  "dt_cq_id": "39884" 
}
];

export let COLREGLIST: ColReg[] = 
[
  { "ColumnName": "customer", "RegexpName": "name", "ColRegName": "NAME", "Regexp": "^[a-zA-Z]{1,15}©s?[a-zA-Z]{1,15}$", "ColumnId": "27545549", "RegexpId": "5", "ColRegId": "62498" }, 
  { "ColumnName": "nsc_code", "RegexpName": "Service Code", "ColRegName": "SERVICE CODE", "Regexp": "^[A-Za-z0-9]{3,8}$", "ColumnId": "27545559", "RegexpId": "37", "ColRegId": "62499" }, 
  { "ColumnName": "ship_to", "RegexpName": "Street Address", "ColRegName": "STREET ADDRESS", "Regexp": "(^©d+©s(?:[A-zA-Z0-9]+?)+©s(?:((Avenue|avenue|AVENUE)|(Lane|lane|LANE)|(Road|road|ROAD)|(Boulevard|boulevard|BOULEVARD)|(Drive|drive|DRIVE)|(Street|street|STREET)|(Court|court|COURT)|(Ave|ave|AVE)|(Dr|dr|DR)|(Rd|rd|RD)|(Blvd|blvd|BLVD)|(Ln|ln|LN)|(St|st|ST)|(Str|str/©STR)|(Ct|ct|CT))©.?)|((P©DO©D?|PO)+©.?))", "ColumnId": "27545560", "RegexpId": "39", "ColRegId": "62500" }
];

