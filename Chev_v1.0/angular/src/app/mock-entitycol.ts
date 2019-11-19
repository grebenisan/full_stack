import { Entitycol } from './entitycol';
import { Column } from './column';

export const VEHALLIEDINV: Entitycol[] = [
    { "col_name": "veh_evnt_lfcycl_id", "data_type": "VARCHAR", "data_length": "20", "is_pk": "Yes", "is_fk": "Yes", "is_null": "Not Null" },
    { "col_name": "veh_evnt_cd", "data_type": "VARCHAR", "data_length": "10", "is_pk": "Yes", "is_fk": "Yes", "is_null": "Not Null" },
    { "col_name": "veh_evnt_src_sys_id", "data_type": "VARCHAR", "data_length": "20", "is_pk": "Yes", "is_fk": "Yes", "is_null": "Not Null" },
    { "col_name": "veh_evnt_eff_ts", "data_type": "TIMESTAMP", "data_length": "6", "is_pk": "Yes", "is_fk": "Yes", "is_null": "Not Null" },
    { "col_name": "veh_evnt_txn_seq", "data_type": "INTEGER", "data_length": "0", "is_pk": "Yes", "is_fk": "Yes", "is_null": "Not Null" },
    { "col_name": "veh_id_nbr", "data_type": "VARCHAR", "data_length": "20", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "veh_nbr", "data_type": "VARCHAR", "data_length": "20", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "veh_nbr_crt_dt", "data_type": "DATE", "data_length": "0", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "veh_sales_ord_nbr", "data_type": "VARCHAR", "data_length": "20", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "veh_evnt_ind", "data_type": "CHAR", "data_length": "1", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "biz_txn_cd", "data_type": "VARCHAR", "data_length": "10", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "biz_txn_src", "data_type": "VARCHAR", "data_length": "8", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "biz_txn_appl_rslt", "data_type": "CHAR", "data_length": "2", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "veh_evnt_tm", "data_type": "VARCHAR", "data_length": "8", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "veh_evnt_term_ts", "data_type": "TIMESTAMP", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "evnt_proc_upd_ts", "data_type": "TIMESTAMP", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "evnt_proc_orig_ts", "data_type": "TIMESTAMP", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "veh_lease_pgm_type", "data_type": "CHAR", "data_length": "1", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "veh_purch_pgm_type", "data_type": "CHAR", "data_length": "1", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "veh_pymt_type", "data_type": "VARCHAR", "data_length": "5", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "veh_deliv_type", "data_type": "VARCHAR", "data_length": "4", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "veh_deliv_type_cd_set_id", "data_type": "INTEGER", "data_length": "0", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "rptd_veh_deliv_ts", "data_type": "TIMESTAMP", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "prjctd_odom_rdng_nbr", "data_type": "INTEGER", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "deliv_odom_rdng_nbr", "data_type": "INTEGER", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "odom_rdng_uom_cd", "data_type": "VARCHAR", "data_length": "15", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "biz_assoc_id", "data_type": "VARCHAR", "data_length": "50", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "biz_func_cd", "data_type": "INTEGER", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "sell_src_cd", "data_type": "CHAR", "data_length": "2", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "orig_carr_dsptch_loc_cd", "data_type": "VARCHAR", "data_length": "7", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "dest_carr_dsptch_loc_cd", "data_type": "VARCHAR", "data_length": "7", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "sales_org_cd", "data_type": "VARCHAR", "data_length": "4", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "fleet_id", "data_type": "VARCHAR", "data_length": "20", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "site_type_cd", "data_type": "VARCHAR", "data_length": "5", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "rgstr_nbr", "data_type": "VARCHAR", "data_length": "20", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "rgstr_proc_ts", "data_type": "TIMESTAMP", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "sales_usr_id", "data_type": "VARCHAR", "data_length": "20", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "spl_alw_claim_cd", "data_type": "VARCHAR", "data_length": "20", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "sale_ord_veh_actv_cd", "data_type": "VARCHAR", "data_length": "10", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "sale_ord_veh_actv_txn_seq_nbr", "data_type": "INTEGER", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "sale_ord_veh_actv_st_cd", "data_type": "VARCHAR", "data_length": "10", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "sale_ord_veh_st_txn_seq_nbr", "data_type": "INTEGER", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "sale_ord_veh_actv_stat_rsn_cd", "data_type": "VARCHAR", "data_length": "10", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "biz_txn_seq_nbr", "data_type": "INTEGER", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "veh_use_type_id", "data_type": "VARCHAR", "data_length": "20", "is_pk": "No", "is_fk": "Yes", "is_null": "Null" },
    { "col_name": "src_sys_crt_ts", "data_type": "TIMESTAMP", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
    { "col_name": "src_sys_id", "data_type": "VARCHAR", "data_length": "20", "is_pk": "Yes", "is_fk": "No", "is_null": "Not Null" },
    { "col_name": "src_sys_iud_cd", "data_type": "CHAR", "data_length": "1", "is_pk": "No", "is_fk": "No", "is_null": "Not Null" },
    { "col_name": "src_sys_upd_by", "data_type": "VARCHAR", "data_length": "30", "is_pk": "No", "is_fk": "No", "is_null": "Not Null" },
    { "col_name": "src_sys_upd_gmt_ts", "data_type": "TIMESTAMP", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Not Null" },
    { "col_name": "src_sys_upd_ts", "data_type": "TIMESTAMP", "data_length": "0", "is_pk": "No", "is_fk": "No", "is_null": "Not Null" }
    ];

    export const EZENTITYCOLS: Entitycol[] = [
        { "col_name": "address_detail_id", "data_type": "NUMBER", "data_length": "11", "is_pk": "Yes", "is_fk": "No", "is_null": "Null" },
        { "col_name": "location_id", "data_type": "NUMBER", "data_length": "11", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
        { "col_name": "address_field_data", "data_type": "VARCHAR", "data_length": "1024", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
        { "col_name": "address_field_id", "data_type": "NUMBER", "data_length": "11", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
        { "col_name": "country_cd", "data_type": "VARCHAR", "data_length": "8", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
        { "col_name": "created_by", "data_type": "VARCHAR", "data_length": "80", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
        { "col_name": "created_timstm", "data_type": "TIMESTAMP", "data_length": "6", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
        { "col_name": "updated_by", "data_type": "VARCHAR", "data_length": "80", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
        { "col_name": "updated_timstm", "data_type": "TIMESTAMP", "data_length": "6", "is_pk": "No", "is_fk": "No", "is_null": "Null" },
        { "col_name": "locale_id", "data_type": "NUMBER", "data_length": "8", "is_pk": "No", "is_fk": "No", "is_null": "Null" }
    ];

    export const COLUMNLIST: Column[] =
    [
        { "Id": "27545549", "ColumnName": "customer", "BusinessName": "customer", "SourceColumnName": "customer", "Description": "customer", "DataType": "STRING", "DataLength": "", "IsNullable": "true", "IsPrimaryKey": "", "IsForeignKey": "", "IsMultibyte": "false", "IsCertified": "false", "DataClassification": "", "UniqueRecords": "4401", "NullRecords": "4400", "DefaultRecords": "", "MinValue": "0002", "MaxValue": "9921", "MeanValue": "2739.7437879883496", "StDev": "0.5", "MinLength": "4", "MaxLength": "4", "AvgLength": "4.0", "StatsUpdatedDate": "2018-03-06 18:37:25.727" }, 
        { "Id": "27545550", "ColumnName": "customer_order", "BusinessName": "customer_order", "SourceColumnName": "customer_order", "Description": "customer_order", "DataType": "STRING", "DataLength": "", "IsNullable": "true", "IsPrimaryKey": "", "IsForeignKey": "", "IsMultibyte": "false", "IsCertified": "false", "DataClassification": "", "UniqueRecords": "4401", "NullRecords": "4400", "DefaultRecords": "", "MinValue": "0002", "MaxValue": "9921", "MeanValue": "2739.7437879883496", "StDev": "0.5", "MinLength": "4", "MaxLength": "4", "AvgLength": "4.0", "StatsUpdatedDate": "2018-03-06 18:37:25.727" }, 
        { "Id": "27545558", "ColumnName": "last_reason_code", "BusinessName": "last_reason_code", "SourceColumnName": "last_reason_code", "Description": "last_reason_code", "DataType": "STRING", "DataLength": "", "IsNullable": "true", "IsPrimaryKey": "", "IsForeignKey": "", "IsMultibyte": "false", "IsCertified": "false", "DataClassification": "", "UniqueRecords": "4401", "NullRecords": "4400", "DefaultRecords": "", "MinValue": "0002", "MaxValue": "9921", "MeanValue": "2739.7437879883496", "StDev": "0.5", "MinLength": "4", "MaxLength": "4", "AvgLength": "4.0", "StatsUpdatedDate": "2018-03-06 18:37:25.727" }, 
        { "Id": "27545559", "ColumnName": "nsc_code", "BusinessName": "nsc_code", "SourceColumnName": "nsc_code", "Description": "nsc_code", "DataType": "STRING", "DataLength": "", "IsNullable": "true", "IsPrimaryKey": "", "IsForeignKey": "", "IsMultibyte": "false", "IsCertified": "false", "DataClassification": "", "UniqueRecords": "4401", "NullRecords": "4400", "DefaultRecords": "", "MinValue": "0002", "MaxValue": "9921", "MeanValue": "2739.7437879883496", "StDev": "0.5", "MinLength": "4", "MaxLength": "4", "AvgLength": "4.0", "StatsUpdatedDate": "2018-03-06 18:37:25.727" }, 
        { "Id": "27545560", "ColumnName": "ship_to", "BusinessName": "ship_to", "SourceColumnName": "ship_to", "Description": "ship_to", "DataType": "STRING", "DataLength": "", "IsNullable": "true", "IsPrimaryKey": "", "IsForeignKey": "", "IsMultibyte": "false", "IsCertified": "false", "DataClassification": "", "UniqueRecords": "4401", "NullRecords": "4400", "DefaultRecords": "", "MinValue": "0002", "MaxValue": "9921", "MeanValue": "2739.7437879883496", "StDev": "0.5", "MinLength": "4", "MaxLength": "4", "AvgLength": "4.0", "StatsUpdatedDate": "2018-03-06 18:37:25.727" }, 
        { "Id": "27545561", "ColumnName": "upddate", "BusinessName": "upddate", "SourceColumnName": "upddate", "Description": "upddate", "DataType": "STRING", "DataLength": "", "IsNullable": "true", "IsPrimaryKey": "", "IsForeignKey": "", "IsMultibyte": "false", "IsCertified": "false", "DataClassification": "", "UniqueRecords": "4401", "NullRecords": "4400", "DefaultRecords": "", "MinValue": "0002", "MaxValue": "9921", "MeanValue": "2739.7437879883496", "StDev": "0.5", "MinLength": "4", "MaxLength": "4", "AvgLength": "4.0", "StatsUpdatedDate": "2018-03-06 18:37:25.727" } 
    ];
        
