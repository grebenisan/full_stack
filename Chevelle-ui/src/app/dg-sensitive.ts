export class DgSensitive {
    dg_status: string;
    table_nm: string;
    column_nm: string;
    schema_nm: string;
    db_nm: string;
    data_cls_nm: string;
    prof_start_ts: string;
    prof_end_ts: string;
    tot_row_cnt: number;
    sample_row_cnt: number;
    col_val_uniq_cnt: number;
    col_val_data_cls_cnt: number;
    col_val_data_cls_percent: number;
    col_max_val: string;
    col_min_val: string;
    col_avg_len: number;
    appl_regex_str: string;
    col_id: string;
    table_id: string;
    batch_id: number;
    crt_by: string;
    crt_ts: string;
    dg_upd_by: string;
    dg_upd_ts: string;
    row_cnt: number;

    constructor() {}

}

export class DgUpdateCol {
    dg_status: string;
    data_cls_nm: string;
    col_id: string;
    table_id: string;
    crt_ts: string;
    dg_upd_by: string;
    batch_id: number;

    constructor() {}
}

/*
export const DG_SENSI_HEADERS: DgSensitive =
    {
        "dg_status": "DG Status",
        "table_nm": "Table name",
        "column_nm": "Column name",
        'schema_nm': 'Schema name',
        'db_nm': 'Database name',
        "data_cls_nm": "Data Class",
        "prof_start_ts": "Prof start",
        "prof_end_ts": "Prod end",
        "tot_row_cnt": "Total cnt",
        "sample_row_cnt": "Sample cnt",
        "col_val_uniq_cnt": "Unique cnt",
        "col_val_data_cls_cnt": "Data Class cnt",
        "col_max_val": "Max",
        "col_min_val": "Min",
        "col_avg_len": "Avg",
        "appl_regex_str": "Applied regexp",
        "col_id": "Column ID",
        "table_id": "Table ID",
        "batch_id": "Batch ID",
        "crt_by": "Created by",
        "crt_ts": "Created TS",
        "dg_upd_by": "DG update by",
        "dg_upd_ts": "DG update TS"
    };
*/
    export const DG_SENSI_EMPTY: DgSensitive =
    {
        'dg_status': '',
        'table_nm': '',
        'column_nm': '',
        'schema_nm': '',
        'db_nm': '',
        'data_cls_nm': '',
        'prof_start_ts': '',
        'prof_end_ts': '',
        'tot_row_cnt': null,
        'sample_row_cnt': null,
        'col_val_uniq_cnt': null,
        'col_val_data_cls_cnt': null,
        'col_val_data_cls_percent': null,
        'col_max_val': '',
        'col_min_val': '',
        'col_avg_len': null,
        'appl_regex_str': '',
        'col_id': '',
        'table_id': '',
        'batch_id': null,
        'crt_by': '',
        'crt_ts': '',
        'dg_upd_by': '',
        'dg_upd_ts': '',
        'row_cnt': null
    };

    export const DG_SENSI_DATA_EMPTY: DgSensitive[] = [];
