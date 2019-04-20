export class SuspectCriteria {

    data_cls_nm: string;
    match_type_opr: string;
    match_nm: string;
    upd_by: string;
    upd_ts: string;
    upd_rsn_txt: string;
    id: number;

    constructor() {

        this.data_cls_nm = '';
        this.match_type_opr = '';
        this.match_nm =  '';
        this.upd_by = '';
        this.upd_ts = '';
        this.upd_rsn_txt = '';
        this.id = 0;
    }

}

export const SUSPECT_CRITERIA_HEADERS: SuspectCriteria =
{
        'data_cls_nm': 'Data Classification name',
        'match_type_opr': 'SQL match operator',
        'match_nm': 'Column Name match',
        'upd_by': 'Updated By',
        'upd_ts': 'Update TS',
        'upd_rsn_txt': 'Update reason',
        'id': 0,
};

export const SUSPECT_CRITERIA_EMPTY: SuspectCriteria =
{
    'data_cls_nm': '',
    'match_type_opr': '',
    'match_nm': '',
    'upd_by': '',
    'upd_ts': '',
    'upd_rsn_txt': '',
    'id': 0,
};

export const SUSPECT_CRITERIA_DATA_EMPTY: SuspectCriteria[] = [];

