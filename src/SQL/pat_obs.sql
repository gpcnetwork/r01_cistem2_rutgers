select * from KTX_TBL1 limit 5;

create or replace procedure get_obs_long(
    REF_COHORT string,
    TGT_LONG_TBL string,
    SITES array,
    DRY_RUN boolean,
    DRY_RUN_AT string
)
returns variant
language javascript
as
$$
/**
 * @param {string} REF_COHORT: name of reference patient table, should at least include (patid, birth_date, index_date)
 * @param {string} TGT_LONG_TBL: name of the targeted long table for event logging
 * @param {array} SITES: an array of site acronyms (matching schema name suffix) - include CMS
 * @param {boolean} DRY_RUN: dry run indicator. If true, only sql script will be created and stored in dev.sp_out table
 * @param {boolean} DRY_RUN_AT: A temporary location to store the generated sql query for debugging purpose. 
                                When DRY_RUN = True, provide absolute path to the table; when DRY_RUN = False, provide NULL 
**/
if (DRY_RUN) {
    var log_stmt = snowflake.createStatement({
        sqlText: `CREATE OR REPLACE TEMPORARY TABLE `+ DRY_RUN_AT +`(QRY VARCHAR);`});
    log_stmt.execute(); 
}

var i;
for(i=0; i<SITES.length; i++){
       var site = SITES[i].toString();
       var site_cdm = (site === 'CMS') ? 'CMS_PCORNET_CDM' : 'PCORNET_CDM_' + site;
       var raw_rx_name = (site === 'CMS') ? 'COALESCE(d.RAW_RX_MED_NAME,g.STR)' : 'g.STR';

       // collect all available labs
       sqlstmt1 = `
              INSERT INTO `+ TGT_LONG_TBL +`(patid,obs_date,days_since_index,obs_code_type,obs_num,obs_unit,obs_qual,obs_name,obs_type,obs_src)
              select  distinct
                      a.PATID
                     ,coalesce(b.specimen_date, b.lab_order_date, b.result_date) as OBS_DATE
                     ,datediff(day,a.index_date,coalesce(b.specimen_date, b.lab_order_date, b.result_date)) as DAYS_SINCE_INDEX
                     ,'LC' as OBS_CODE_TYPE
                     ,b.lab_loinc as OBS_CODE
                     ,coalesce(c.component,b.raw_lab_name) as OBS_NAME
                     ,b.result_num as OBS_NUM
                     ,b.result_unit as OBS_UNIT
                     ,b.norm_range_low as OBS_REF_LOW
                     ,b.norm_range_high as OBS_REF_HIGH
                     ,b.result_qual as OBS_QUAL
                     ,'lab' as OBS_TYPE
                     ,'`+ site +`' as OBS_SRC
              from `+ REF_COHORT +` a
              join GROUSE_DEID_DB.`+ site_cdm +`.V_DEID_LAB_RESULT_CM b
                     on a.patid = b.patid
              left join ONTOLOGY.LOINC.LOINC_V2_17 c
                     on b.lab_loinc = c.loinc_num
              ;
       `;
       var run_sqlstmt1 = snowflake.createStatement({sqlText: sqlstmt1});

       // collect all available vitals
       sqlstmt2 = `
              INSERT INTO `+ TGT_LONG_TBL +`(patid,obs_date,days_since_index,obs_code_type,obs_num,obs_unit,obs_qual,obs_name,obs_type,obs_src)
              with cte_unpvt_num as (
                     select patid, measure_date, measure_time, 
                            OBS_NAME, OBS_NUM,'NI' as OBS_QUAL,
                            case when OBS_NAME in ('SYSTOLIC','DIASTOLIC') then 'mm[Hg]'
                                 when OBS_NAME = 'HT' then 'in_us'
                                 when OBS_NAME = 'WT' then 'lb_av'
                                 when OBS_NAME = 'ORIGINAl_BMI' then 'kg/m2'
                                 else null
                            end as OBS_UNIT
                     from (
                            select patid, measure_date, measure_time,
                                   round(systolic) as systolic, 
                                   round(diastolic) as diastolic, 
                                   round(ht) as ht, 
                                   round(wt) as wt, 
                                   round(original_bmi) as original_bmi
                            from GROUSE_DEID_DB.`+ site_cdm +`.V_DEID_VITAL
                     )
                     unpivot (
                            OBS_NUM
                            for OBS_NAME in (
                                   systolic, diastolic, ht, wt, original_bmi
                            )
                     )
                     where OBS_NUM is not null and trim(OBS_NUM) <> ''
              ), cte_unpvt_qual as (
                     select patid, measure_date, measure_time, 
                            OBS_NAME, NULL as OBS_NUM, OBS_QUAL, NULL as OBS_UNIT
                     from (
                            select patid, measure_date, measure_time,
                            smoking, tobacco, tobacco_type
                            from GROUSE_DEID_DB.`+ site_cdm +`.V_DEID_VITAL
                     ) 
                     unpivot (
                            OBS_QUAL
                            for OBS_NAME in (
                                   smoking, tobacco, tobacco_type
                            )
                     )
                     where OBS_QUAL is not null and trim(OBS_QUAL) <> '' 
                       and OBS_QUAL not in ('UN','NI','OT')
              )
              select  distinct
                      a.PATID
                     ,b.measure_date as OBS_DATE
                     ,datediff(day,a.index_date,b.measure_date) as DAYS_SINCE_INDEX
                     ,'UD' as OBS_CODE_TYPE 
                     ,b.OBS_NUM
                     ,b.OBS_UNIT
                     ,b.OBS_QUAL
                     ,b.OBS_NAME
                     ,'vital' as OBS_TYPE
                     ,'`+ site +`' as OBS_SRC
              from `+ REF_COHORT +` a
              join (
                     select * from cte_unpvt_num
                     union 
                     select * from cte_unpvt_qual
              ) b
                     on a.patid = b.patid
              ;
       `;
       var run_sqlstmt2 = snowflake.createStatement({sqlText: sqlstmt2});

       // collect all available obs_clin
       sqlstmt3 = `
              INSERT INTO `+ TGT_LONG_TBL +`(patid,obs_date,days_since_index,obs_code_type,obs_code,obs_name,obs_num,obs_unit,obs_qual,obs_type,obs_src)
              select  distinct
                      a.PATID
                     ,coalesce(b.obsclin_start_date, b.obsclin_stop_date) as OBS_DATE
                     ,datediff(day,a.index_date,coalesce(b.obsclin_start_date, b.obsclin_stop_date)) as DAYS_SINCE_INDEX
                     ,b.obsclin_type as OBS_CODE_TYPE
                     ,b.obsclin_code as OBS_CODE
                     ,coalesce(c.component,b.raw_obsclin_name) as OBS_NAME
                     ,b.obsclin_result_num as OBS_NUM
                     ,b.obsclin_result_unit as OBS_UNIT
                     ,coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) as OBS_QUAL
                     ,'obsclin' as OBS_TYPE
                     ,'`+ site +`' as OBS_SRC
              from `+ REF_COHORT +` a
              join GROUSE_DEID_DB.`+ site_cdm +`.V_DEID_OBS_CLIN b
                     on a.patid = b.patid
              left join ONTOLOGY.LOINC.LOINC_V2_17 c
                     on b.obsclin_code = c.loinc_num and b.obsclin_type = 'LC'
              where b.obsclin_result_num is not null
                 or (
                     coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) is not null 
                     and coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) <> '' 
                     and coalesce(trim(b.obsclin_result_qual),trim(b.obsclin_result_text)) not in ('UN','NI','OT')
                    )
              ;

       `;

       // collect all available from obs_gen

       var run_sqlstmt3 = snowflake.createStatement({sqlText: sqlstmt3});

       if (DRY_RUN) {
              // preview of the generated dynamic SQL scripts - comment it out when perform actual execution
              var log_stmt = snowflake.createStatement({
                            sqlText: `INSERT INTO `+ DRY_RUN_AT +` (qry) values (:1),(:2),(:3);`,
                            binds: [sqlstmt1, sqlstmt2,sqlstmt3]});
        log_stmt.execute(); 
       } else {
              // run dynamic dml query
              var commit_txn = snowflake.createStatement({sqlText: `commit;`}); 
              try{run_sqlstmt1.execute();} catch(error) {};
              try{run_sqlstmt2.execute();} catch(error) {};
              try{run_sqlstmt3.execute();} catch(error) {};
              commit_txn.execute();
       }
}
$$
;

create or replace table ALL_OBS (
        PATID varchar(50) NOT NULL
       ,OBS_DATE date
       ,DAYS_SINCE_INDEX number
       ,OBS_CODE_TYPE varchar(10)
       ,OBS_CODE varchar(100)
       ,OBS_NAME varchar(500)
       ,OBS_NUM number 
       ,OBS_UNIT varchar(50)
       ,OBS_REF_LOW varchar(100)
       ,OBS_REF_HIGH varchar(100) 
       ,OBS_QUAL varchar(100)
       ,OBS_TYPE varchar(105)
       ,OBS_SRC varchar(10)
);


/* test */
-- call get_obs_long(
--        'KTX_TBL1',
--        'ALL_OBS',
--        array_construct(
--               'MU'
--              ,'WASHU'
--        ),
--        True, 'TMP_SP_OUTPUT'
-- );
-- select * from TMP_SP_OUTPUT;


call get_obs_long(
       'KTX_TBL1',
       'ALL_OBS',
       array_construct(
         'ALLINA'
        ,'IHC'
        ,'KUMC'
        ,'MCW'
        ,'MU'
        ,'UIOWA'
        ,'UNMC'
        ,'UTHOUSTON'
        ,'UTSW'
        ,'UU'
        ,'WASHU'
    ), 
    FALSE, NULL
);

select * from ALL_OBS limit 5;
select count(distinct patid), count(*) 
from ALL_OBS
;

create or replace table SEL_OBS_BMI as
with cte_wt as (
       select * from ALL_OBS
       where obs_name in ('WT') 
         and obs_num > 60 and obs_num < 1400
),   cte_ht as (
       select * from ALL_OBS
       where obs_name in ('HT')
         and obs_num > 40 and obs_num < 100
),   cte_bmi as (
       select * from ALL_OBS
       where obs_name in ('ORIGINAL_BMI') 
         and obs_num between 10 and 200
),  cte_all_dt as (
       select distinct patid, obs_date, days_since_index,obs_src from cte_wt
       union 
       select distinct patid, obs_date, days_since_index,obs_src from cte_bmi
),  cte_imputed as (
    select a.patid,
        a.obs_date,
        a.days_since_index,
        wt.obs_num as wt,
        coalesce(wt.obs_num,lag(wt.obs_num) ignore nulls over (partition by a.patid,a.obs_src order by a.obs_date)) as wt_imputed_lag,
        coalesce(wt.obs_num,lead(wt.obs_num) ignore nulls over (partition by a.patid,a.obs_src order by a.obs_date)) as wt_imputed_lead,
        ht.obs_num as ht,
        coalesce(ht.obs_num,lag(ht.obs_num) ignore nulls over (partition by a.patid,a.obs_src order by a.obs_date)) as ht_imputed_lag,
        coalesce(ht.obs_num,lead(ht.obs_num) ignore nulls over (partition by a.patid,a.obs_src order by a.obs_date)) as ht_imputed_lead,
        bmi.obs_num as bmi,
        coalesce(bmi.obs_num,lag(bmi.obs_num) ignore nulls over (partition by a.patid,a.obs_src order by a.obs_date)) as bmi_imputed_lag,
        coalesce(bmi.obs_num,lead(bmi.obs_num) ignore nulls over (partition by a.patid,a.obs_src order by a.obs_date)) as bmi_imputed_lead,
        a.obs_src,
        row_number() over (partition by a.patid, a.obs_date order by a.obs_date) as dedup_idx
    from cte_all_dt a
    left join cte_wt wt on wt.patid = a.patid and wt.obs_src = a.obs_src and wt.obs_date = a.obs_date
    left join cte_ht ht on ht.patid = a.patid and ht.obs_src = a.obs_src  and ht.obs_date = a.obs_date
    left join cte_bmi bmi on bmi.patid = a.patid and bmi.obs_src = a.obs_src  and bmi.obs_date = a.obs_date
)
select patid,
       obs_date,
       days_since_index,
       coalesce(ht,ht_imputed_lag,ht_imputed_lead) as ht,
       coalesce(wt,wt_imputed_lag,wt_imputed_lead) as wt,
       round(coalesce(bmi,bmi_imputed_lag,bmi_imputed_lead,coalesce(wt,wt_imputed_lag,wt_imputed_lead)/(coalesce(ht,ht_imputed_lag,ht_imputed_lead)*coalesce(ht,ht_imputed_lag,ht_imputed_lead))*703)) as bmi,
       row_number() over (partition by patid order by abs(days_since_index)) as rk_idx_asc
from cte_imputed
where dedup_idx = 1
;

select * from SEL_OBS_BMI limit 5;
select count(distinct patid), count(*)
from SEL_OBS_BMI
;
-- 11,780

create or replace table BMI_IDX as 
select distinct
       a.PATID, 
       round(b.ht,2) as ht,
       round(b.wt,2) as wt,
       round(b.bmi,2) as bmi,
       case when b.bmi <18.5 then 'underwt'
            when b.bmi >=18.5 and b.bmi <25 then 'normal'
            when b.bmi >=25 and b.bmi <30 then 'overwt'
            when b.bmi >=30 and b.bmi <35 then 'obeseI'
            when b.bmi >=35 and b.bmi <40 then 'obeseII'
            when b.bmi >=40 then 'obeseIII'
       else 'NI' end as bmi_cls,
       b.days_since_index
from KTX_TBL1 a 
left join SEL_OBS_BMI b
on a.patid = b.patid and b.rk_idx_asc = 1
;
select * from BMI_IDX limit 5;
select count(distinct patid), count(*)
from BMI_IDX
;
--14657