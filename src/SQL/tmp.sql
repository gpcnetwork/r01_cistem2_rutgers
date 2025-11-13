select count(distinct patid) from KTX_TBL1;
-- 15,585

select count(distinct patid) from KTX_TBL1
where AR_IND = 1;
-- 635

select count(distinct patid) from KTX_TBL1
where MI_IND = 1;
-- 417

select count(distinct patid) from KTX_TBL1
where NODAT_IND = 1;
-- 2063

select count(distinct site) from KTX_DXPX_LONG
where phe_type = 'KTx'
;