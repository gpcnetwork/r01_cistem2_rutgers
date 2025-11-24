select count(distinct patid) from KTX_TBL1;
-- 14,657

select count(distinct patid) from KTX_TBL1
where AR_IND = 1;
-- 635

select count(distinct patid) from KTX_TBL1
where MI_IND = 1;
-- 390

select count(distinct patid) from KTX_TBL1
where NODAT_IND = 1;
-- 1957

select count(distinct site) from KTX_DXPX_LONG
where phe_type = 'KTx'
;
--13

select min(DAYS_TO_CENSOR), max(DAYS_TO_CENSOR), max(censor_date)
from KTX_TBL1
; 

select * from KTX_TBL1 
order by days_to_censor desc
limit 10
;