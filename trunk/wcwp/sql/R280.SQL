                                                                                                                                           
R280	实体渠道资源配置信息日月关系	
sel val1,val2                                                                                                    
from (                                                                                                                                               
		sel count(distinct cor_chnl_id) val1 from $DWPVIEW.TB_SVC_CORCHNL_RES_CONF                                                                       
		where cmcc_prov_prvd_id=$Branch_ID                                                                                                               
		and eff_dt<=ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1                                                                       
		and end_dt>ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1		                                                                     
) t1                                                                                                                                                 
inner join (                                                                                                                                         
 		sel count(distinct cor_chnl_id) val2 from $DWPVIEW.TB_SVC_CORCHNL_RES_CONF_DAY                                                                   
		where cmcc_prov_prvd_id=$Branch_ID                                                                                                               
		and eff_dt<=ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1                                                                       
		and end_dt>ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1		                                                                     
 ) t2                                                                                                                                                
 on 1=1  
   
          
		  



CREATE TABLE BASS1   .G_A_06037_DAY_SNAPDT0531  (                  
                  CHANNEL_ID CHAR(40) 
				  )   
                 DISTRIBUTE BY HASH(CHANNEL_ID)   
                   IN TBS_APP_BASS1 INDEX IN TBS_INDEX
				   



db2 "load client from /bassapp/bihome/panzw/tmp/i_30000_201205_91002_001.dat of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L ( \
 1	8 \
,9	18 \
,19	34
) \
 messages ./bass2.DIM_TACNUM_DEVID_LOAD.msg \
 replace into bass2.DIM_TACNUM_DEVID_LOAD nonrecoverable"
 



select (
select count(0)
from 
(select * from G_I_06023_MONTH a  where time_id = 201205 ) a
,(select * from G_I_06021_MONTH a where time_id = 201205 and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 

) - (
select count(0) from 
(
                select *
                from
                (
                select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
                from G_A_06037_DAY a
                where time_id / 100 <= 201205
                ) t where t.rn =1  
) a ,(select * from G_I_06021_MONTH a where time_id = 201205 and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 
) from bass2.dual with ur



2012-06-27 10:52:19
2012-06-27 10:52:22
elapsedSec: 3 
928
928.000



select (
select count(0)
from 
(select * from G_I_06023_MONTH a  where time_id = 201205 ) a
,(select * from G_I_06021_MONTH a where time_id = 201205 and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 

) - (
select count(0) from 
(
                select *
                from
                (
                select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
                from G_A_06037_DAY a
                where time_id / 100 <= 201205
                ) t where t.rn =1  
) a ,(select * from G_I_06021_MONTH a where time_id = 201205 and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 
) from bass2.dual with ur



2012-06-27 10:52:26
2012-06-27 10:52:27
elapsedSec: 1 
928



select (
select count(0)
from 
(select * from G_I_06023_MONTH a  where time_id = 201205 ) a
,(select * from G_I_06021_MONTH a where time_id = 201205 and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 

) - (
select count(0) from 
(
                select *
                from
                (
                select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
                from G_A_06037_DAY a
                where time_id / 100 <= 201205
                ) t where t.rn =1  
) a ,(select * from G_I_06021_MONTH a where time_id = 201205 and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 
) from bass2.dual with ur







select val1,val2 
from 
(
select count(DISTINCT A.CHANNEL_ID) val1
from 
(select * from G_I_06023_MONTH a  where time_id = 201205 ) a
,(select * from G_I_06021_MONTH a where time_id = 201205 and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 

) a ,(
select  count(DISTINCT A.CHANNEL_ID) val2
from 
(
                select *
                from
                (
                select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
                from G_A_06037_DAY a
                where time_id / 100 <= 201205
                ) t where t.rn =1  
) a ,(select * from G_I_06021_MONTH a where time_id = 201205 and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 
)  b where 1 = 1


VAL1	VAL2
3967	3039

3967.000000	3040.000000






select val1,val2 
from 
(
select count(DISTINCT A.CHANNEL_ID) val1
from 
(select * from G_I_06023_MONTH a  where time_id = 201205 ) a
,(select * from G_I_06021_MONTH a where time_id = 201205 and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 

) a ,(
select  count(DISTINCT A.CHANNEL_ID) val2
from 
(
                select *
                from
                (
                select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
                from G_A_06037_DAY a
                where time_id / 100 <= 201205
                ) t where t.rn =1  
) a ,
TABLE (
select CHANNEL_ID
from
(
 select 
 a.*
 ,row_number()over(partition by CHANNEL_ID order by TIME_ID desc )  rn 
 from G_A_06035_DAY  a
 where time_id/100 <= 201205
) o where o.rn = 1
and CHNL_STATE = '1'
) B
where a.CHANNEL_ID = b.CHANNEL_ID 
)  b where 1 = 1




db2 "load client from /bassapp/bihome/panzw/tmp/13100_06037_20120531.txt of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L ( 4	43 ) \
 messages ./bass1.G_A_06037_DAY_SNAPDT0531.msg \
 replace into bass1.G_A_06037_DAY_SNAPDT0531 nonrecoverable"
 




select CHANNEL_ID from bass1.G_A_06037_DAY_SNAPDT0531
except
select  channel_id
from 
(
                select *
                from
                (
                select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
                from G_A_06037_DAY a
                where time_id / 100 <= 201205
                ) t where t.rn =1  
) a


CHANNEL_ID
100003024                               


select * from bass1.G_A_06037_DAY where CHANNEL_ID = '100003024'





db2 "load client from /bassapp/bihome/panzw/tmp/i_30000_201205_91002_001.dat of asc \
 modified by timestampformat=\"YYYYMMDDHHMMSS\" dateformat=\"YYYYMMDD\" \
 timeformat=\"HHMMSS\" \
 method L ( \
 1	8 \
,9	18 \
,19	34
) \
 messages ./bass2.DIM_TACNUM_DEVID_LOAD.msg \
 replace into bass2.DIM_TACNUM_DEVID_LOAD nonrecoverable"
 
 