
select count(*) from 
                    (
                     select user_id,count(*) cnt from bass1.g_a_02008_day
                      where time_id =20120531
                     group by user_id
                     having count(*)>1
                    ) as a



89160002171973

89160000184970




select user_id,product_no,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
 from bass2.dw_product_20120531
 where user_id = '89160000184970'


select user_id,product_no,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
 from bass2.dw_product_20120516
 where user_id = '89160002171973'


select user_id,product_no,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
 from bass2.dw_product_20120517
 where product_no = '13648910608'


USER_ID
89157331686734
89160002171973



                     select * from bass1.g_a_02008_day
                      where time_id =20120517
				      and user_id in( '89160002171973','89157331686734')
					  


                     select * from bass1.g_a_02004_day
                      where time_id =20120517
				      and product_no = '13648910608'


select user_id,product_no,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
 from bass2.dw_product_20120517
 where user_id in ('89160000184970','89160002171973')
USER_ID
89160002171973
89160000184970


                     select * from bass1.g_a_02004_day
 where user_id in ('89160000184970','89160002171973')




delete from 
 (
	 select * from bass1.g_a_02008_day
	  where time_id =20120517
	  and user_id in( '89160002171973','89160000184970')
and USERTYPE_ID = '2020'

) t


TIME_ID     USER_ID              USERTYPE_ID
----------- -------------------- -----------
   20120517 89160002171973       2020       
   20120517 89160002171973       1031       
   20120517 89160000184970       1010  
   
   
update (
	 select * from bass1.g_a_02008_day
	  where time_id =20120517
	  and user_id in( '89160002171973','89160000184970')
) t set 			  USERTYPE_ID = '2020'
where USER_ID = '89160000184970'

	 select * from bass1.g_a_02008_day
	  where user_id in( '89160002171973','89160000184970')



select user_id,product_no,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
 from bass2.dw_product_20120517
 where user_id in (
select user_id
from G_A_02004_02008_NEWANDLEAVE where time_id=int(replace(char(current date - 1 days),'-',''))
and TYPE = 'LEA1'
)


  select product_no
   from  CHECK_0200402008_DAY_4
   group by product_no
   having count(*) >=2 
   with ur
   
   

select * from bass1.g_a_02008_day
	  where time_id =20120531
	  and user_id in( '89160000184970')



delete from 
 (
	 select * from bass1.g_a_02008_day
	  where time_id =20120518
	  and user_id in( '89160000184970')

) t



89160000482964
89157333262030

	 select * from bass1.g_a_02008_day
	  where time_id =20120518
	  and user_id in( '89160000482964','89157333262030')



delete from 
 (
	 select * from bass1.g_a_02008_day
	  where time_id =20120518
	  and user_id in( '89160000482964','89157333262030')

) t


db2 "
	 select count(0) from bass1.g_a_02008_day
	  where time_id =20120518
"




select user_id,product_no,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE
 from bass2.dw_product_20120531
 where product_no = 'd15289014474'
 
 
 
                      select * from bass1.g_a_02004_day
 where product_no = 'd15289014474'
 
 
 
 
 BASS1_G_A_02004_DAY.tcl
 
  sh bass1_adj_sql.sh -or BASS1_G_A_02008_DAY.tcl
  sh bass1_adj_sql.sh -or BASS1_G_A_02004_DAY.tcl  
  sh bass1_adj_sql.sh -or BASS1_INT_CHECK_02004_02008_DAY.tcl  
  
  BASS1_INT_CHECK_02004_02008_DAY.tcl
  
  
  ALL_AFTERS
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_02004_02008_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_GPRS_FLOW_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_INDEX_SAME_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R023R024_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R177182_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R178183_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R179184_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R180185_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R255_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R263_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_R284R285_DAY.tcl
sh bass1_adj_sql.sh -or BASS1_INT_CHECK_TD_DAY.tcl


BASS1_EXP_G_A_02004_DAY

BASS1_EXP_G_A_02008_DAY

  sh bass1_adj_sql.sh -or BASS1_EXP_G_A_02004_DAY  
  sh bass1_adj_sql.sh -or BASS1_EXP_G_A_02008_DAY
  
  