
CONTROL_CODE                                       ALARMTIME                  FLAG        CONTENT                                                                                             
-------------------------------------------------- -------------------------- ----------- ----------------------------------------------------------------------------------------------------
BASS1_INT_CHECK_COMP_KPI_DAY.tcl                   2012-06-13-03.02.30.426268          -1 R156校验不通过                                                                                      
BASS1_INT_CHECK_COMP_KPI_DAY.tcl                   2012-06-13-03.02.26.727849          -1 R155校验不通过                                                                                      
BASS1_INT_CHECK_COMP_KPI_DAY.tcl                   2012-06-13-03.02.23.594564          -1 R154校验不通过                                                                                      
BASS1_INT_CHECK_COMP_KPI_DAY.tcl                   2012-06-13-03.02.19.913599          -1 R153校验不通过                                                                                      

  4 record(s) selected.


select * from bass1.G_RULE_CHECK where
 time_id=20120612
 and rule_code in  ('R172','R171','R170','R169','R166','R164','R156','R155','R154','R153') order by 2 


select * from bass1.G_RULE_CHECK where
 time_id=20120612
 and rule_code in  ('R172','R171','R170','R169','R166','R164','R156','R155','R154','R153') order by 2 


puts "R153	日	(当日联通新增客户数-当日联通离网客户数)与(当日联通客户到达数-昨日联通客户到达数)的偏差在1%以内"
 puts "R154	日	(当日联通移动新增客户数-当日联通移动离网客户数)与(当日联通移动客户到达数-昨日联通移动客户到达数)偏差在1％以内"
 puts "R155	日	(当日电信新增客户数-当日电信离网客户数)与（当日电信客户到达数-昨日电信客户到达数）的偏差在1%以内"
 
 puts "R156	日	(当日电信移动新增客户数-当日电信移动离网客户数)与(当日电信移动客户到达数-昨日电信移动客户到达数)偏差在1％以内"
 

select * from bass1.G_RULE_CHECK where
 time_id=20120612
 and rule_code in  ('R172','R171','R170','R169','R166','R164','R156','R155','R154','R153') order by 2 


 
TIME_ID     RULE_CODE  TARGET1              TARGET2              TARGET3              TARGET4             
----------- ---------- -------------------- -------------------- -------------------- --------------------
   20120612 R153                  397.00000            453.00000             -0.12360              0.00000
   20120612 R154                  397.00000            453.00000             -0.12360              0.00000
   20120612 R155                  556.00000            606.00000             -0.08250              0.00000
   20120612 R156                  516.00000            566.00000             -0.08830              0.00000
   20120612 R164               569261.00000         568655.00000              0.00100              0.00000
   20120612 R166               333963.00000         333397.00000              0.00160              0.00000
   20120612 R169                  795.00000            756.00000              0.05150              0.00000
   20120612 R170                 1288.00000           1271.00000              0.01330              0.00000
   20120612 R171                  793.00000            752.00000              0.05450              0.00000
   20120612 R172                  919.00000            861.00000              0.06730              0.00000

  10 record(s) selected.
  
  
TIME_ID     RULE_CODE  TARGET1              TARGET2              TARGET3              TARGET4             
----------- ---------- -------------------- -------------------- -------------------- --------------------
   20120612 R153                  397.00000            397.00000              0.00000              0.00000
   20120612 R154                  397.00000            397.00000              0.00000              0.00000
   20120612 R155                  556.00000            556.00000              0.00000              0.00000
   20120612 R156                  516.00000            516.00000              0.00000              0.00000
   20120612 R164               569211.00000         568655.00000              0.00090              0.00000
   20120612 R166               333913.00000         333397.00000              0.00150              0.00000
   20120612 R169                  795.00000            756.00000              0.05150              0.00000
   20120612 R170                 1288.00000           1271.00000              0.01330              0.00000
   20120612 R171                  793.00000            752.00000              0.05450              0.00000
   20120612 R172                  919.00000            861.00000              0.06730              0.00000

  10 record(s) selected.


update (
select * from G_S_22073_DAY where time_id = 20120612
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) -50 - 50)


update (
select * from G_S_22073_DAY where time_id = 20120612
) t 
set UNION_MOBILE_ARRIVE_CNT = char(bigint(UNION_MOBILE_ARRIVE_CNT) - 56 - 56)





select TEL_MOBILE_NEW_ADD_CNT, val10,val11,val2,val3,val2-val3
						--,decimal(round(val1/1.00/(val2-val3)-1,4),9,4) rate 
			from
			 (select TEL_MOBILE_NEW_ADD_CNT, ( int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) ) val10,
							 ( int(TEL_MOBILE_LOST_CNT)+int(TEL_NET_LOST_CNT)+int(TEL_FIX_LOST_CNT) ) val11
							 ,int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val2
				from bass1.G_S_22073_DAY where time_id= 20120617 ) M
			,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) val3
				from bass1.G_S_22073_DAY
				where time_id= 20120616 
				) t where 1 = 1
				
				
				update G_S_22073_DAY
				set TEL_MOBILE_NEW_ADD_CNT = '625'
				where time_id= 20120617
				
				
				20120622：





select TEL_MOBILE_NEW_ADD_CNT, TEL_NEW_ADD_CNT,TEL_LOST_CNT,TEL_NEW_ADD_CNT-TEL_LOST_CNT,TEL_ARRIVE_CNT,TEL_ARRIVE_CNT_pre,TEL_ARRIVE_CNT-TEL_ARRIVE_CNT_pre
			from
			 (select TEL_MOBILE_NEW_ADD_CNT, ( int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) ) TEL_NEW_ADD_CNT,
							 ( int(TEL_MOBILE_LOST_CNT)+int(TEL_NET_LOST_CNT)+int(TEL_FIX_LOST_CNT) ) TEL_LOST_CNT
							 ,int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) TEL_ARRIVE_CNT
				from bass1.G_S_22073_DAY where time_id= 20120623 ) M
			,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) TEL_ARRIVE_CNT_pre
				from bass1.G_S_22073_DAY
				where time_id= 20120622
				) t where 1 = 1
				

TEL_MOBILE_NEW_ADD_CNT TEL_NEW_ADD_CNT TEL_LOST_CNT 4           TEL_ARRIVE_CNT TEL_ARRIVE_CNT_PRE 7          
---------------------- --------------- ------------ ----------- -------------- ------------------ -----------
788                               1080         1357        -277         571896             572093        -197

  1 record(s) selected.

TIME_ID     RULE_CODE  TARGET1              TARGET2              TARGET3              TARGET4             
----------- ---------- -------------------- -------------------- -------------------- --------------------
   20120622 R153                  134.00000            135.00000             -0.00740              0.00000
   20120622 R154                  131.00000            132.00000             -0.00760              0.00000
   20120622 R155                 -277.00000           -197.00000              0.40610              0.00000
   20120622 R156                 -104.00000            -24.00000              3.33330              0.00000
   20120622 R164               571896.00000         572093.00000             -0.00030              0.00000
   20120622 R166               336996.00000         337020.00000              0.00000              0.00000
   20120622 R169                  905.00000            830.00000              0.09030              0.00000
   20120622 R170                 1080.00000           1254.00000             -0.13870              0.00000
   20120622 R171                  900.00000            821.00000              0.09620              0.00000
   20120622 R172                  788.00000            879.00000             -0.10350              0.00000

  10 record(s) selected.
  
  
  
  select * from bass1.G_RULE_CHECK where
 time_id=20120623
 and rule_code in  ('R172','R171','R170','R169','R166','R164','R156','R155','R154','R153') order by 2 





select TEL_MOBILE_NEW_ADD_CNT, TEL_NEW_ADD_CNT,TEL_LOST_CNT,TEL_NEW_ADD_CNT-TEL_LOST_CNT,TEL_ARRIVE_CNT,TEL_ARRIVE_CNT_pre,TEL_ARRIVE_CNT-TEL_ARRIVE_CNT_pre
			from
			 (select TEL_MOBILE_NEW_ADD_CNT, ( int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) ) TEL_NEW_ADD_CNT,
							 ( int(TEL_MOBILE_LOST_CNT)+int(TEL_NET_LOST_CNT)+int(TEL_FIX_LOST_CNT) ) TEL_LOST_CNT
							 ,int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) TEL_ARRIVE_CNT
				from bass1.G_S_22073_DAY where time_id= 20120628 ) M
			,(select int(TEL_MOBILE_ARRIVE_CNT)+int(TEL_NET_ARRIVE_CNT)+int(TEL_FIX_ARRIVE_CNT) TEL_ARRIVE_CNT_pre
				from bass1.G_S_22073_DAY
				where time_id= 20120627
				) t where 1 = 1
				