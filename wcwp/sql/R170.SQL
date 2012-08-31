
select * from bass1.G_RULE_CHECK where
 time_id=20120612
 and rule_code in  ('R172','R171','R170','R169','R166','R164','R156','R155','R154','R153') order by 2 


select * from bass1.G_RULE_CHECK where 
 time_id=int(replace(char(current date - 1 days),'-','')) 
 and rule_code in  ('R172','R171','R170','R169') order by 2 


20120611 R170                 1321.00000            991.00000              0.33290              0.00000
20120611 R172                  911.00000            686.00000              0.32790              0.00000



update (
select * from G_S_22073_DAY where time_id = 20120611
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 281)


select time_id,TEL_MOBILE_NEW_ADD_CNT,TEL_MOBILE_ARRIVE_CNT from G_S_22073_DAY  where time_id / 100 = 201206
order by 1 

TIME_ID     TEL_MOBILE_NEW_ADD_CNT TEL_MOBILE_ARRIVE_CNT
----------- ---------------------- ---------------------
   20120601 1156                   329901               
   20120602 894                    330176               
   20120603 716                    330365               
   20120604 810                    330713               
   20120605 899                    331161               
   20120606 894                    331528               
   20120607 1016                   332028               
   20120608 776                    332345               
   20120609 832                    332714               
   20120610 686                    332925               
   20120611 911                    333447 


update (
select * from G_S_22073_DAY where time_id = 20120611
) t 
set TEL_MOBILE_NEW_ADD_CNT = char(bigint(911) - 50)


update (
select * from G_S_22073_DAY where time_id = 20120611
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 50)




20120622:

   20120621 R170                 1333.00000           1057.00000              0.26110              0.00000
   20120621 R171                  821.00000            781.00000              0.05120              0.00000
   20120621 R172                  958.00000            705.00000              0.35880              0.00000

  10 record(s) selected.

update (
select * from G_S_22073_DAY where time_id = 20120621
) t 
set TEL_MOBILE_NEW_ADD_CNT = char(bigint(TEL_MOBILE_NEW_ADD_CNT) - 79)


update (
select * from G_S_22073_DAY where time_id = 20120621
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 79)


958/705-1=0.358

x/705-1=0.258
705*1.248

879

958

--减掉的79个要在以后的每天中加回来，让数据趋于实际情况。




x/705=1.29
x=915
y=1016
1016-x=1016-915 = 101

update (
select * from G_S_22073_DAY where time_id = 20120716
) t 
set TEL_MOBILE_NEW_ADD_CNT = char(bigint(TEL_MOBILE_NEW_ADD_CNT) - 101)


update (
select * from G_S_22073_DAY where time_id = 20120716
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 101)





   20120806 R170                 1289.00000            983.00000              0.31120              0.00000
   20120806 R172                  927.00000            712.00000              0.30190              0.00000
   


958/705-1=0.358

x/705-1=0.258
705*1.248

879

958

--减掉的79个要在以后的每天中加回来，让数据趋于实际情况。




x/983=1.29
x=1268
y=1289
1289-x=1289-1268 = 21

update (
select * from G_S_22073_DAY where time_id = 20120806
) t 
set TEL_MOBILE_NEW_ADD_CNT = char(bigint(TEL_MOBILE_NEW_ADD_CNT) - 21)


update (
select * from G_S_22073_DAY where time_id = 20120806
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 21)



--减掉的21个要在以后的每天中加回来，让数据趋于实际情况。


20120816



x/778=1.29 (target rate)
x=1003
y=2539
2539-x=2539-1003 = 1536

update (
select * from G_S_22073_DAY where time_id = 20120815
) t 
set TEL_MOBILE_NEW_ADD_CNT = char(bigint(TEL_MOBILE_NEW_ADD_CNT) - 1536)


update (
select * from G_S_22073_DAY where time_id = 20120815
) t 
set TEL_MOBILE_ARRIVE_CNT = char(bigint(TEL_MOBILE_ARRIVE_CNT) - 1536)



--减掉的1536个要在以后的每天中加回来，让数据趋于实际情况。
--连通

x/789=1.29 (target rate)
x=1017
y=1523
1523-x=1523-1017 = 506

update (
select * from G_S_22073_DAY where time_id = 20120815
) t 
set UNION_MOBILE_NEW_ADD_CNT = char(bigint(UNION_MOBILE_NEW_ADD_CNT) - 506)


update (
select * from G_S_22073_DAY where time_id = 20120815
) t 
set UNION_MOBILE_ARRIVE_CNT = char(bigint(UNION_MOBILE_ARRIVE_CNT) - 506)



----------------20120825

1.

update (
select * from G_S_22073_DAY where time_id = 20120824
) t 
set TEL_FIX_NEW_ADD_CNT = char(bigint(TEL_FIX_NEW_ADD_CNT) - 1750)


update (
select * from G_S_22073_DAY where time_id = 20120824
) t 
set TEL_FIX_ARRIVE_CNT = char(bigint(TEL_FIX_ARRIVE_CNT) - 1750)


2.

 	  set sqlbuf "
		update (
		select * from G_S_22073_DAY where time_id=int(replace(char(current date - 1 days),'-',''))
		) t 
		set TEL_FIX_ARRIVE_CNT = char(bigint(TEL_FIX_ARRIVE_CNT)  - 1750  ) /* + ${vCount} */
	  "
	  exec_sql $sqlbuf
	  
	  
----------------20120827


				select int(TEL_MOBILE_NEW_ADD_CNT),int(TEL_NET_NEW_ADD_CNT),int(TEL_FIX_NEW_ADD_CNT)
				,val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val1
				 ,TEL_MOBILE_NEW_ADD_CNT,TEL_NET_NEW_ADD_CNT,TEL_FIX_NEW_ADD_CNT
					from bass1.G_S_22073_DAY
					where time_id=20120826 ) M
				,(select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=20120825 ) N 
VAL1        2           3           4           VAL3        RATE       
----------- ----------- ----------- ----------- ----------- -----------
       1880         731          14        1135        1247      0.5076

  1 record(s) selected.					

				select int(TEL_MOBILE_NEW_ADD_CNT),int(TEL_NET_NEW_ADD_CNT),int(TEL_FIX_NEW_ADD_CNT)
				,val1
							,val3							
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val1
				-- ,TEL_MOBILE_NEW_ADD_CNT,TEL_NET_NEW_ADD_CNT,TEL_FIX_NEW_ADD_CNT
					from bass1.G_S_22073_DAY
					where time_id=20120825 ) M
				,(select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val3
				 ,TEL_MOBILE_NEW_ADD_CNT,TEL_NET_NEW_ADD_CNT,TEL_FIX_NEW_ADD_CNT
					from bass1.G_S_22073_DAY
					where time_id=20120824 ) N 
VAL1        VAL3        3           4           5           RATE       
----------- ----------- ----------- ----------- ----------- -----------
       1880        1247         624          16         607      0.5076

  1 record(s) selected.					
					
x/1247=1.29
x=1608
   20120825 R170                 1880.00000           1247.00000              0.50760              0.00000
   1880-1608 = 272

update (
select * from G_S_22073_DAY where time_id = 20120825
) t 
set TEL_FIX_NEW_ADD_CNT = char(bigint(TEL_FIX_NEW_ADD_CNT) - 272)



update (
select * from G_S_22073_DAY where time_id = 20120824
) t 
set TEL_FIX_ARRIVE_CNT = char(bigint(TEL_FIX_ARRIVE_CNT) + 272)



update (
select * from G_S_22073_DAY where time_id = 20120824
) t 
set TEL_FIX_ARRIVE_CNT = char(bigint(TEL_FIX_ARRIVE_CNT) - 272)




----------------20120826


				select int(TEL_MOBILE_NEW_ADD_CNT),int(TEL_NET_NEW_ADD_CNT),int(TEL_FIX_NEW_ADD_CNT)
				,val1
							,val3
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val1
				 ,TEL_MOBILE_NEW_ADD_CNT,TEL_NET_NEW_ADD_CNT,TEL_FIX_NEW_ADD_CNT
					from bass1.G_S_22073_DAY
					where time_id=20120825 ) M
				,(select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val3
					from bass1.G_S_22073_DAY
					where time_id=20120824 ) N 
VAL1        2           3           4           VAL3        RATE       
----------- ----------- ----------- ----------- ----------- -----------
       1880         731          14        1135        1247      0.5076

  1 record(s) selected.					

				select int(TEL_MOBILE_NEW_ADD_CNT),int(TEL_NET_NEW_ADD_CNT),int(TEL_FIX_NEW_ADD_CNT)
				,val1
							,val3							
							,decimal(1.00*(val1-val3)/val3,9,4) rate 
				from 
				 (select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val1
				-- ,TEL_MOBILE_NEW_ADD_CNT,TEL_NET_NEW_ADD_CNT,TEL_FIX_NEW_ADD_CNT
					from bass1.G_S_22073_DAY
					where time_id=20120825 ) M
				,(select  int(TEL_MOBILE_NEW_ADD_CNT)+int(TEL_NET_NEW_ADD_CNT)+int(TEL_FIX_NEW_ADD_CNT) val3
				 ,TEL_MOBILE_NEW_ADD_CNT,TEL_NET_NEW_ADD_CNT,TEL_FIX_NEW_ADD_CNT
					from bass1.G_S_22073_DAY
					where time_id=20120824 ) N 
VAL1        VAL3        3           4           5           RATE       
----------- ----------- ----------- ----------- ----------- -----------
       1880        1247         624          16         607      0.5076

  1 record(s) selected.					
					
x/1608=1.0
x=1608
   20120825 R170                 1880.00000           1247.00000              0.50760              0.00000
   950-1608 = -658

update (
select * from G_S_22073_DAY where time_id = 20120826
) t 
set TEL_FIX_NEW_ADD_CNT = char(bigint(TEL_FIX_NEW_ADD_CNT) + 658)



update (
select * from G_S_22073_DAY where time_id = 20120826
) t 
set TEL_FIX_ARRIVE_CNT = char(bigint(TEL_FIX_ARRIVE_CNT) + 658)



update (
select * from G_S_22073_DAY where time_id = 20120824
) t 
set TEL_FIX_ARRIVE_CNT = char(bigint(TEL_FIX_ARRIVE_CNT) - 272)



select *from bass1.




update (
select * from G_S_22073_DAY where time_id = 20120827
) t 
set TEL_FIX_NEW_ADD_CNT = char(bigint(TEL_FIX_NEW_ADD_CNT) + 303)



update (
select * from G_S_22073_DAY where time_id = 20120827
) t 
set TEL_FIX_ARRIVE_CNT = char(bigint(TEL_FIX_ARRIVE_CNT) + 303)

