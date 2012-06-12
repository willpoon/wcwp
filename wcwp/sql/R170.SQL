
select * from bass1.G_RULE_CHECK where 
 time_id=int(replace(char(current date - 1 days),'-','')) 
 and rule_code in  ('R172','R171','R170','R169','R166','R164','R156','R155') order by 2 


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


