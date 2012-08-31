
select * from bass1.G_RULE_CHECK where 
 time_id=int(replace(char(current date - 1 days),'-','')) 
 and rule_code in  ('R172','R171','R170','R169','R166','R164','R156','R155') order by 2 


select * from bass1.G_RULE_CHECK where 
 time_id=int(replace(char(current date - 1 days),'-','')) 
 and rule_code in  ('R172','R171','R170','R169') order by 2 



BASS1_INT_CHECK_COMP_KPI_DAY.tcl

20120611 R170                 1321.00000            991.00000              0.33290              0.00000
20120611 R172                  911.00000            686.00000              0.32790              0.00000



select time_id,UNION_MOBILE_NEW_ADD_CNT,UNION_MOBILE_ARRIVE_CNT from G_S_22073_DAY  where time_id / 100 = 201206
order by 1 

TIME_ID     UNION_MOBILE_NEW_ADD_CNT UNION_MOBILE_ARRIVE_CNT
----------- ------------------------ -----------------------
   20120601 1240                     180517                 
   20120602 970                      180975                 
   20120603 852                      181538                 
   20120604 929                      182145                 
   20120605 796                      182568                 
   20120606 762                      183002                 
   20120607 801                      183458                 
   20120608 741                      183794                 
   20120609 710                      184075                 
   20120610 641                      184324                 
   20120611 807                      184759                 


update (
select * from G_S_22073_DAY where time_id = 20120611
) t 
set UNION_MOBILE_NEW_ADD_CNT = char(bigint(UNION_MOBILE_NEW_ADD_CNT) - 55)


update (
select * from G_S_22073_DAY where time_id = 20120611
) t 
set UNION_MOBILE_ARRIVE_CNT = char(bigint(UNION_MOBILE_ARRIVE_CNT) - 55)




update (
select * from G_S_22073_DAY where time_id = 20120716
) t 
set UNION_MOBILE_ARRIVE_CNT = char(bigint(UNION_MOBILE_ARRIVE_CNT) - 55)
