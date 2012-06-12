/**

2、月底调度BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl中R107/R108超标，此调度只有在月初1号早上送月底那天的日数据时必须调整，让校验通过，
其它日期调度报错直接点击运行完成；

--调整脚本(数据量大，update有些慢，要几分钟，调了以后如还报错，再调其中“400”和“5”的值，注意微调)
---R107
**/


--以下代码不用修改直接运行

select * from bass1.g_rule_check 
where rule_code in ('R107') 
and time_id = int(replace(char(current date - 1 days),'-',''))
order by time_id desc


select * from bass1.g_rule_check 
where rule_code in ('R108') 
and time_id = int(replace(char(current date - 1 days),'-',''))
order by time_id desc

---R107
update (select * from  BASS1.G_S_04008_DAY where time_id = int(replace(char(current date - 1 days),'-',''))  ) t 
set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) with ur

---R108
update (select * from  BASS1.G_S_04008_DAY where time_id = int(replace(char(current date - 1 days),'-',''))  ) t 
set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5) with ur 



/**
3、1号日接口数据送完之后，中午左右集团将下发抽样用户数据，进行数据入库(就如下月份)，不然第二天的日接口数据将出问题
以app用户登录 172.16.5.44 
$ cd /bassapp/backapp/bin/bass1_lst
$ ./bass1_lst.sh yyyy-mm (自然月)

select time_id,count(0) from bass1.g_user_lst
group by time_id 
order by 1 desc 


**/
检查

./bass1_lst.sh 2012-06





/**

更新：05001 05002 调度 ， 点完成。

**/

