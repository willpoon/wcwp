1、22073竞争KPI涉及的校验R163、R164、R165、R166、R167、R168、R169、R170、R171、R172(调度为BASS1_INT_CHECK_COMP_KPI_DAY.tcl)和
21007短信涉及的校验C1(调度BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl)超标，数据正常生成的，不要进行任何数据调整，调度直接点击完成后运行后续；

2、R173/R174 校验告警请通知我，由我向局方确认是否正常，再上报。

3. R159_4 超标告警 （这个比较频繁）
22012日kpi接口涉及的一致性检查，如超标，必须进行调整(调度为BASS1_INT_CHECK_INDEX_SAME_DAY.tcl)，调整22012接口的指标，重跑报错调度，
一定不能点击完成(最频繁的是“离网用户数”这个指标，差一个用户就超标，暂没解决口径不一致情况，二经未改造用户资料表)；


如：
select 
 time_id,
 case when rule_code='R159_1' then '新增客户数'
      when rule_code='R159_2' then '客户到达数'
      when rule_code='R159_3' then '上网本客户数'
      when rule_code='R159_4' then '离网客户数'
 end,
 target1,
 target2,
 target3
from bass1.g_rule_check
where rule_code in ('R159_1','R159_2','R159_3','R159_4')
  and time_id=int(replace(char(current date - 1 days),'-',''));

--target1 是二经值
--target2 是一经值
--一经值永远是对的，不用调，要调的是二经值。调的话就是把二经值调成和一经值一样。
--调整方法：
update bass1.g_s_22012_day set m_off_users='和target2一样' 
where time_id=int(replace(char(current date - 1 days),'-',''))



--调整脚本，''里更新一定的值就是
--离网客户数
update bass1.g_s_22012_day set m_off_users='' 
where time_id=int(replace(char(current date - 1 days),'-',''));
commit;

--新增客户数
update bass1.g_s_22012_day set m_new_users='' 
where time_id=int(replace(char(current date - 1 days),'-',''));
commit;

--上网本客户数
update bass1.g_s_22201_day set mtl_td_3gbook_mark='' 
where time_id=int(replace(char(current date - 1 days),'-',''));
commit;




--其他异常请及时告诉我来处理

