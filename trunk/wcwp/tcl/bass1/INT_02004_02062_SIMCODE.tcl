proc Deal_imp { op_time optime_month } {
##上线操作：
##  02004 tcl 中 Deal_imp 去注释 

   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    #当天 yyyy-mm-dd
    set optime $op_time
                set curr_month [string range $op_time 0 3][string range $op_time 5 6]


set sql_buff "
ALTER TABLE g_a_02004_02008_stage ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
"
exec_sql $sql_buff

set sql_buff "
insert into bass1.g_a_02004_02008_stage 
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,e.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id  
        ,CUST_ID
        ,USER_BUS_TYP_ID
        ,IMSI
        ,CMCC_ID
        ,CHANNEL_ID
        ,MCV_TYP_ID
        ,PROMPT_TYPE
        ,SUBS_STYLE_ID
from (select user_id,CUST_ID,USERTYPE_ID,create_date,USER_BUS_TYP_ID,product_no,IMSI
    ,CMCC_ID
    ,CHANNEL_ID
    ,MCV_TYP_ID
    ,PROMPT_TYPE
    ,SUBS_STYLE_ID
    ,brand_id
    ,sim_code
    ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=$timestamp ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=$timestamp ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1
"
exec_sql $sql_buff

  aidb_runstats bass1.g_a_02004_02008_stage 3


set sql_buff "
ALTER TABLE g_a_02004_02062_simcode ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
"
exec_sql $sql_buff

#core step:
# 变更的情况 ： 
#1. 在G_A_02062_DAY_STAGE 新增， 是 ‘2‘
#2.在02004 转换 为 ’2' ， 应该也在 在G_A_02062_DAY_STAGE ， 同 1 处理。
#3. 不在 G_A_02062_DAY_STAGE， 不再 是 '2'。按 02004 划分方法，重新区分 为 ‘0’ or '1'
#4. 离网 , 保持 最后一次状态 
#
# 获取 sim_code 增量数据 
# 在 G_A_02062_DAY_STAGE 的 一定 是 ‘2’
#  不在 G_A_02062_DAY_STAGE 的 ， ０?02004 划分方法，重新区分 为 ‘0’ or '1'

## 另外，既是 1 也是 2 的 ， 1 优先
set sql_buff "
insert into bass1.g_a_02004_02062_simcode
select  value(a.user_id , b.user_id) user_id
    ,a.sim_code old_sim_code
    ,case when a.sim_code = '1' then a.sim_code 
          when b.user_id is not null then '2' 
          when b.user_id is null then value(u.sim_code,'0')
    end new_sim_code
    from bass1.g_a_02004_02008_stage a 
     join (select user_id ,case when a.crm_brand_id2=70 then '1'  else '0' end sim_code 
                from  bass2.dw_product_bass1_$timestamp a ) u on  a.user_id = u.user_id     
    left join (select user_id , '2' sim_code from  G_A_02062_DAY_STAGE ) b on a.user_id = b.user_id 
where a.SIM_CODE <> (case when a.sim_code = '1' then a.sim_code 
          when b.user_id is not null then '2' 
          when b.user_id is null then value(u.sim_code,'0')
    end )
"
exec_sql $sql_buff

  aidb_runstats bass1.g_a_02004_02062_simcode 3
  
## 对于  sim_code 的增量 的 user_id 在  当日 02004 增量 里面的， 更新 02004.sim_code

set sql_buff "
update  (select * from g_a_02004_day where time_id = $timestamp ) a 
set a.sim_code =  (select new_sim_code from g_a_02004_02062_simcode b where a.user_id = b.user_id )
where exists (select 1 from g_a_02004_02062_simcode b where a.user_id = b.user_id )
"
exec_sql $sql_buff


# 对于  sim_code 的增量 de user_id 不 在  当日 02004 增量 里面的，插入 之
# 忽略离网用户的变化
set sql_buff "
insert into g_a_02004_day
select 
         $timestamp TIME_ID
        ,a.USER_ID
        ,a.CUST_ID
        ,a.USERTYPE_ID
        ,a.CREATE_DATE
        ,a.USER_BUS_TYP_ID
        ,a.PRODUCT_NO
        ,a.IMSI
        ,a.CMCC_ID
        ,a.CHANNEL_ID
        ,a.MCV_TYP_ID
        ,a.PROMPT_TYPE
        ,a.SUBS_STYLE_ID
        ,a.BRAND_ID
        ,b.new_sim_code SIM_CODE
from g_a_02004_02008_stage a 
,g_a_02004_02062_simcode b 
where a.user_id = b.user_id 
and b.user_id not in 
(
select b.user_id from 
(select * from g_a_02004_day where time_id = $timestamp ) a 
,bass1.g_a_02004_02062_simcode b 
where a.user_id = b.user_id 
)
and ( a.usertype_id NOT IN ('2010','2020','2030','9000') )
"
exec_sql $sql_buff

## 检查 主键唯一 

  #1.检查chkpkunique
set tabname "g_a_02004_day"
set pk                  "USER_ID"
chkpkunique ${tabname} ${pk} ${timestamp}

#配置依赖 校验！

return 0  

}