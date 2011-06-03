CREATE FUNCTION bass1.get_task(p_control_code varchar(128))
RETURNS
TABLE (control_code varchar(128),FUNCTION_DESC           VARCHAR(200))
RETURN
select control_code,FUNCTION_DESC 
from app.sch_control_task 
where  locate(upper(p_control_code),upper(control_code)) > 0


---------------------------------------------------------------------------------------------


CREATE FUNCTION bass1.get_before(p_control_code varchar(128))
RETURNS
TABLE (control_code varchar(128),before_control_code varchar(128))
RETURN
select control_code,before_control_code from app.sch_control_before where control_code = p_control_code
---------------------------------------------------------------------------------------------

CREATE FUNCTION bass1.get_after(p_before_control_code varchar(128))
RETURNS
TABLE (control_code varchar(128),before_control_code varchar(128))
RETURN
select control_code,before_control_code from app.sch_control_before where before_control_code = p_before_control_code
---------------------------------------------------------------------------------------------

select * from  table( bass1.get_task('BASS1_G_I_03007_MONTH.tcl')) a 
select * from  table( bass1.get_before('BASS1_G_I_03007_MONTH.tcl')) a 
select * from  table( bass1.get_after('BASS1_G_I_03007_MONTH.tcl')) a 


---------------------------------------------------------------------------------------------


SET SCHEMA BASS1;

SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","BASS1";

CREATE FUNCTION "BASS1"."FN_GET_ALL_DIM_EX"
 ("GID" VARCHAR(20),
  "DID" VARCHAR(20)
 ) 
  RETURNS VARCHAR(32)
  LANGUAGE SQL
  DETERMINISTIC
  READS SQL DATA
  STATIC DISPATCH
  CALLED ON NULL INPUT
  NO EXTERNAL ACTION
  INHERIT SPECIAL REGISTERS
  BEGIN ATOMIC
    RETURN
      SELECT BASS1_VALUE
        FROM BASS1.ALL_DIM_LKP
        WHERE BASS1_TBID = GID
          AND XZBAS_VALUE = DID;
  END;

---------------------------------------------------------------------------------------------

drop FUNCTION bass1.chk_wave
CREATE FUNCTION bass1.chk_wave()
RETURNS
TABLE ( time_id int
        ,seq int
        ,rule_name varchar(128)
        ,wave_rate decimal(8,4)
        ,if_ok VARCHAR(8)
      )
RETURN
select 
time_id
,int(substr(rule_code,6)) seq
,case 
    when rule_code = 'R161_1' then '新增客户数'
    when rule_code = 'R161_2' then '客户到达数'
    when rule_code = 'R161_3' then '净增客户数'
    when rule_code = 'R161_4' then '通信客户数'
    when rule_code = 'R161_5' then '当月累计通信客户数'
    when rule_code = 'R161_6' then '使用TD网络的客户数'
    when rule_code = 'R161_7' then '当月累计使用TD网络的手机客户数'
    when rule_code = 'R161_8' then '当月累计使用TD网络的信息机客户数'
    when rule_code = 'R161_9' then '当月累计使用TD网络的数据卡客户数'
    when rule_code = 'R161_10' then '当月累计使用TD网络的上网本客户数'
    when rule_code = 'R161_11' then '联通移动客户总数'
    when rule_code = 'R161_12' then '电信移动客户总数'
    when rule_code = 'R161_13' then '联通移动新增客户数'
    when rule_code = 'R161_14' then '电信移动新增客户数'
    when rule_code = 'R161_15' then '使用TD网络的客户在T网上计费时长'
    when rule_code = 'R161_16' then '使用TD网络的客户在T网上的数据流量'
    when rule_code = 'R161_17' then '离网客户数' else '0' end rule_name 
    , target3*100 wave_rate
    ,case 
    when rule_code = 'R161_1' and abs(target3*100) > 15 then '超标'
    when rule_code = 'R161_2' and abs(target3*100) > 2 then '超标'
    when rule_code = 'R161_3' and abs(target3*100) > 100 then '超标'
    when rule_code = 'R161_4' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_5' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_6' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_7' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_8' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_9' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_10' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_11' and abs(target3*100) > 2 then '超标'
    when rule_code = 'R161_12' and abs(target3*100) > 2 then '超标'
    when rule_code = 'R161_13' and abs(target3*100) > 8 then '超标'
    when rule_code = 'R161_14' and abs(target3*100) > 8 then '超标'
    when rule_code = 'R161_15' and abs(target3*100) > 20 then '超标'
    when rule_code = 'R161_16' and abs(target3*100) > 20 then '超标'
    when rule_code = 'R161_17' and abs(target3*100) > 70 then '超标'
else '0' end if_ok
from 
bass1.g_rule_check a 
where rule_code like 'R161_%'
and time_id = int(replace(char(current date - 1 days),'-',''))

---------------------------------------------------------------------------------------------

select * from   table( bass1.chk_wave() ) a
order by 1

---------------------------------------------------------------------------------------------


drop FUNCTION bass1.chk_same
CREATE FUNCTION bass1.chk_same()
RETURNS
TABLE ( time_id int
        ,rule_name varchar(128)
        ,bass2_val decimal(18,5)
        ,bass1_val decimal(18,5)
        ,wave_rate_percent decimal(18,5)
      )
RETURN
select 
         time_id,
         case when rule_code='R159_1' then '新增客户数'
              when rule_code='R159_2' then '客户到达数'
              when rule_code='R159_3' then '上网本客户数'
              when rule_code='R159_4' then '离网客户数'
         end rule_name,
         target1,
         target2,
         target3*100
from bass1.g_rule_check
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
    and time_id=int(replace(char(current date - 1 days),'-',''))

---------------------------------------------------------------------------------------------

CREATE FUNCTION bass1.get_flret_cnt()
RETURNS 
TABLE ( dummy varchar(8),cnt    integer )
RETURN
select  'xxxxx' dummy,count(0) cnt from ( select  a.* ,row_number()over(partition by  substr(filename,18,5) 							order by deal_time desc ) rn 
from APP.G_FILE_REPORT a 
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') 
and err_code='00' 
) t where rn = 1 

select * from table(bass1.get_flret_cnt()) a;
---------------------------------------------------------------------------------------------



CREATE FUNCTION bass1.fn_dn_flret_cnt(p_deadline int)
RETURNS 
TABLE ( dummy varchar(8),cnt    integer )
RETURN
select  'xxxxx',count(0)
						from 
						( 
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
						from APP.G_FILE_REPORT a 
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') 
						and err_code='00' 
						and substr(filename,18,5) 
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE 
								   where deadline = p_deadline and sts = 1 
								) 
						) t where rn = 1 


select * from table(bass1.fn_dn_flret_cnt()) a;

