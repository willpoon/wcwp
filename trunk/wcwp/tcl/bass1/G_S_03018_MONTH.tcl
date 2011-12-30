######################################################################################################
#接口名称：集团客户非统付收入
#接口编码：03018
#接口说明：记录由集团客户统一付费的各项收入，即该帐务周期内集团客户账单收入与一次性费项收入之和，
#          需要各省级经分系统将二者进行合并后上传。
#程序名称: G_S_03018_MONTH.tcl
#功能描述: 生成03018的数据
#运行粒度: 月
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2009-07-02
#问题记录：采用原有03014的业务统计口径
#          20090727 修改82、152行代码 对用户类型过滤
#          2011-04-14 加入and a.service_id <> '926' ，剔除手机邮箱收入。
#					 2011-05-26 21:41:41 加入 and exists (select 1 from (select distinct user_id bass1.g_a_02004_day )u where a.user_id = u.user_id)
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2008-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        puts $timestamp
        #----求上月最后一天---#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        set objecttable "G_S_03018_MONTH"
        #set db_user $env(DB_USER)

        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.$objecttable where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

#01	用户标识	参见接口单元【用户】用户标识。	char(20)
#02	集团业务类型	参见维度指标说明中的BASS_STD1_0108。	char(4)
#03 应用管理模式 1-纳入mas管理 2-纳入adc管理 3-其他	char(1)
#04	个人非非统付帐目科目编码	0-通信费 1-功能费 2-其他 	char(1)
#05	应收金额	优惠后的应收金额，单位:分 	number(12)



 #建立G_S_03018_MONTH的session表，表名session.tmp03018
	set sql_buff "declare global temporary table session.tmp03018
	        (
	                user_id                char(20)   not null,
                  ent_busi_id            char(4)    not null,
                  manage_mod             char(1)    not null,
                  acct_type              char(1)    not null,
                  income                 bigint
                )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
  puts $sql_buff
  exec_sql $sql_buff
	puts "建立G_S_03018_MONTH的session表，表名session.tmp03018"


  #生成集团产品非统付收入
  #2011-04-14 加入and a.service_id <> '926' ，剔除手机邮箱收入。
	set sql_buff "insert into session.tmp03018
               	select  b.user_id       as user_id
                       ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')     as ent_busi_id
                       ,case
												  when d.BASS1_VALUE='1520' then '3'
													when upper(c.config_value)='MAS' then '1'
													when upper(c.config_value)='ADC' then '2'
													else '3'
											  end             as manage_mod
                       ,'1'             as acct_type
               	       ,sum(bigint(non_unipay_fee))  as income
                  from bass2.dw_enterprise_unipay_$op_month a,
                       (select acct_id,min(user_id) as user_id from  bass2.dw_product_$op_month where userstatus_id<>0 and usertype_id in (1,2,9) group by acct_id) b,
                       (select * from bass2.dim_service_config where config_id=1000027)  c,
				               (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') d
                 where a.acct_id = b.acct_id 
                   and a.service_id =c.service_id
                   and a.service_id = d.xzbas_value
                   and a.test_mark = 0 
                   and a.service_id not in ( '926','717')
                 group by b.user_id,
                 coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002'),
                 case
									when d.BASS1_VALUE='1520' then '3'
									when upper(c.config_value)='MAS' then '1'
									when upper(c.config_value)='ADC' then '2'
									else '3'
							   end"
  puts $sql_buff
  exec_sql $sql_buff
	puts "生成商信通非统付费用(个人)"

  #2011-05-26 21:41:41 加入                 			 and exists (select 1 from (select distinct user_id bass1.g_a_02004_day )u where a.user_id = u.user_id)

 #生成财信通1300、校讯通1310、银信通1380、农信通1320收入
 set sql_buff "insert into session.tmp03018
               	select  a.user_id   as user_id
                       ,case when apptype_id =1 then  '1310'
                       			 when apptype_id =2 then  '1380'
                       			 when apptype_id =3 then  '1320'
                       			 when apptype_id =4 then  '1300'
                         end as ent_busi_id
                       ,case when apptype_id =3 then '2'
                       			 when apptype_id =4 then '1'
                       	else '3' end as manage_mod
                       ,'1'         as acct_type
               	       ,sum(fee)    as income
                  from bass2.dw_enterprise_industry_apply  a
                 where a.apptype_id in (1,2,3,4) 
		 and a.op_time = '$this_month_firstday' 
                 and a.user_id is not null 
                 and exists (select 1 from (select distinct user_id from bass1.g_a_02004_day )u where a.user_id = u.user_id)
                 group by a.user_id,
		                 case when apptype_id =1 then  '1310'
		               			  when apptype_id =2 then  '1380'
		               			  when apptype_id =3 then  '1320'
		               			  when apptype_id =4 then  '1300'
		                  end,
		                 case when apptype_id =3 then '2'
		                 			when apptype_id =4 then '1'
		                 	    else '3' 
		                 	end"

  exec_sql $sql_buff

#20110805 加入 1230 blackberry 非统付
set sql_buff "insert into session.tmp03018
               	select 
		 USER_ID
		 ,'1230' ENT_BUSI_ID
		 ,'3' MANAGE_MOD
		 ,'1' acct_type
		 ,sum(bigint(non_unipay_fee))  as income
		 from  bass2.dw_enterprise_new_unipay_$op_month a
		 ,(select acct_id,min(user_id) as user_id from  bass2.dw_product_$op_month where userstatus_id<>0 and usertype_id in (1,2,9) group by acct_id) b
		 where a.test_mark = 0
		 and a.acct_id = b.acct_id
		 and a.ITEM_ID in  (80000180,80000184,80000598,80000599,80000599,82000111)
		 group by USER_ID
		 with ur
		"

  exec_sql $sql_buff



	#============向正式表中插入数据============================
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_03018_MONTH
                  select
                    ${op_month},
                    aa.user_id,
                    aa.ent_busi_id,
                    aa.manage_mod,
                    aa.acct_type,
                    char(aa.income)
                  from
                  (
                   select
                     a.user_id         as user_id ,
                     a.ent_busi_id     as ent_busi_id,
                     a.manage_mod      as manage_mod,
                     a.acct_type       as acct_type,
                     sum(a.income*100) as income
                  from
                      session.tmp03018 a
                    , bass2.dw_product_$op_month b
                  where a.user_id=b.user_id
                  		and a.ent_busi_id  <> '4002'
                  		and b.usertype_id in (1,2,9)
                  group by
                      a.user_id ,
                      a.ent_busi_id,
                      a.manage_mod,
                      a.acct_type
                )aa"

          puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "向正式表中插入数据"



	aidb_close $handle

	return 0
}

