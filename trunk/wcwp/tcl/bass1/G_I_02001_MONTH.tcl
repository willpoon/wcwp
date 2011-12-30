######################################################################################################
#接口名称：资费营销案
#接口编码：02001
#接口说明：资费营销案是中国移动及其子公司为配合市场行销的需要而设计推出的销售方案，
#          它包括了能提供给用户的一种或多种业务功能，相应的月租，各个业务功能的资费定价（包括资费优惠）
#          以及能享受的优惠信息。
#程序名称: G_I_02001_MONTH.tcl
#功能描述: 生成02001的数据
#运行粒度: 月
#源    表：1.bass2.dwd_promoplan_yyyymmdd(资费营销案)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.集团对资费营销案标识有主键约束,但是根据西藏实际业务,不能满足需求.暂时不改
#          2.根据规范,本接口包含基本资费营销案（基本包),但目前只有资费营销案的可选包。鉴于BOSS已经送过来
#            资费营销案，所以直接取该接口的数据
#          3.该接口"基本资费营销案所属客户品牌标识"，实际业务存在三品牌通用资费营销案，无法归属
#          4.资费营销案存在资费营销案名称为“山南”，但是归属地局为全省的记录。
#修改历史: 1. 20090630 刘智龙 修改对 TD套餐标识td_plan_flag 的判断
#          2. 20090701 刘智龙 根据胡银辉要求修改口径 
#          3. 20090723 刘智龙 根据陈阳要求修改口径 .去掉对TD数据卡套餐的判断 --when A.prod_id in (90004026,90004027,90004028,90004029,90004050,73700005) then '1'
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02001_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
       
 set handle [aidb_open $conn]
 
##set sql_buff "insert into bass1.g_i_02001_month
##                   select a.time_id,
##                          a.plan_id,
##                          a.TYPE_ID,
##                          a.plan_name,
##                          a.brand_id,
##                          a.describe,
##                          a.START_DATE,
##                          a.end_date,
##                          a.CMCC_ID,
##                          case when a.plan_id in ('90004026','90004027','90004028','90004029','90004050','73700005') then '1'
##                          		 when a.plan_name like '%上网本%' then  '2'
##                               else '0' 
##                          end td_plan_flag
##                   from
##                    (
##                      select
##                        $op_month time_id
##                     	 ,char(prod_id) plan_id
##                     	 ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0077',type_id),'1') TYPE_ID
##                     	 ,prod_name plan_name
##                     	 ,'100' brand_id
##                     	 ,describe
##                     	 ,replace(char(date(valid_date)),'-','') START_DATE
##                     	 ,replace(char(date(expire_date)),'-','') END_DATE
##                     	 ,cmcc_id
##                     	 ,row_number()over(partition by char(prod_id) order by valid_date desc) row_id
##                      from 
##                      	 bass2.dwd_promoplan_$this_month_last_day
##                     ) a
##                     where a.row_id=1  "
                      
	set sql_buff "insert into bass1.g_i_02001_month
	                   select a.time_id,
	                          a.plan_id,
	                          a.TYPE_ID,
	                          a.plan_name,
	                          a.brand_id,
	                          a.describe,
	                          a.START_DATE,
	                          a.end_date,
	                          a.CMCC_ID,
	                          a.td_plan_flag
	                   from
	                    (
						select $op_month time_id 
									 ,char(A.prod_id )                                            as PLAN_ID
									 ,case when A.is_prom=0 then '1' else '2' end                 as TYPE_ID
									 ,A.prod_name                                                 as PLAN_NAME
									 ,case when value(B.brand_id, D.brand_id )=1 then '100'
									       when value(B.brand_id, D.brand_id ) in (3,5,7) then '200'
									       when value(B.brand_id, D.brand_id )=4 and A.prod_name like '%音乐%' then '301'
									       when value(B.brand_id, D.brand_id )=4 and A.prod_name not like '%音乐%' then '399'
									       else ''
									  end                       as BRAND_ID
									 ,''                                                          as DESCRIBE
									 ,replace(char(date(A.valid_date)),'-','')                      as START_DATE
									 ,replace(char(date(A.expire_date)),'-','')                     as END_DATE
									 ,case when left(char(A.prod_id),3)='891' or left(char(C.plan_id),3)='891' then '13101'      
									 			 when left(char(A.prod_id),3)='892' or left(char(C.plan_id),3)='892' then '13102'      
									 			 when left(char(A.prod_id),3)='893' or left(char(C.plan_id),3)='893' then '13103'      
									 			 when left(char(A.prod_id),3)='894' or left(char(C.plan_id),3)='894' then '13104'      
									 			 when left(char(A.prod_id),3)='895' or left(char(C.plan_id),3)='895' then '13105'      
									 			 when left(char(A.prod_id),3)='896' or left(char(C.plan_id),3)='896' then '13106'      
									 			 when left(char(A.prod_id),3)='897' or left(char(C.plan_id),3)='897' then '13107'      
									 	    else '13101' 
									 	end                as CMCC_ID
									 ,case 
							           when A.prod_name like '%上网本%' and A.is_prom=1 then  '2'
							           else '0' 
							      end                as TD_PLAN_FLAG
							     ,row_number()over(partition by char(A.prod_id) order by A.valid_date desc) row_id
						from  BASS2.DIM_PRODUCT_ITEM A
						left join BASS2.DWD_PM_PLANS_$this_month_last_day B on A.prod_id = B.plan_id
						left join BASS2.DWD_PLAN_PROM_$this_month_last_day C on A.prod_id = C.prom_id
						left join BASS2.DWD_PM_PLANS_$this_month_last_day D on c.plan_id = D.plan_id 
						where replace(char(date(A.expire_date)),'-','')>'$this_month_last_day'
                      ) a
                      where a.row_id=1  "                      
                
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle
	
	
  #删除异常实体渠道信息  
	set sql_buff "update G_I_02001_MONTH set CMCC_ID = ' ' where time_id = $op_month and CMCC_ID = '13101';"
	puts $sql_buff
  exec_sql $sql_buff
	

        return 0
}


#内部函数部分	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------
