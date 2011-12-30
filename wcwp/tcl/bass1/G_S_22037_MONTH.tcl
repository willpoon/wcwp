######################################################################################################
#接口名称：集团客户M2M产品使用情况
#接口编码：22037
#接口说明：记录集团客户业务中M2M产品的使用情况。
#程序名称: G_S_22037_MONTH.tcl
#功能描述: 生成22037的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #本月最后一天 $op_monthdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        puts $op_time
        puts $op_month 
      
      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      
      set thismonth [string range $op_month 4 5]
      set thisyear  [string range $op_month 0 3] 

      #得到本月1号的日期
      puts $op_month
      #上月	$last_month	
      set last_month [GetLastMonth [string range $op_month 0 5]]

      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay 


      #得到上个月的1号日期
    	set sql_buff "select date('$ThisMonthFirstDay')-1 month from bass2.dual"
	    puts $sql_buff
	    set LastMonthFirstDay [get_single $sql_buff]
	    puts $LastMonthFirstDay
      set LastMonthYear [string range $LastMonthFirstDay 0 3]
      set LastMonth [string range $LastMonthFirstDay 5 6]
	    
      #得到下个月的1号日期
    	set sql_buff "select date('$ThisMonthFirstDay')+1 month from bass2.dual"
	    puts $sql_buff
	    set NextMonthFirstDay [get_single $sql_buff]
	    puts $NextMonthFirstDay
      set NextMonthYear [string range $NextMonthFirstDay 0 3]
      set NextMonth [string range $NextMonthFirstDay 5 6]

	    
     
       
  #删除本期数据
	set sql_buff "delete from bass1.g_s_22037_month where time_id=$op_month"
	puts $sql_buff
	exec_sql $sql_buff
	 
 
  #建立临时表
	set sql_buff "declare global temporary table session.g_s_22037_month_tmp
	              (FAB_TYPE           CHARACTER(3),
                 ENT_NUM            bigint,
                 ENTPERSON_NUM      bigint,
                 UNENTPERSON_NUM    bigint,
                 ENTPERSON_NEWNUM   bigint,
                 ENTPERSON_LOSTNUM  bigint,
                 USER_NUM           bigint,
                 USER_NUMLJ         bigint,
                 USER_NUMJF         bigint,
                 USER_NUMJFLJ       bigint,
                 UPSMS_COUNT        bigint,
                 DOWNSMS_COUNT      bigint
                )  
                partitioning key 
                (FAB_TYPE)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	exec_sql $sql_buff
	
  #03	订购集团数
  #04	集团个人订购客户数
  #05	非集团个人订购客户数
  #06	新增订购用户数
  #07	退订用户数
  #08	当月使用客户数
  #09	累计使用客户数
  #10	当月计费客户数
  #11	累计计费客户数
  #12	上行短信业务量
  #13	下行短信业务量



#===================================================================================================    
#H001 财信通	财信通订购集团数	                      家
#H003 财信通	其中：集团个人客户数	                  户
#H004 财信通	     非集团个人客户数	                  户
#H005 财信通	财信通新增订购用户数	                  户
#H006 财信通	财信通退订用户数	                      户
#H007 财信通	财信通当月使用客户数	                  户
#H008 财信通	财信通累计使用客户数	                  户
#H009 财信通	财信通当月计费客户数	                  户
#H010 财信通	财信通累计计费客户数	                  户
#H014 财信通	--其中上行短信业务量	                  MB
#H015 财信通	--其中下行短信业务量	                  MB
#===================================================================================================
#H031	校讯通	校讯通订购集团数	                      家	
#H033	校讯通	其中：集团个人客户数	                  户	
#H034	校讯通	       非集团个人客户数	                户	
#H035	校讯通	校讯通新增订购用户数	                  户	
#H036	校讯通	校讯通退订用户数	                      户	
#H037	校讯通	校信通当月使用客户数	                  户	
#H038	校讯通	校信通累计使用客户数	                  户	
#H039	校讯通	校讯通当月计费客户数	                  户	
#H040	校讯通	校信通累计计费客户数	                  户	
#H044	校讯通	--其中上行短信业务量	                  MB	
#H045	校讯通	--其中下行短信业务量	                  MB	
#===================================================================================================
#H046	银信通	本地银信通银信通订购集团数	            家	
#H048	银信通  其中： 集团个人客户数	                  户	
#H049	银信通       非集团个人客户数	                  户	
#H050	银信通  本地银信通银信通新增订购用户数	        户	
#H051	银信通  本地银信通银信通退订用户数	            户	
#H052	银信通  本地银信通银信通当月使用客户数	        户	
#H053	银信通  本地银信通银信通累计使用客户数	        户	
#H054	银信通  本地银信通银信通当月计费客户数	        户	
#H055	银信通  本地银信通银信通累计计费客户数	        户	
#H059	银信通  --其中上行短信业务量	                  MB	
#H060	银信通  --其中下行短信业务量	                  MB	
#===================================================================================================
#H061	农信通	农信通订购集团数	                      家	
#H064	农信通	其中：集团个人客户数	                  户	
#H065	农信通	     非集团个人客户数	                  户	
#H067	农信通	农信通新增订购用户数	                  户	
#H069	农信通	农信通退订用户数	                      户	
#H073	农信通	农信通当月使用客户数	                  户	
#H075	农信通	农信通累计使用客户数	                  户	
#H077	农信通	农信通当月使用客户数	                  户	
#H079	农信通	农信通累计计费客户数	                  户	
#H084	农信通	--其中上行短信业务量	                  MB	
#H085	农信通	--其中下行短信业务量	                  MB	
#===================================================================================================
#警务通	H016	警务通订购集团数	                      家	        
#警务通	H017	警务通订购用户数	                      户	        
#警务通	H018	其中：  集团个人客户数	                户	        
#警务通	H019	      非集团个人客户数	                户	        
#警务通	H020	警务通新增订购用户数	                  户	        
#警务通	H021	警务通退订用户数	                      户	        
#警务通	H022	警务通当月使用客户数	                  户	        
#警务通	H023	警务通累计使用客户数	                  户	        
#警务通	H024	警务通当月计费客户数	                  户	        
#警务通	H025	警务通累计计费客户数	                  户	        
#警务通	H029	--其中上行短信业务量	                  MB	        
#警务通	H030	--其中下行短信业务量	                  MB	     
#===================================================================================================

#集团行业应用
#H001 财信通	财信通订购集团数	                      家    
#H031	校讯通	校讯通订购集团数	                      家
#H046	银信通	本地银信通银信通订购集团数	            家	
#H061	农信通	农信通订购集团数	                      家	
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通


    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENT_NUM)
                   select '110',
                          count(distinct enterprise_id)
                     from bass2.dw_enterprise_sub_$op_month
                    where service_id = '911' and rec_status = 1 and enterprise_id not in ('891910006274')"
  	puts $sql_buff
	  exec_sql $sql_buff
	
	
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENT_NUM)
                   select case b.apptype_id
                             when 1 then '110'
                             when 2 then '120'
                             when 3 then '140'
                             when 4 then '210'  
                          end,
                          count(distinct a.enterprise_id)
                   from bass2.dw_enterprise_industry_apply b,bass2.dw_enterprise_member_mid_$op_month a 
                   where a.dummy_mark = 0
                         and b.op_time = '$ThisMonthFirstDay'
                         and b.apptype_id in (3) and a.user_id = b.user_id
                   group by case b.apptype_id
                             when 1 then '110'
                             when 2 then '120'
                             when 3 then '140'
                             when 4 then '210'  
                           end;"
	puts $sql_buff
	exec_sql $sql_buff


	
	
set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENT_NUM)
                  select  '120',count(distinct a.ENTERPRISE_ID)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a 
                   where b.MOBILE_NUM = a.product_no    
                   and b.OPERATE_TYPE = '已签约' and b.OPERATE_DATE <'$NextMonthFirstDay' and b.OPERATE_DATE >= '$ThisMonthFirstDay'
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%')"
	puts $sql_buff
	exec_sql $sql_buff
                            
                            
                            
                            	

#警务通	H016	警务通订购集团数	 家
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENT_NUM)
                   select
                        '100',
                        count(distinct a.enterprise_id)
                      from 
                        bass2.dw_enterprise_sub_$op_month a,bass2.dw_enterprise_msg_$op_month b
                     where a.SERVICE_ID = '945' and a.enterprise_id = b.enterprise_id
                           and a.enterprise_id not in ('891880005002')"
	puts $sql_buff
	exec_sql $sql_buff


#H033	校讯通	其中： 集团个人客户数	  户	
#H034	校讯通	     非集团个人客户数	  户	
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通

#select count(distinct user_id) from bass2.dw_enterprise_membersub_$op_month where order_id in (select  order_id  from bass2.dw_enterprise_sub_$op_month where service_id = '911' and rec_status = 1 and 
#enterprise_id not in ('891910006274')) and rec_status = 1  with ur




 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                       select '110', count(distinct a.user_id)
                   from  
                        bass2.dw_enterprise_membersub_$op_month a,
                        bass2.dw_enterprise_member_mid_$op_month b
                   left join
                        (select distinct enterprise_id 
                           from bass2.dw_enterprise_sub_$op_month 
                          where service_id = '911' and rec_status = 1 and 
                                enterprise_id not in ('891910006274')
                        ) c
                     on b.enterprise_id = c.enterprise_id
                  where a.USER_ID = b.USER_ID and
                        order_id in (select  order_id  from bass2.dw_enterprise_sub_$op_month where service_id = '911' and rec_status = 1 and 
                        enterprise_id not in ('891910006274')) and 
                        rec_status = 1 and c.enterprise_id is not null
                        group by c.enterprise_id;"
	puts $sql_buff
	exec_sql $sql_buff

  set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                       select '110', count(distinct a.user_id) result
						           	from
								           bass2.dw_enterprise_membersub_$op_month a
						           	left join
							           	bass2.dw_enterprise_member_mid_$op_month b
							           on
							           	a.USER_ID = b.USER_ID
						           	left join
						          		(select distinct enterprise_id from bass2.dw_enterprise_sub_$op_month where service_id = '911' and rec_status = 1 and enterprise_id not in ('891910006274')) c
							           on
							            b.enterprise_id = c.enterprise_id 
						           	where
							              	order_id in (select  order_id  from bass2.dw_enterprise_sub_$op_month where service_id = '911' and rec_status = 1 and enterprise_id not in ('891910006274')) and rec_status = 1
                              and c.enterprise_id is null ;"
	puts $sql_buff
	exec_sql $sql_buff

  
	
	
	


                          
                          
#    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
#                       select '110', count(distinct b.user_id)
#                   from bass2.dw_enterprise_industry_apply b left outer join 
#                   (select user_id as user_id,level_def_mode as level_def_mode,ent_city_id as ent_city_id 
#				           from bass2.dw_enterprise_member_mid_$op_month where dummy_mark = 0 group by user_id,level_def_mode,ent_city_id) a on a.user_id = b.user_id
#                   left outer join bass2.dw_product_$op_month c
#                   on a.user_id = c.user_id
#                   where b.apptype_id in (1)
#                     and b.op_time ='$ThisMonthFirstDay' and c.ENTERPRISE_MARK = 1;"
#	puts $sql_buff
#	exec_sql $sql_buff
#
#    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
#                       select '110', count(distinct b.user_id)
#                   from bass2.dw_enterprise_industry_apply b left outer join 
#                   (select user_id as user_id,level_def_mode as level_def_mode,ent_city_id as ent_city_id 
#				           from bass2.dw_enterprise_member_mid_$op_month where dummy_mark = 0 group by user_id,level_def_mode,ent_city_id) a on a.user_id = b.user_id
#                   left outer join bass2.dw_product_$op_month c
#                   on a.user_id = c.user_id
#                   where b.apptype_id in (1)
#                     and b.op_time ='$ThisMonthFirstDay' and (c.ENTERPRISE_MARK = 0 or c.ENTERPRISE_MARK is null ) ;"
#	puts $sql_buff
#	exec_sql $sql_buff




#H048	银信通	其中：  集团个人客户数	     户	
#H049	银信通	      非集团个人客户数	     户	
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                   select  '120', 
                           count(distinct a.user_id)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a ,bass2.dw_product_$op_month c
                   where b.MOBILE_NUM = a.product_no
                   and b.MOBILE_NUM = c.product_no  
                   and b.OPERATE_TYPE = '已签约' 
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%')
                  and c.ENTERPRISE_MARK = 1;"
	puts $sql_buff
	exec_sql $sql_buff

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                   select  '120', 
                           count(distinct a.user_id)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a ,bass2.dw_product_$op_month c
                   where b.MOBILE_NUM = a.product_no
                   and b.MOBILE_NUM = c.product_no  
                   and b.OPERATE_TYPE = '已签约' 
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%')
                     and (c.ENTERPRISE_MARK = 0 or c.ENTERPRISE_MARK is null );"
	puts $sql_buff
	exec_sql $sql_buff


#===================================================================================================   
#H064	农信通	其中： 集团个人客户数	户	
#H065	农信通	     非集团个人客户数	户	
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                       select '140', count(distinct b.user_id)
                   from bass2.dw_enterprise_industry_apply b left outer join 
                   (select user_id as user_id,level_def_mode as level_def_mode,ent_city_id as ent_city_id 
				           from bass2.dw_enterprise_member_mid_$op_month where dummy_mark = 0 group by user_id,level_def_mode,ent_city_id) a on a.user_id = b.user_id
                   left outer join bass2.dw_product_$op_month c
                   on a.user_id = c.user_id
                   where b.apptype_id in (3)
                     and b.op_time ='$ThisMonthFirstDay' and c.ENTERPRISE_MARK = 1;"
	puts $sql_buff
	exec_sql $sql_buff

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                       select '140', count(distinct b.user_id)
                   from bass2.dw_enterprise_industry_apply b left outer join 
                   (select user_id as user_id,level_def_mode as level_def_mode,ent_city_id as ent_city_id 
				           from bass2.dw_enterprise_member_mid_$op_month where dummy_mark = 0 group by user_id,level_def_mode,ent_city_id) a on a.user_id = b.user_id
                   left outer join bass2.dw_product_$op_month c
                   on a.user_id = c.user_id
                   where b.apptype_id in (3)
                     and b.op_time ='$ThisMonthFirstDay' and (c.ENTERPRISE_MARK = 0 or c.ENTERPRISE_MARK is null );"
	puts $sql_buff
	exec_sql $sql_buff


#===================================================================================================   
#H003 财信通	其中：集团个人客户数	                  户
#H004 财信通	     非集团个人客户数	                  户	
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                       select '210', count(distinct b.user_id)
              from  bass2.DW_PRODUCT_REGSP_$op_month b left outer join bass2.dw_enterprise_member_mid_$op_month a on a.user_id = b.user_id 
              left outer join bass2.dw_product_$op_month c on b.user_id = c.user_id 
              where a.dummy_mark = 0 
                    and b.sts=1 
                    and b.SP_CODE in ('400002','901848','900139','810611')
                    and b.VALID_DATE <'$NextMonthFirstDay' and b.EXPIRE_DATE >= '$NextMonthFirstDay'
                    and c.ENTERPRISE_MARK = 1;"
	puts $sql_buff
	exec_sql $sql_buff

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                       select '210', count(distinct b.user_id)
              from  bass2.DW_PRODUCT_REGSP_$op_month b left outer join bass2.dw_enterprise_member_mid_$op_month a on a.user_id = b.user_id 
              left outer join bass2.dw_product_$op_month c on b.user_id = c.user_id 
              where a.dummy_mark = 0 
                    and b.sts=1 
                    and b.SP_CODE in ('400002','901848','900139','810611')
                    and b.VALID_DATE <'$NextMonthFirstDay' and b.EXPIRE_DATE >= '$NextMonthFirstDay'
                    and (c.ENTERPRISE_MARK = 0 or c.ENTERPRISE_MARK is null );"
	puts $sql_buff
	exec_sql $sql_buff
	
	
#警务通	H018	其中：  集团个人客户数	                户	        
#警务通	H019	      非集团个人客户数	                户	        
#    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
#                   select '100', count(distinct b.user_id)
#                   from bass2.DW_PRODUCT_BUSI_SPROM_DM_$op_month a left outer join bass2.dw_enterprise_member_mid_$op_month b on a.user_id = b.user_id 
#                   left outer join bass2.dw_product_$op_month c
#                   on  b.user_id =c.user_id
#                   where a.sprom_id=90004001 and a.busi_type=945
#                   and a.user_id not in ('1160046359') and  c.ENTERPRISE_MARK = 1;"
#	puts $sql_buff
#	exec_sql $sql_buff

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                   select '100', count(distinct a.user_id)
                   from bass2.dw_enterprise_extsub_rela_$op_month a left outer join bass2.dw_enterprise_member_mid_$op_month b on a.user_id = b.user_id 
                   left outer join bass2.dw_product_$op_month c
                   on  b.user_id =c.user_id
                   where a.SERVICE_ID = '945' 
                     and a.enterprise_id not in ('891880005002','891910006274') 
                     and a.rec_status=1 and c.ENTERPRISE_MARK = 1 ;"
	puts $sql_buff
	exec_sql $sql_buff
							                
							                
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                   select '100', count(distinct a.user_id)
                   from bass2.dw_enterprise_extsub_rela_$op_month a left outer join bass2.dw_enterprise_member_mid_$op_month b on a.user_id = b.user_id 
                   left outer join bass2.dw_product_$op_month c
                   on  b.user_id =c.user_id
                   where a.SERVICE_ID = '945' 
                     and a.enterprise_id not in ('891880005002','891910006274') 
                     and a.rec_status=1 and c.ENTERPRISE_MARK is null ;"
	puts $sql_buff
	exec_sql $sql_buff

							                
#    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
#                  select '100', count(distinct b.user_id)
#                   from bass2.DW_PRODUCT_BUSI_SPROM_DM_$op_month a left outer join bass2.dw_enterprise_member_mid_$op_month b on a.user_id = b.user_id 
#                   left outer join bass2.dw_product_$op_month c
#                   on  b.user_id =c.user_id
#                   where a.sprom_id=90004001 and a.busi_type=945
#                   and a.user_id not in ('1160046359') ;"
#	puts $sql_buff
#	exec_sql $sql_buff
	
#===================================新增订购用户数==============================================   
#财信通	H005	财信通新增订购用户数	户	0

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NEWNUM)
                select '210',   
                     count(distinct b.user_id)
              from  bass2.DW_PRODUCT_REGSP_$op_month b left outer join bass2.dw_enterprise_member_mid_$op_month a on a.user_id = b.user_id 
              where a.dummy_mark = 0
                    and b.sts=1 
                    and b.SP_CODE in ('400002','901848','900139','810611')
                    and b.STS_DATE>='$ThisMonthFirstDay' and b.STS_DATE<'$NextMonthFirstDay'; "
	puts $sql_buff
	exec_sql $sql_buff


#H035	校讯通	校讯通新增订购用户数	 户	 
  set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NEWNUM)
              select 
                     '110',
                     count(distinct a.user_id)
              from bass2.dw_enterprise_member_mid_$op_month a,
			             (select user_id as user_id from bass2.dw_enterprise_industry_apply where op_time='$ThisMonthFirstDay' and apptype_id in (1) group by user_id
                    except
                    select user_id as user_id from bass2.dw_enterprise_industry_apply where op_time='$LastMonthFirstDay' and apptype_id in (1) group by user_id
                   ) b,
				          bass2.dw_product_$op_month c
              where a.dummy_mark = 0
                and a.user_id = b.user_id  
                and a.user_id = c.user_id    
             with ur;"
	puts $sql_buff
	exec_sql $sql_buff

 
             
             
                                    
                        

#H050	银信通  本地银信通银信通新增订购用户数	        户	 
   set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NEWNUM)
                  select  '120',
                          count(distinct a.user_id)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a 
                   where b.MOBILE_NUM = a.product_no    
                   and b.OPERATE_TYPE = '已签约' and b.OPERATE_DATE <'$NextMonthFirstDay' and b.OPERATE_DATE >= '$ThisMonthFirstDay'
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%');"
	puts $sql_buff
	exec_sql $sql_buff

  
#警务通	H020	警务通新增订购用户数	                  户	
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NEWNUM)
                   select '100',
                         count(distinct a.user_id)
                   from 
                   (select 
                                         user_id
                                         from 
                                      bass2.DW_PRODUCT_BUSI_SPROM_DM_$op_month
                                      where  sprom_id=90004001 and  busi_type=945
                                      and  user_id not in ('1160046359')
                   except
                   select 
                                         user_id
                                         from 
                                      bass2.DW_PRODUCT_BUSI_SPROM_DM_$last_month
                                      where  sprom_id=90004001 and  busi_type=945
                                      and  user_id not in ('1160046359')) a left outer join bass2.dw_enterprise_member_mid_200807 b            
                   on a.user_id =b.user_id                 
                   group by  case 
                                  when b.level_def_mode = 1 then 888
                                  else value(int(b.ent_city_id),891)
                         end"      
	puts $sql_buff
	exec_sql $sql_buff

#===================================通退订用户数==============================================   
#财信通	H006	财信通退订用户数	户	0    
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_LOSTNUM)
                select '210',   
                     count(distinct b.user_id)
              from  (select user_id from bass2.DW_PRODUCT_REGSP_${LastMonthYear}${LastMonth}  
                     where  sts=1 and SP_CODE in ('400002','901848','900139','810611')
                     and VALID_DATE <'$ThisMonthFirstDay' and EXPIRE_DATE >= '$ThisMonthFirstDay' 
                     group by user_id
                     except 
                     select user_id from bass2.DW_PRODUCT_REGSP_$op_month 
                     where  sts=1 and SP_CODE in ('400002','901848','900139','810611')
                     and VALID_DATE <'$NextMonthFirstDay' and EXPIRE_DATE >= '$ThisMonthFirstDay'
                     group by user_id
                    )b     
              left outer join bass2.dw_enterprise_member_mid_$op_month a on a.user_id = b.user_id 
              where a.dummy_mark = 0;"
	puts $sql_buff
	exec_sql $sql_buff
                       
                       
#H036	校讯通	校讯通退订用户数	                      户	
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_LOSTNUM)
               select 
                     '110',
                     count(distinct a.user_id)
              from bass2.dw_enterprise_member_mid_$op_month a,
			             (select user_id as user_id from bass2.dw_enterprise_industry_apply where op_time='$LastMonthFirstDay' and apptype_id in (1) group by user_id
                    except
                    select user_id as user_id from bass2.dw_enterprise_industry_apply where op_time='$ThisMonthFirstDay' and apptype_id in (1) group by user_id
                   ) b,
				          bass2.dw_product_$op_month c
              where a.dummy_mark = 0
                and a.user_id = b.user_id  
                and a.user_id = c.user_id    
             with ur;"
	puts $sql_buff
	exec_sql $sql_buff





#H051	银信通  本地银信通银信通退订用户数	            户	    
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_LOSTNUM)
                   select '120',
                          count(distinct a.user_id)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a 
                   where b.MOBILE_NUM = a.product_no    
                   and b.OPERATE_TYPE = '已解约' and b.OPERATE_DATE <'$NextMonthFirstDay' and b.OPERATE_DATE >= '$ThisMonthFirstDay'
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%');"
	puts $sql_buff
	exec_sql $sql_buff



#警务通	H021	警务通退订用户数	                      户
   set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_LOSTNUM)
                 select '100',
                         count(distinct a.user_id)
                   from 
                   (select 
                                         user_id
                                         from 
                                      bass2.DW_PRODUCT_BUSI_SPROM_DM_$last_month
                                      where  sprom_id=90004001 and  busi_type=945
                                      and  user_id not in ('1160046359')
                   except
                   select 
                                         user_id
                                         from 
                                      bass2.DW_PRODUCT_BUSI_SPROM_DM_$op_month
                                      where  sprom_id=90004001 and  busi_type=945
                                      and  user_id not in ('1160046359')    ) a left outer join bass2.dw_enterprise_member_mid_200807 b            
                   on a.user_id =b.user_id  "
	puts $sql_buff
	exec_sql $sql_buff
                         
                         
                         
                         
                         
#=================================当月使用客户数==============================================   
#H037	  校讯通	  校信通当月使用客户数	          户	
#H052	  银信通	  本地银信通银信通当月使用客户数	户	
#H073	  农信通	  农信通当月使用客户数	          户	
#H007   财信通	  财信通当月使用客户数	          户
#H022   警务通		警务通当月使用客户数	          户
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUM)
                    select case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140' 
                              when 4 then '210'
                              when 5 then '100'
                          end,
                          count(distinct a.user_id)
                   from bass2.dw_enterprise_industry_apply a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b 
                        on a.user_id=b.user_id
                   where a.op_time = '$ThisMonthFirstDay'
                     and a.apptype_id in (1,2,3,4,5)
                   group by case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140' 
                              when 4 then '210'
                              when 5 then '100'
                          end;"
	puts $sql_buff
	exec_sql $sql_buff

#add by zhanght on 2009.06.17
#农信通	  农信通当月使用客户数 其中：12582用户数
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUM)
                    select '582',
                          count(distinct a.user_id)
                   from bass2.dw_enterprise_industry_apply a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b 
                        on a.user_id=b.user_id
                   where a.op_time = '$ThisMonthFirstDay'
                     and a.apptype_id = 3
                   with ur"
	puts $sql_buff
	exec_sql $sql_buff



#=================================通累计使用客户数=============================================   
#H038	校讯通	校信通累计使用客户数	          户	
#H053	银信通	本地银信通银信通累计使用客户数	户	
#H075	农信通	农信通累计使用客户数	          户	
#H008 财信通	财信通累计使用客户数	          户
#H023 警务通	警务通累计使用客户数	          户
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUMLJ)
                   select case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140' 
                              when 4 then '210'
                              when 5 then '100'
                          end,
                            count(distinct a.user_id)
                     from (select user_id,city_id,apptype_id
                           from bass2.dw_enterprise_industry_apply
                           where op_time between '$thisyear-01-01' and '$ThisMonthFirstDay'
                             and apptype_id in (1,2,3,4,5)
                           group by user_id,city_id,apptype_id
                           having count(distinct op_time) >= 3
                           union all
                           select a.user_id,a.city_id,a.apptype_id
                           from bass2.dw_enterprise_industry_apply a left outer join bass2.dw_product_$op_month b
                             on a.user_id = b.user_id
                           where a.op_time between '$thisyear-01-01' and '$ThisMonthFirstDay'
                             and a.apptype_id in (1,2,3,4,5)
                             
                           group by a.user_id,a.city_id,a.apptype_id) a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b
                               on a.user_id=b.user_id
                     group by case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140' 
                              when 4 then '210'
                              when 5 then '100'
                          end;"
	puts $sql_buff
	exec_sql $sql_buff




#===============================当月计费客户数=============================================   
#校讯通	H039	校讯通当月计费客户数	          户	        
#银信通	H054	本地银信通银信通当月计费客户数	户	  
#财信通	H009	财信通当月计费客户数	          户	          
#警务通	H024	警务通当月计费客户数	          户
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUMJF)
                   select case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140'
                              when 4 then '210'
                              when 5 then '100'
                          end,
                          count(distinct a.user_id)
                   from (select user_id,city_id,apptype_id
                         from bass2.dw_enterprise_industry_apply
                         where op_time = '$ThisMonthFirstDay'
                           and apptype_id in (1,2,3,4,5)
                           and fee > 0
                         group by user_id,city_id,apptype_id
                        ) a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b 
                   on a.user_id=b.user_id
                   group by case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140'
                              when 4 then '210'
                              when 5 then '100'
                          end;"
	puts $sql_buff
	exec_sql $sql_buff
                          
                          
#===============================累计计费客户数============================================   
#H040	校讯通	校信通累计计费客户数	          户	
#H055	银信通	本地银信通银信通累计计费客户数	户
#H079	农信通	农信通累计计费客户数          	户
#H010 财信通	财信通累计计费客户数	          户
#H025 警务通	警务通累计计费客户数	          户
#110-校信通 #120-银信通 #130-气象通 #140-农信通 #150-城管通 #160-商贸通 #170-医疗通 #180-物流通 #190-电力通 #200-安防通 #210-财信通

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUMJFLJ)
                   select case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140'
                              when 4 then '210'
                              when 5 then '100'
                          end,
                          count(distinct a.user_id)
                   from (select user_id,city_id,apptype_id
                         from bass2.dw_enterprise_industry_apply
                         where op_time between '$thisyear-01-01' and '$ThisMonthFirstDay'
                           and apptype_id in (1,2,3,4,5)
                           and fee > 0
                         group by user_id,city_id,apptype_id
                         having count(distinct op_time) >= 3
                         union all
                         select a.user_id,a.city_id,a.apptype_id
                         from bass2.dw_enterprise_industry_apply a,bass2.dw_product_$op_month b
                         where a.op_time between '$thisyear-01-01' and '$ThisMonthFirstDay'
                           and a.apptype_id in (1,2,3,4,5)
                           and a.fee > 0
                           and a.user_id = b.user_id
                         group by a.user_id,a.city_id,a.apptype_id) a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b 
                   on a.user_id=b.user_id
                   group by case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140'
                              when 4 then '210'
                              when 5 then '100'
                          end;"                         
	puts $sql_buff
	exec_sql $sql_buff
                          

                          
#===============================上下行短信业务量===========================================   
#银信通	H059	        --其中上行短信业务量	MB	0	          
#银信通	H060	        --其中下行短信业务量	MB	0	 

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UPSMS_COUNT)
                   select '120',
                     sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and sp_code = '931027'  and b.CALLTYPE_ID <> 1"
	puts $sql_buff
	exec_sql $sql_buff

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,DOWNSMS_COUNT)
                   select '120',
                     sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and sp_code = '931027'  and b.CALLTYPE_ID = 1;"
	puts $sql_buff
	exec_sql $sql_buff


#农信通	H084	--其中上行短信业务量	MB	0	      
#农信通	H085	--其中下行短信业务量	MB	0	有数据
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UPSMS_COUNT)
                   select '140',
                     sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and SER_CODE='12582' and b.CALLTYPE_ID <> 1"
	puts $sql_buff
	exec_sql $sql_buff
                     
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,DOWNSMS_COUNT)
                   select '140',
                     sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and SER_CODE='12582' and b.CALLTYPE_ID = 1;"
	puts $sql_buff
	exec_sql $sql_buff
                     
                     
                     
                       
                       
#H014 财信通	--其中上行短信业务量	                  MB
#H015 财信通	--其中下行短信业务量	                  MB
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UPSMS_COUNT)
                   select '210',
                   sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and b.sp_code in ('901848','400002','900139') and b.CALLTYPE_ID <> 1"
	puts $sql_buff
	exec_sql $sql_buff

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,DOWNSMS_COUNT)
                   select '210',
                   sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and b.sp_code in ('901848','400002','900139') and b.CALLTYPE_ID = 1;"
	puts $sql_buff
	exec_sql $sql_buff








	#============向正式表中插入数据============================
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_22037_MONTH 
                  select
                  ${op_month},
                  '${op_month}',
                  FAB_TYPE,
                  value(char(ENT_NUM          ),'0'),
                  value(char(ENTPERSON_NUM    ),'0'),
                  value(char(UNENTPERSON_NUM  ),'0'),
                  value(char(ENTPERSON_NEWNUM ),'0'),
                  value(char(ENTPERSON_LOSTNUM),'0'),
                  value(char(USER_NUM         ),'0'),
                  value(char(USER_NUMLJ       ),'0'),
                  value(char(USER_NUMJF       ),'0'),
                  value(char(USER_NUMJFLJ     ),'0'),
                  value(char(UPSMS_COUNT      ),'0'),
                  value(char(DOWNSMS_COUNT    ),'0')
                  from
                  (
                   select
                     FAB_TYPE  as FAB_TYPE,
                     sum(ENT_NUM          ) as ENT_NUM          ,
                     sum(ENTPERSON_NUM    ) as ENTPERSON_NUM    ,
                     sum(UNENTPERSON_NUM  ) as UNENTPERSON_NUM  ,
                     sum(ENTPERSON_NEWNUM ) as ENTPERSON_NEWNUM ,
                     sum(ENTPERSON_LOSTNUM) as ENTPERSON_LOSTNUM,
                     sum(USER_NUM         ) as USER_NUM         ,
                     sum(USER_NUMLJ       ) as USER_NUMLJ       ,
                     sum(USER_NUMJF       ) as USER_NUMJF       ,
                     sum(USER_NUMJFLJ     ) as USER_NUMJFLJ     ,
                     sum(UPSMS_COUNT      ) as UPSMS_COUNT      ,
                     sum(DOWNSMS_COUNT    ) as DOWNSMS_COUNT        
                                     
                  from 
                      session.g_s_22037_month_tmp
                  group by FAB_TYPE
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
