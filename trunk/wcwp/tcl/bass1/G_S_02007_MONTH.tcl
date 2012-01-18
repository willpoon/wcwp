######################################################################################################
#接口名称：用户积分回馈情况
#接口编码：02007
#接口说明：记录用户积分兑换情况
#程序名称: G_S_02007_MONTH.tcl
#功能描述: 生成02007的数据
#运行粒度: 日
#源    表：1.bass2.Dw_product_ord_so_log_dm_yyyymm
#          2.bass2.Dw_product_sc_payment_dm_yyyymm
#          3.bass2.dwd_product_sc_scorelist_yyyymm
#          4.bass2.dw_product_$op_month
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2010-06-09
#问题记录：1.注意
#修改历史: 1.南方基地上线新脚本/新的业务抓取口径
#          2.1.7.1规范修改 liuqf 20110127
#          3.1.7.2规范修改 panzw 2011-04-24 
#          除"省公司类"中的“自有类”填报2210～2250等三级编码外，
#          其他分类只填报二级编码（1100、1200、1300、2100、2300）
#						1000	总部类	
#						1100	实物类	
#						1200	自有类	
#						1210	话费类	
#						1220	服务类	
#						1230	业务类	
#						1240	终端类	
#						1250	其他类	
#						1300	合作类	
#						2000	省公司类	
#						2100	实物类	
#						2200	自有类	
#						2210	话费类	
#						2220	服务类	
#						2230	业务类	
#						2240	终端类	
#						2250	其他类	
#						2300	合作类	
#  现有的取法已经满足要求。
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


        #本月 yyyymm
        #set op_time 2011-04-01
        #set optime_month 2011-03
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        puts $op_time
        puts $op_month

		set app_name "G_S_02007_MONTH.tcl"        


##################################################






  #删除本期数据
	set sql_buff "delete from bass1.g_s_02007_month where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "alter table bass1.g_s_02007_month_1 activate not logged initially with empty table"
	exec_sql $sql_buff
	set sql_buff "alter table bass1.g_s_02007_month_2 activate not logged initially with empty table"
	exec_sql $sql_buff
	set sql_buff "alter table bass1.g_s_02007_month_2a activate not logged initially with empty table"
	exec_sql $sql_buff
	set sql_buff "alter table bass1.g_s_02007_month_3 activate not logged initially with empty table"
	exec_sql $sql_buff
	set sql_buff "alter table bass1.g_s_02007_month_4 activate not logged initially with empty table"
	exec_sql $sql_buff


##~   取得用户兑换值

	set sql_buff "
	insert into G_S_02007_MONTH_1
			select user_id
				,sum(USRSCR) MONTH_CONVERTED_POINTS 
				,count(distinct sc_payment_id)  MONTH_CONVERTED_CNT
			from bass2.dwd_product_sc_payout_$op_month
			group by user_id
            with ur
	
	"
	exec_sql $sql_buff
	
	
	##~   取得总部兑换
	
	set sql_buff "

insert into G_S_02007_MONTH_2
select 
a.user_id
,a.SC_PAYMENT_ID
,1 lvl
,a.peer_seq
,a.AMOUNT
from  bass2.dw_product_sc_payment_dm_$op_month a
where exists 
 (select 1 from  bass2.ODS_SC_SCRD_ORD_INFO_$op_month  b where op_time like '$op_month%'  and a.peer_seq = b.ORD_SEQ and  a.PRODUCT_NO = b.MOB_NUM )
 and exists 
 (select 1 from  bass2.dwd_product_sc_payout_$op_month  c where a.SC_PAYMENT_ID = c.SC_PAYMENT_ID and a.user_id = c.USER_ID)
			 with ur
	"
	exec_sql $sql_buff
	
	
	
	##~   取得本地兑换
	
	set sql_buff "
insert into G_S_02007_MONTH_2
select 
a.user_id
,a.SC_PAYMENT_ID
,2 lvl
,a.peer_seq
,a.AMOUNT
from  bass2.dw_product_sc_payment_dm_$op_month a
where not exists 
 (select 1 from  G_S_02007_MONTH_2 b where    a.peer_seq = b.peer_seq and  a.user_id = b.user_id )
 and exists 
 (select 1 from  bass2.dwd_product_sc_payout_$op_month  c where a.SC_PAYMENT_ID = c.SC_PAYMENT_ID and a.user_id = c.USER_ID)
 with ur
			
	"
	exec_sql $sql_buff
	
	##~   按用户去重
	
 	set sql_buff "
insert into G_S_02007_MONTH_2a
select 
         USER_ID
        ,SC_PAYMENT_ID
        ,LVL
        ,value(PEER_SEQ,'0')
        ,AMOUNT
        from (
                select a.*,row_number()over(partition by user_id order by lvl asc ,value(PEER_SEQ,'0') desc , amount desc ) rn 
                from G_S_02007_MONTH_2 a
        ) t where rn = 1
with ur			
	"
	exec_sql $sql_buff
	
	
 ##~   总部分类去重
 	
 	set sql_buff "
insert into G_S_02007_MONTH_3
select  
	ORD_SEQ 
	,ORDER_SUM_POINT
	,type1 
	,ITEM_TYPE
from 
( select a.* , row_number()over(partition by ORD_SEQ order by ORDER_SUM_POINT desc  ) rn 
 from (select ORD_SEQ,ITEM_TYPE,type1,sum(ORDER_SUM_POINT) ORDER_SUM_POINT  from bass2.ODS_SC_SCRD_ORD_INFO_$op_month
  where op_time like '$op_month%' 
  group by  ORD_SEQ,ITEM_TYPE,type1
 )  a
,G_S_02007_MONTH_2a b 
where  a.ORD_SEQ = b.peer_seq
) t where t.rn = 1
with ur
			
	"
	exec_sql $sql_buff
	
	
	
		
 ##~   本地分类去重
 	
 	set sql_buff "

insert into G_S_02007_MONTH_4
select 
ord_code
,max(offer_name) offer_name
    from bass2.dw_product_ord_so_log_dm_$op_month a,G_S_02007_MONTH_2a b
    where a.ORD_CODE = b.peer_seq
    and (offer_name like '%积分%' or offer_name like '%M值%')
    group by ord_code 
with ur
			
	"
	exec_sql $sql_buff
	
	
	
	
			
 ##~   本地分类去重
 	
 	set sql_buff "
insert into G_S_02007_MONTH_4
select 
ord_code
,max(offer_name) offer_name
    from bass2.dw_product_ord_so_log_dm_$op_month a,G_S_02007_MONTH_2a b
    where a.ORD_CODE = b.peer_seq
    and  not exists (select 1 from G_S_02007_MONTH_4  c where a.ORD_CODE = c.ord_code)
    and offer_name is not null
    group by ord_code 
with ur
			
	"
	exec_sql $sql_buff
	


 	set sql_buff "
	insert into g_s_02007_month
			select 	
			$op_month
			,value(feedback_id ,
			  case when (d.offer_name like '%加油%' or d.offer_name like '%购物%') then '2100'
			   when (d.offer_name like '%充值卡%' or d.offer_name like '%送%话费%') then '2210'
			   when (d.offer_name like '%GPRS套餐%' or d.offer_name like '%手机报%' or d.offer_name like '%短信套餐%' or 
					 d.offer_name like '%送彩铃%'   or d.offer_name like '%彩信套餐%' or d.offer_name like '%无线音乐俱乐部%') then '2230'
			   when (d.offer_name like '%VIP客户%') then '2220'
			   when (d.offer_name like '%送手机%') or (d.offer_name like '%兑手机%') then '2240'
			   when (d.offer_name like '%电影票%') or (d.offer_name like '%演唱会门票%') or (d.offer_name like '%新专辑%') then '2100'
			   else '2250'
			end ) feedback_id
			,a.USER_ID
			,char(a.MONTH_CONVERTED_POINTS)
			,char(a.MONTH_CONVERTED_CNT)
		from G_S_02007_MONTH_1 a
		left join  G_S_02007_MONTH_2a b on a.user_id = b.user_id  
		left join (select ORD_SEQ,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id 
						from 
						(
						select
										ORD_SEQ,ITEM_TYPE
										,case 
											when type1 = '实物类' then '00'
											when type1 = '自有类' then '01'
											when type1 = '合作类' then '02'
											else type1 end type1 
							 from G_S_02007_MONTH_3 a
						) t
						) c on  b.PEER_SEQ = c.ORD_SEQ
		left join G_S_02007_MONTH_4 d on  b.PEER_SEQ = d.ORD_CODE
with ur
	"

exec_sql $sql_buff

  ##~   #建立中间临时表1
	##~   set sql_buff "
	##~   declare global temporary table session.g_i_02007_month_tmp_1
	##~   (   
	  ##~   feedback_id        varchar(4),
		##~   user_id            varchar(20),
		##~   used_point         bigint,
		##~   t_total_point      bigint,
		##~   tone_total_point   bigint,
		##~   ttwo_total_point   bigint,
		##~   tthree_used_point  bigint,
		##~   feedback_cnt       bigint
	##~   )
	##~   partitioning key (user_id) using hashing
	##~   with replace on commit preserve rows not logged in tbs_user_temp"
	##~   puts $sql_buff
	##~   exec_sql $sql_buff

	##~   #促销类型
	##~   ##2000 省公司类
	##~   ##2100 实物类
	##~   ##2200 自有类
	##~   ##2210 话费类
	##~   ##2220 服务类
	##~   ##2230 业务类
	##~   ##2240 终端类
	##~   ##2250 其他类
	##~   ##2300 合作类
    
  ##~   #提取积分回馈清单
	##~   set sql_buff "
	##~   insert into session.g_i_02007_month_tmp_1
	  ##~   (
        ##~   feedback_id
		##~   ,user_id
		##~   ,used_point
		##~   ,feedback_cnt
	  ##~   )
	##~   select 
	  ##~   case when (a.offer_name like '%加油%' or a.offer_name like '%购物%') then '2100'
	       ##~   when (a.offer_name like '%充值卡%' or a.offer_name like '%送%话费%') then '2210'
	       ##~   when (a.offer_name like '%GPRS套餐%' or a.offer_name like '%手机报%' or a.offer_name like '%短信套餐%' or 
	             ##~   a.offer_name like '%送彩铃%'   or a.offer_name like '%彩信套餐%' or a.offer_name like '%无线音乐俱乐部%') then '2230'
	       ##~   when (a.offer_name like '%VIP客户%') then '2220'
	       ##~   when (a.offer_name like '%送手机%') or (a.offer_name like '%兑手机%') then '2240'
	       ##~   when (a.offer_name like '%电影票%') or (a.offer_name like '%演唱会门票%') or (a.offer_name like '%新专辑%') then '2100'
	       ##~   else '2250'
	  ##~   end feedback_id,
	  ##~   b.user_id,
	  ##~   sum(b.amount),
	  ##~   sum(a.cnt)
	##~   from (select user_id,ord_code,max(offer_name) offer_name,count(*) cnt from  bass2.dw_product_ord_so_log_dm_$op_month 
	         ##~   group by user_id,ord_code ) a ,
	     ##~   bass2.dw_product_sc_payment_dm_$op_month b
	##~   where a.ord_code=b.peer_seq
	  ##~   and b.operation_type in ('3','8')
	##~   group by 
	  ##~   case when (a.offer_name like '%加油%' or a.offer_name like '%购物%') then '2100'
	       ##~   when (a.offer_name like '%充值卡%' or a.offer_name like '%送%话费%') then '2210'
	       ##~   when (a.offer_name like '%GPRS套餐%' or a.offer_name like '%手机报%' or a.offer_name like '%短信套餐%' or 
	             ##~   a.offer_name like '%送彩铃%'   or a.offer_name like '%彩信套餐%' or a.offer_name like '%无线音乐俱乐部%') then '2230'
	       ##~   when (a.offer_name like '%VIP客户%') then '2220'
	       ##~   when (a.offer_name like '%送手机%') or (a.offer_name like '%兑手机%') then '2240'
	       ##~   when (a.offer_name like '%电影票%') or (a.offer_name like '%演唱会门票%') or (a.offer_name like '%新专辑%') then '2100'
	       ##~   else '2250'
	  ##~   end,
	 ##~   b.user_id
	##~   "

 	##~   puts $sql_buff
	##~   exec_sql $sql_buff

##~   ##增加总部类的

    
  ##~   #提取积分回馈清单
	##~   set sql_buff "
	##~   insert into session.g_i_02007_month_tmp_1
	  ##~   (
     ##~   feedback_id
		##~   ,user_id
		##~   ,used_point
		##~   ,feedback_cnt
	  ##~   )
	
##~   select 
	  ##~   a.feedback_id,
	  ##~   b.user_id,
	  ##~   sum(a.used_point),
	  ##~   count(distinct mob_num||feedback_id) feedback_cnt
	##~   from (                   
					##~   select mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id
					##~   ,sum(ORDER_SUM_POINT)  used_point
						##~   from 
						##~   (
						##~   select
										 ##~   mob_num 
										##~   ,ITEM_TYPE
										##~   ,case 
											##~   when type1 = '实物类' then '00'
											##~   when type1 = '自有类' then '01'
											##~   when type1 = '合作类' then '02'
											##~   else type1 end type1 
										##~   ,sum(value(ORDER_SUM_POINT,0)) ORDER_SUM_POINT
							 ##~   from bass2.ODS_SC_SCRD_ORD_INFO_${op_month} a
							 			##~   where substr(OP_TIME,1,6) = '$op_month'
							 			  ##~   group by 
										 ##~   mob_num 
										##~   ,ITEM_TYPE
										##~   ,case 
											##~   when type1 = '实物类' then '00'
											##~   when type1 = '自有类' then '01'
											##~   when type1 = '合作类' then '02'
											##~   else type1 end 
										##~   having sum(value(ORDER_SUM_POINT,0)) > 0
						##~   ) t
						##~   group by                    
						##~   mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00'
						##~   ) a, 
	 ##~   bass2.dw_product_$op_month b
	 ##~   where a.mob_num = b.product_no
	    ##~   and b.userstatus_id in (1,2,3,6,8) 
		  ##~   and b.usertype_id in (1,2,9) 
		  ##~   and b.test_mark=0
##~   group by 	  a.feedback_id,
	  ##~   b.user_id
	##~   "

	##~   exec_sql $sql_buff



    #为了无缝割接历史数据，所以对老数据进行特殊处理
    #
    #
###	set sql_buff "
###	insert into session.g_i_02007_month_tmp_1
###	  (
###        feedback_id
###		,user_id
###		,used_point
###		,feedback_cnt
###	  )
###		select
###		  case when (b.cond_name like '%加油%' or b.cond_name like '%购物%') then '2100'
###		       when (b.cond_name like '%充值卡%' or b.cond_name like '%送%话费%') then '2210'
###		       when (b.cond_name like '%GPRS套餐%' or b.cond_name like '%手机报%' or b.cond_name like '%短信套餐%' or 
###		             b.cond_name like '%送彩铃%'   or b.cond_name like '%彩信套餐%' or b.cond_name like '%无线音乐俱乐部%') then '2230'
###		       when (b.cond_name like '%VIP客户%') then '2220'
###		       when (b.cond_name like '%送手机%') or (b.cond_name like '%兑手机%') then '2240'
###		       when (b.cond_name like '%电影票%') or (b.cond_name like '%演唱会门票%') or (b.cond_name like '%新专辑%') then '2100'
###		       else '2250'
###		  end ,
###		  coalesce(c.new_user_id,a.user_id) as user_id,
###		  sum(a.value),
###		  count(*)
###		from BASS2.DWD_CM_BUSI_COIN_20100625 a
###		left outer join 
###		  (select distinct so_nbr,user_id,cond_name from  BASS2.DW_PRODUCT_BUSI_PROMO_201006) b
###		on a.so_nbr = b.so_nbr
###		left join bass2.trans_user_id_20100625 c on a.user_id=c.user_id
###		where a.charge_date > '2010-06-01 00:00:00' and a.charge_date < '2010-07-01 00:00:00'
###		group by                       
###		 case when (b.cond_name like '%加油%' or b.cond_name like '%购物%') then '2100'
###		       when (b.cond_name like '%充值卡%' or b.cond_name like '%送%话费%') then '2210'
###		       when (b.cond_name like '%GPRS套餐%' or b.cond_name like '%手机报%' or b.cond_name like '%短信套餐%' or 
###		             b.cond_name like '%送彩铃%'   or b.cond_name like '%彩信套餐%' or b.cond_name like '%无线音乐俱乐部%') then '2230'
###		       when (b.cond_name like '%VIP客户%') then '2220'
###		       when (b.cond_name like '%送手机%') or (b.cond_name like '%兑手机%') then '2240'
###		       when (b.cond_name like '%电影票%') or (b.cond_name like '%演唱会门票%') or (b.cond_name like '%新专辑%') then '2100'
###		       else '2250'
###		  end ,
###		  coalesce(c.new_user_id,a.user_id)
###		having sum(a.value) > 0
###	"
###
### 	puts $sql_buff
###	exec_sql $sql_buff
###





  ##~   #建立中间临时表2
	##~   set sql_buff "
	##~   declare global temporary table session.g_i_02007_month_tmp_2
	##~   (   
		##~   user_id            varchar(20),
		##~   t_total_point      bigint,
		##~   tone_total_point   bigint,
		##~   ttwo_total_point   bigint,
		##~   tthree_used_point  bigint
	##~   )
	##~   partitioning key (user_id) using hashing
	##~   with replace on commit preserve rows not logged in tbs_user_temp"
	##~   puts $sql_buff
	##~   exec_sql $sql_buff

    
  ##~   #计算当月所有用户已兑换的积分
	##~   set sql_buff "
	##~   insert into session.g_i_02007_month_tmp_2
	  ##~   (
     ##~   user_id
		##~   ,t_total_point
		##~   ,tone_total_point
		##~   ,ttwo_total_point
		##~   ,tthree_used_point
	  ##~   )
	##~   select 
	   ##~   a.product_instance_id user_id
	   ##~   ,sum(case when a.year=$syear then b.usrscr else 0 end)       t_total_point
	   ##~   ,sum(case when a.year=$oneyear then b.usrscr else 0 end)     tone_total_point
	   ##~   ,sum(case when a.year=$twoyear then b.usrscr else 0 end)     ttwo_total_point
	   ##~   ,sum(case when a.year<=$threeyear then b.usrscr else 0 end)  tthree_used_point
	 ##~   from bass2.dwd_product_sc_scorelist_$op_month a,bass2.dwd_product_sc_payout_$op_month b
    ##~   where a.sc_scorelist_id=b.sc_scorelist_id
	  ##~   and a.actflag='1'
   ##~   group by a.product_instance_id
   ##~   "

 	##~   puts $sql_buff
	##~   exec_sql $sql_buff


  ##~   #关联在网用户，插入结果表
	##~   set sql_buff "
	##~   insert into bass1.G_S_02007_month
	##~   (
		##~   time_id
		##~   ,point_feedback_id
		##~   ,user_id
		##~   ,used_point
		##~   ,t_used_point
		##~   ,tone_used_point
		##~   ,ttwo_used_point
		##~   ,tthree_used_point
		##~   ,Used_Count
	##~   )
	##~   select
		 ##~   $op_month
		##~   ,a.feedback_id
		##~   ,a.user_id
		##~   ,char(coalesce(a.used_point,0))
		##~   ,char(coalesce(b.t_total_point,0))
		##~   ,char(coalesce(b.tone_total_point,0))
		##~   ,char(coalesce(b.ttwo_total_point,0))
		##~   ,char(coalesce(b.tthree_used_point,0))
		##~   ,char(coalesce(a.feedback_cnt,0))
	##~   from
	   ##~   (
		##~   select
		  ##~   a.feedback_id
		  ##~   ,a.user_id
		  ##~   ,a.used_point
		  ##~   ,a.feedback_cnt
		##~   from 
			##~   session.g_i_02007_month_tmp_1 a,
			##~   bass2.dw_product_$op_month b 
		##~   where a.user_id = b.user_id 
		  ##~   and b.userstatus_id in (1,2,3,6,8) 
		  ##~   and b.usertype_id in (1,2,9) 
		  ##~   and b.test_mark=0
		##~   ) a
	##~   left join session.g_i_02007_month_tmp_2 b on a.user_id = b.user_id
  ##~   "

	##~   exec_sql $sql_buff
	
	#增加校验：分类ID需在指定范围内
	
	set sql_buff "
		select count(0) 
		from    G_S_02007_MONTH
		where 
		time_id = $op_month
		and POINT_FEEDBACK_ID 	not in ('2210','2220','2230','2240','2250'
								,'1100','1200','1300','2100','2100')
	"
	set RESULT_VAL [get_single $sql_buff]
	puts $RESULT_VAL
		if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent "02007有非法分类编码!"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }


	return 0
}


###########参考############
#and b.userstatus_id in (1,2,3,6,8)  --在网用户--
#userstatus_id in (4,5,7,9) --离网用户
#and b.usertype_id in (1,2,9) --非虚拟用户--
#and (b.crm_brand_id1=1 or b.crm_brand_id1=4) --全球通和动感地带用户--
#and a.sts=1 --正常记录--
#############################################################
