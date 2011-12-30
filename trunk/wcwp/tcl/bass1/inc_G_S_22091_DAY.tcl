######################################################################################################
#接口名称：实体渠道业务办理日汇总
#接口编码：22091
#接口说明：记录实体渠道业务办理日汇总信息，涉及自营厅、委托经营厅、24小时自助营业厅或社会代理网点
#程序名称: G_S_22091_DAY.tcl
#功能描述: 生成22091的数据
#运行粒度: 月
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-06-08
#问题记录：
#修改历史: ref:22062
#######################################################################################################


proc Deal_imp { op_time optime_month } {

   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    #当天 yyyy-mm-dd
    set optime $op_time
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]


	set sql_buf "ALTER TABLE BASS1.G_S_22091_DAY_P1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf




    #1、通过营业日志抓取中心业务SP的各类增值业务
    #往临时表插入本月数据
    set sql_buff "
    insert into BASS1.G_S_22091_DAY_P1
				  (
				   channel_id
					,accept_type
					,imp_accepttype
					,cnt)
		select b.org_id
           ,case when b.so_mode='5' then 2 else 1 end accept_type
		       ,case when b.busi_type in ('115','730') then '06'
		             when b.busi_type='117' then '12'
		             when b.busi_type='118' then '25'
		             when b.busi_type='126' then '10'
		             when b.busi_type='132' then '11'
		             when b.busi_type='137' then '19'
		             when b.busi_type='139' then '16'
		             when b.busi_type='708' then '26'
		             when b.busi_type='709' then '04'
		             when b.busi_type='716' then '27'
		             when b.busi_type='733' then '22'
		             when b.busi_type='726' then '20'
		             when b.busi_type='140' then '28'
		        end imp_accepttype
		       ,count(distinct b.user_id||char(b.offer_id)) 
		from bass2.Dim_channel_info a,
		(
			select c.org_id,b.busi_type,b.so_nbr,b.so_mode ,c.USER_ID,c.OFFER_ID
			  from bass2.Dw_cm_busi_radius_dm_$curr_month b,
			       bass2.dw_product_ord_so_log_dm_$curr_month c 
			 where b.ext_holds2=c.so_log_id
			 and c.CREATE_DATE = '$op_time'
		 ) b
		where a.channel_id = b.org_id
		  and a.channel_type_class in (90105,90102)
		  and b.busi_type in ('115','117','118','126','132','137','139','708','709','716','730','733','726','140')
		group by  b.org_id,
		          case when b.so_mode='5' then 2 else 1 end,
	            case when b.busi_type in ('115','730') then '06'
			             when b.busi_type='117' then '12'
			             when b.busi_type='118' then '25'
			             when b.busi_type='126' then '10'
			             when b.busi_type='132' then '11'
			             when b.busi_type='137' then '19'
			             when b.busi_type='139' then '16'
			             when b.busi_type='708' then '26'
			             when b.busi_type='709' then '04'
			             when b.busi_type='716' then '27'
			             when b.busi_type='733' then '22'
			             when b.busi_type='726' then '20'
			             when b.busi_type='140' then '28'
			        end		          
	"
    exec_sql $sql_buff



    #2、通过订单来抓取个人、集团等增值受理业务
    #往临时表插入本月数据
    set sql_buff "
    insert into BASS1.G_S_22091_DAY_P1
				  (
				   channel_id
					,accept_type
					,imp_accepttype
					,cnt)
			select
			 a.org_id,
			 case when a.channel_type='9' then 2 else 1 end accept_type,
			 case when b.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230) then '08'
			      when b.offer_id in (113090000003) then '03'
			      when b.offer_id in (113090003319,113090003320,113090003321,111090033319) then '07'
			      when b.offer_id in (111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631) then '01'
			      when b.offer_id in (111090002901) then '02'
			      when b.offer_id in (113110166406) then '05'
			      when b.offer_id in (113110203699) then '24'
			      when b.offer_id in (111090003215,113110180134) then '09'
			      when b.offer_id in (111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002) then '13'
			      when b.offer_id in (111000000705) then '14'
			      when b.offer_id in (113110175125,113110195329) then '17'
			      when b.offer_id in (111098000044,113110180139,113110197954) then '18'
			      when b.offer_id in (113110156821,113110166390,113110168281,113110178283,113110208633,113110208635) then '21'
			      when b.offer_id in (111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290) then '23'
					  when b.offer_id in (113110168208,113110168210,113110168209,113110280023,113110211974,113110280047,113110280049,113110211980,113110168211,113110211982,113110211969,113110280044,113110211973,113110211976,113110211983,113110220029,113110280046,113110211970,113110211979,113110211981,113110211971,113110220027,113110211975,113110211977,113110280057,113110211978,113110280013,113110280015,113110280065,113110280068,113110280000,113110280026,113110203850,113110280037,113110280007,113110280009,113110280020,111090009494,113110280039,113110280062,113110220028,111090001371,113110211972) then '29'
			 end imp_accepttype,
       count(distinct value(a.PRODUCT_INSTANCE_ID,'0') || value(char(b.OFFER_ID),'0') )
			  from bass2.dw_product_ord_cust_dm_$curr_month a,
			       bass2.dw_product_ord_offer_dm_$curr_month b,
			       bass2.dim_prod_up_product_item d,
			       bass2.dim_pub_channel e,
			       bass2.dim_sys_org_role_type f,
			       bass2.dim_cfg_static_data g,
			       bass2.Dim_channel_info h
			 where a.customer_order_id = b.customer_order_id
			   and b.offer_id = d.product_item_id
			   and  date(b.CREATE_DATE) = '$op_time'
			   and a.org_id = e.channel_id
			   and a.channel_type = g.code_value
			   and g.code_type = '911000'
			   and b.offer_type=2
			   and e.channeltype_id = f.org_role_type_id
			   and a.org_id = h.channel_id
			   and h.channel_type_class in (90105,90102)
			   and b.offer_id in 
			   (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230,
					113090000003,
					113090003319,113090003320,113090003321,111090033319,
					111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631,
					111090002901,
					113110166406,
					113110203699,
					111090003215,113110180134,
					111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002,
					111000000705,
					113110175125,113110195329,
					111098000044,113110180139,113110197954,
					113110156821,113110166390,113110168281,113110178283,113110208633,113110208635,
					111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290,
					113110168208,113110168210,113110168209,113110280023,113110211974,113110280047,113110280049,113110211980,113110168211,113110211982,113110211969,113110280044,113110211973,113110211976,113110211983,113110220029,113110280046,113110211970,113110211979,113110211981,113110211971,113110220027,113110211975,113110211977,113110280057,113110211978,113110280013,113110280015,113110280065,113110280068,113110280000,113110280026,113110203850,113110280037,113110280007,113110280009,113110280020,111090009494,113110280039,113110280062,113110220028,111090001371,113110211972
					)
			   and a.channel_type in ('b','9','5')
			   and b.customer_order_id not in 
			   (select b.ORD_CUST_ID
			   from bass2.Dim_channel_info a,
						(
							select c.org_id,b.busi_type,b.so_nbr,b.so_mode ,c.ORD_CUST_ID
							  from bass2.Dw_cm_busi_radius_dm_$curr_month b,
							       bass2.dw_product_ord_so_log_dm_$curr_month c 
							 where b.ext_holds2=c.so_log_id
							 and c.CREATE_DATE = '$op_time'
						 ) b
							where a.channel_id = b.org_id
							  and a.channel_type_class in (90105,90102)
							  and b.busi_type in ('115','117','118','126','132','137','139','708','709','716','730','733','726','140')
				 )
			 group by  a.org_id,
			         case when a.channel_type='9' then 2 else 1 end,
							 case when b.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230) then '08'
							      when b.offer_id in (113090000003) then '03'
							      when b.offer_id in (113090003319,113090003320,113090003321,111090033319) then '07'
							      when b.offer_id in (111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631) then '01'
							      when b.offer_id in (111090002901) then '02'
							      when b.offer_id in (113110166406) then '05'
							      when b.offer_id in (113110203699) then '24'
							      when b.offer_id in (111090003215,113110180134) then '09'
							      when b.offer_id in (111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002) then '13'
							      when b.offer_id in (111000000705) then '14'
							      when b.offer_id in (113110175125,113110195329) then '17'
							      when b.offer_id in (111098000044,113110180139,113110197954) then '18'
							      when b.offer_id in (113110156821,113110166390,113110168281,113110178283,113110208633,113110208635) then '21'
							      when b.offer_id in (111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290) then '23'
							      when b.offer_id in (113110168208,113110168210,113110168209,113110280023,113110211974,113110280047,113110280049,113110211980,113110168211,113110211982,113110211969,113110280044,113110211973,113110211976,113110211983,113110220029,113110280046,113110211970,113110211979,113110211981,113110211971,113110220027,113110211975,113110211977,113110280057,113110211978,113110280013,113110280015,113110280065,113110280068,113110280000,113110280026,113110203850,113110280037,113110280007,113110280009,113110280020,111090009494,113110280039,113110280062,113110220028,111090001371,113110211972) then '29'
							 end 
	"
    exec_sql $sql_buff


	set sql_buf "ALTER TABLE BASS1.G_S_22091_DAY_P2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

    #插入目标表
    set sql_buff "
    insert into BASS1.G_S_22091_DAY_P2
				  (
				   time_id
				  ,channel_id
					,accept_type
					,imp_accepttype
					,cnt)
		select $timestamp
		      ,trim(char(channel_id))
					,trim(char(accept_type))
					,imp_accepttype
					,sum(cnt)
			from BASS1.G_S_22091_DAY_P1
	group by trim(char(channel_id)),
	         trim(char(accept_type)),
	         imp_accepttype
	"
    exec_sql $sql_buff






	return 0
}

