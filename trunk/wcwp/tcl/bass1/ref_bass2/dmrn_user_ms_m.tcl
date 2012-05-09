#***********************************************************************************
# 名称：dmrn_user_ms_m.tcl
# 功能描述：生成重入网用户详单表
# 表名：DMRN_USER_MS
# 编写人：zhb,liyunlei
# yyyymmdd 20040901 程序运行时间
#***********************************************************************************

source /bassapp/bass2/tcl/base.tcl

proc deal {p_optime p_timestamp} {

 global conn
 global handle
 
	set yyyymm1 [get_first_month $p_optime]
	set yyyymm2 [get_second_month $p_optime]
	set yyyymm3 [get_third_month $p_optime]
	
		
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1000
		return -1
	}
	
	if {[get_user_ms $yyyymm1 $yyyymm2 $yyyymm3]  != 0} {
		aidb_roll $conn
		aidb_close $handle
		return -1
	}
	
	aidb_commit $conn
	aidb_close $handle
 
 return 0
}
proc get_user_ms {yyyymm1 yyyymm2 yyyymm3} {

	global conn
	global handle
	
	set dw_table_space [get_table_space]
	set dw_index_space [get_index_space]
	set debug [get_debug]
	set city_list [get_city_list]
	
	set yyyy2 [string range $yyyymm2 0 3] 
	set mm2 [string trimleft [string range $yyyymm2 4 5] "0"]
 
 
	if { $mm2 <= 2 } {
		set mm_more [expr 10+$mm2]
		set yyyy_more [expr $yyyy2-1]
	} else {
		set mm_more [expr $mm2-2]
		set yyyy_more $yyyy2
	}
	set yyyymm_more [format "%04d%02d" $yyyy_more $mm_more]
 
	if { $mm2 <= 3 } {
		set mm_most [expr 9+$mm2]
		set yyyy_most [expr $yyyy2-1]
	} else {
		set mm_most [expr $mm2-3]
		set yyyy_most $yyyy2
	}
	set yyyymm_most [format "%04d%02d" $yyyy_most $mm_most]
	set yyyymmdd2 [format "%04d-%02d-%02d" $yyyy2 $mm2 1]

#	if { $mm2 <= 6 } {
#		set mm8 [expr 6+$mm2]
#		set yyyy8 [expr $yyyy2-1]
#	} else {
#		set mm8 [expr $mm2-6]
#		set yyyy8 $yyyy2
#	}
#	set yyyymm8 [format "%04d%02d" $yyyy8 $mm8]
#	set yyyymmdd8 [format "%04d-%02d-%02d" $yyyy8 $mm8 1]
 
 
	foreach city_id "$city_list" {

		if { [create_table "DMRN_USER_$city_id\_$yyyymm2" "dmrn_user_ms" "$dw_table_space" "$dw_index_space" "rn_user_id"] < 0 } {
			if { $debug } {puts "create table DMRN_USER_$city_id\_$yyyymm2 error!"}
			return -1
		}
		aidb_commit $conn 
		if { [create_table "tmp_dmrn_user_$city_id\_$yyyymm2\_ms_first" "dmrn_user_ms" "$dw_table_space" "$dw_index_space" "rn_user_id"] < 0 } {
			if { $debug } {puts "create table tmp_dmrn_user_$city_id\_$yyyymm2\_ms_first error!"}
			return -1
		}
		
		if { [create_table "tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second" "dmrn_user_ms" "$dw_table_space" "$dw_index_space" "rn_user_id"] < 0 } {
			if { $debug } {puts "create table tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second error!"}
			return -1
		}
		 
		 
		set sql "   \
			INSERT INTO tmp_dmrn_user_$city_id\_$yyyymm2\_ms_first \
			( rn_date                     ,   \
				rn_user_id                  ,   \
				his_user_id                 ,   \
				rn_user_number              ,   \
				rn_base_brand_id            ,   \
				rn_base_region_code         ,   \
				rn_base_county_code         ,   \
				rn_base_plan_id             ,   \
				rn_base_plan_pay_mode       ,   \
				rn_create_date              ,   \
				rn_base_should_fee          ,   \
				rn_base_fav_fee             ,   \
				rn_next_should_fee          ,   \
				rn_next_fav_fee             ,   \
				his_next_life_flag          ,   \
				rn_base_org_id              ,   \
				rn_base_dept_code           ,   \
				accord_rate                 ,   \
				change_phone_flag           ,   \
				rn_card_code                ,   \
				rn_cust_name                )   \
			SELECT  \
				[ai_to_date $yyyymmdd2] AS rn_date        ,  \
				a.new_user_id   AS RN_USER_ID             ,  \
				a.his_user_id   AS his_user_id            ,  \
				b.user_number   AS rn_user_number         ,  \
				b.brand_id      AS rn_base_brand_id       ,  \
				b.region_code   AS rn_base_region_code    ,  \
				b.county_code   AS rn_base_county_code    ,  \
				b.plan_id       AS rn_base_plan_id        ,  \
				b.plan_pay_mode AS rn_base_plan_pay_mode  ,  \
				b.create_date   AS rn_create_date         ,  \
				b.should_fee    AS rn_base_should_fee     ,  \
				b.fav_fee       AS rn_base_fav_fee        ,  \
				c.should_fee    AS rn_next_should_fee     ,  \
				c.fav_fee       AS rn_next_fav_fee        ,  \
				(CASE WHEN d.user_id IS NOT NULL THEN 1 ELSE 0 END) AS his_next_life_flag   ,  \
				b.org_id        AS rn_base_org_id         ,  \
				b.dept_code     AS rn_base_dept_code      ,  \
				a.accord_rate   AS accord_rate            ,  \
				a.change_phone_flag AS change_phone_flag  ,  \
				c.card_code     AS rn_card_code           ,  \
				c.cust_name     AS rn_cust_name              \                                                      
			FROM dmrn_identify_list_$city_id\_$yyyymm2 a   \
			LEFT OUTER JOIN dmrn_user_baseinfo_$city_id\_$yyyymm2 b ON a.new_user_id = b.user_id \        
			LEFT OUTER JOIN dmrn_user_baseinfo_$city_id\_$yyyymm3 c ON a.new_user_id = c.user_id \
			LEFT OUTER JOIN dmrn_valid_user_$city_id\_$yyyymm3 d ON a.his_user_id = d.user_id \
		"   
		
		puts $sql
		if [catch { aidb_sql $handle $sql } errmsg ] {
			trace_sql $errmsg 1101
			return -1
		} 
		aidb_commit $conn
			
		set sql "   \
			INSERT INTO tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second \
			( rn_date                     ,   \
				rn_user_id                  ,   \
				his_user_id                 ,   \
				rn_user_number              ,   \
				his_user_number             ,   \
				rn_base_brand_id            ,   \
				rn_base_region_code         ,   \
				rn_base_county_code         ,   \
				rn_base_plan_id             ,   \
				rn_base_plan_pay_mode       ,   \
				rn_create_date              ,   \
				rn_base_should_fee          ,   \
				rn_base_fav_fee             ,   \
				rn_next_should_fee          ,   \
				rn_next_fav_fee             ,   \
				his_before_brand_id         ,   \
				his_before_online           ,   \
				his_before_plan_id          ,   \
				his_before_plan_pay_mode    ,   \
				his_create_date             ,   \
				his_base_should_fee         ,   \
				his_base_fav_fee            ,   \
				his_before_should_fee       ,   \
				his_before_fav_fee          ,   \
				his_base_unpay_fee          ,   \
				his_base_avg_payed_count    ,   \
				his_base_avg_payed_money    ,   \
				his_base_unpay_duration     ,   \
				his_next_life_flag          ,   \
				rn_base_org_id              ,   \
				his_before_org_id           ,   \
				rn_base_dept_code           ,   \
				accord_rate                 ,   \
				change_phone_flag           ,   \
				flea_flag                   ,   \
				rn_card_code                ,   \
				rn_cust_name                ,   \
				his_card_code               ,   \
				his_cust_name               )   \
			select  \
				[ai_to_date $yyyymmdd2]   AS rn_date                 ,  \
				a.rn_user_id              AS rn_user_id              ,  \
				a.his_user_id             AS his_user_id             ,  \
				a.rn_user_number          AS rn_user_number          ,  \
				d.user_number             AS his_user_number         ,  \
				a.rn_base_brand_id        AS rn_base_brand_id        ,  \
				a.rn_base_region_code     AS rn_base_region_code     ,  \
				a.rn_base_county_code     AS rn_base_county_code     ,  \
				a.rn_base_plan_id         AS rn_base_plan_id         ,  \
				a.rn_base_plan_pay_mode   AS rn_base_plan_pay_mode   ,  \
				a.rn_create_date          AS rn_create_date          ,  \
				a.rn_base_should_fee      AS rn_base_should_fee      ,  \
				a.rn_base_fav_fee         AS rn_base_fav_fee         ,  \
				a.rn_next_should_fee      AS rn_next_should_fee      ,  \
				a.rn_next_fav_fee         AS rn_next_fav_fee         ,  \
				d.brand_id                AS his_before_brand_id     ,  \
				d.online_duration         AS his_before_online       ,  \
				d.plan_id                 AS his_before_plan_id      ,  \
				d.plan_pay_mode           AS his_before_plan_pay_mode,  \
				d.create_date             AS his_create_date         ,  \
				[ai_nvl "e.should_fee,0"] AS his_base_should_fee     ,  \
				[ai_nvl "e.fav_fee   ,0"] AS his_base_fav_fee        ,  \
				d.should_fee              AS his_before_should_fee   ,  \
				d.fav_fee                 AS his_before_fav_fee      ,  \
				[ai_nvl "e.unpay_fee      ,0"] AS his_base_unpay_fee      ,  \
				[ai_nvl "e.avg_payed_count,0"] AS his_base_avg_payed_count,  \
				[ai_nvl "e.avg_payed_money,0"] AS his_base_avg_payed_money,  \
				[ai_nvl "e.unpay_duration,0"]  AS his_base_unpay_duration ,  \
				a.his_next_life_flag      AS his_next_life_flag      ,  \
				a.rn_base_org_id          AS rn_base_org_id          ,  \ 
				d.org_id                  AS his_before_org_id       ,  \
				a.rn_base_dept_code       AS rn_base_dept_code       ,  \ 
				a.accord_rate             AS accord_rate             ,  \
				a.change_phone_flag       AS change_phone_flag       ,  \
				CASE WHEN f.new_user_id is not null THEN 1 ELSE 0 END AS flea_flag,  \
				a.rn_card_code            AS rn_card_code            ,  \
				a.rn_cust_name            AS rn_cust_name            ,  \
				d.card_code               AS his_card_code           ,  \
				d.cust_name               AS his_cust_name              \
			FROM tmp_dmrn_user_$city_id\_$yyyymm2\_ms_first a   \
			LEFT OUTER JOIN dmrn_user_baseinfo_$city_id\_$yyyymm1 d ON a.His_user_id = d.user_id \        
			LEFT OUTER JOIN dmrn_user_baseinfo_$city_id\_$yyyymm2 e ON a.His_user_id = e.user_id \
			LEFT OUTER JOIN dmrn_flea_list_$city_id\_$yyyymm2 f ON a.rn_user_id = f.new_user_id \
		"   
#				(CASE WHEN d.create_date >= [ai_to_date $yyyymmdd8] THEN 1 ELSE 0 END) AS flea_flag,  \
#
		if [catch { aidb_sql $handle $sql } errmsg ] {
			trace_sql $errmsg 1102
			return -1
		}     
		aidb_commit $conn    

		set sql " \
			DELETE FROM tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second \
			WHERE rn_user_number IS NULL OR his_user_number IS NULL \
			OR rn_user_id = his_user_id "  
		if [catch { aidb_sql $handle $sql } errmsg ] {
			trace_sql $errmsg 1102
			return -1
		}
		aidb_commit $conn      
		      
		if [catch {aidb_truncate $conn $handle "tmp_dmrn_user_$city_id\_$yyyymm2\_ms_first"} errmsg] {
			trace_sql $errmsg 1001
			return -1
		} 
		
##		set sql "   \
##			INSERT INTO tmp_dmrn_user_$city_id\_$yyyymm2\_ms_first \
##			( rn_date                     ,   \
##				rn_user_id                  ,   \
##				his_user_id                 ,   \
##				rn_user_number              ,   \
##				his_user_number             ,   \
##				rn_base_brand_id            ,   \
##				rn_base_region_code         ,   \
##				rn_base_county_code         ,   \
##				rn_base_plan_id             ,   \
##				rn_base_plan_pay_mode       ,   \
##				rn_create_date              ,   \
##				rn_base_should_fee          ,   \
##				rn_base_fav_fee             ,   \
##				rn_next_should_fee          ,   \
##				rn_next_fav_fee             ,   \
##				his_before_brand_id         ,   \
##				his_before_online           ,   \
##				his_before_plan_id          ,   \
##				his_before_plan_pay_mode    ,   \
##				his_create_date             ,   \
##				his_base_should_fee         ,   \
##				his_base_fav_fee            ,   \
##				his_before_should_fee       ,   \
##				his_before_fav_fee          ,   \
##				his_base_unpay_fee          ,   \
##				his_base_avg_payed_count    ,   \
##				his_base_avg_payed_money    ,   \
##				his_before2_should_fee      ,   \
##				his_before2_fav_fee         ,   \
##				his_before3_should_fee      ,   \
##				his_before3_fav_fee         ,   \
##				his_next_life_flag          ,   \
##				rn_base_org_id              ,   \
##				rn_base_dept_code           ,   \
##				accord_rate                 ,   \
##				change_phone_flag           ,   \
##				flea_flag                   ,   \
##				rn_card_code                ,   \
##				rn_cust_name                ,   \
##				his_card_code               ,   \
##				his_cust_name               ,   \
##				card_code_same_flag         ,   \
##				cust_name_same_flag         )   \
##			select  \
##				[ai_to_date $yyyymmdd2]     AS rn_date                  ,  \
##				a.rn_user_id                AS rn_user_id               ,  \
##				a.his_user_id               AS his_user_id              ,  \
##				a.rn_user_number            AS rn_user_number           ,  \
##				a.his_user_number           AS his_user_number          ,  \
##				a.rn_base_brand_id          AS rn_base_brand_id         ,  \
##				a.rn_base_region_code       AS rn_base_region_code      ,  \
##				a.rn_base_county_code       AS rn_base_county_code      ,  \
##				a.rn_base_plan_id           AS rn_base_plan_id          ,  \
##				a.rn_base_plan_pay_mode     AS rn_base_plan_pay_mode    ,  \
##				a.rn_create_date            AS rn_create_date           ,  \
##				a.rn_base_should_fee        AS rn_base_should_fee       ,  \
##				a.rn_base_fav_fee           AS rn_base_fav_fee          ,  \
##				a.rn_next_should_fee        AS rn_next_should_fee       ,  \
##				a.rn_next_fav_fee           AS rn_next_fav_fee          ,  \
##				a.his_before_brand_id       AS his_before_brand_id      ,  \
##				a.his_before_online         AS his_before_online        ,  \
##				a.his_before_plan_id        AS his_before_plan_id       ,  \
##				a.his_before_plan_pay_mode  AS his_before_plan_pay_mode ,  \
##				a.his_create_date           AS his_create_date          ,  \
##				a.his_base_should_fee       AS his_base_should_fee      ,  \
##				a.his_base_fav_fee          AS his_base_fav_fee         ,  \
##				a.his_before_should_fee     AS his_before_should_fee    ,  \
##				a.his_before_fav_fee        AS his_before_fav_fee       ,  \
##				a.his_base_unpay_fee        AS his_base_unpay_fee       ,  \
##				a.his_base_avg_payed_count  AS his_base_avg_payed_count ,  \
##				a.his_base_avg_payed_money  AS his_base_avg_payed_money ,  \
##				[ai_nvl "f.should_fee,0"]   AS his_before2_should_fee   ,  \
##				[ai_nvl "f.fav_fee   ,0"]   AS his_before2_fav_fee      ,  \
##				[ai_nvl "g.should_fee,0"]   AS his_before3_should_fee   ,  \
##				[ai_nvl "g.fav_fee   ,0"]   AS his_before3_fav_fee      ,  \
##				a.his_next_life_flag        AS his_next_life_flag       ,  \
##				a.rn_base_org_id            AS rn_base_org_id           ,  \
##				a.rn_base_dept_code         AS rn_base_dept_code        ,  \
##				a.accord_rate               AS accord_rate              ,  \
##				a.change_phone_flag         AS change_phone_flag        ,  \
##				a.flea_flag                 AS flea_flag                ,  \                                                      
##				a.rn_card_code              AS rn_card_code             ,  \
##				a.rn_cust_name              AS rn_cust_name             ,  \
##				a.his_card_code             AS his_card_code            ,  \
##				a.his_cust_name             AS his_cust_name            ,  \
##				case when a.rn_card_code = a.his_card_code then 1 else 0 end as card_code_same_flag ,  \
##				case when a.rn_cust_name = a.his_cust_name then 1 else 0 end as cust_name_same_flag    \
##			FROM tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second a   \
##			LEFT OUTER JOIN dmrn_user_baseinfo_$city_id\_$yyyymm_more f ON a.his_user_id = f.user_id \        
##			LEFT OUTER JOIN dmrn_user_baseinfo_$city_id\_$yyyymm_most g ON a.his_user_id = g.user_id \
##		"   
		set sql "   \
			INSERT INTO tmp_dmrn_user_$city_id\_$yyyymm2\_ms_first \
			( rn_date                     ,   \
				rn_user_id                  ,   \
				his_user_id                 ,   \
				rn_user_number              ,   \
				his_user_number             ,   \
				rn_base_brand_id            ,   \
				rn_base_region_code         ,   \
				rn_base_county_code         ,   \
				rn_base_plan_id             ,   \
				rn_base_plan_pay_mode       ,   \
				rn_create_date              ,   \
				rn_base_should_fee          ,   \
				rn_base_fav_fee             ,   \
				rn_next_should_fee          ,   \
				rn_next_fav_fee             ,   \
				his_before_brand_id         ,   \
				his_before_online           ,   \
				his_before_plan_id          ,   \
				his_before_plan_pay_mode    ,   \
				his_create_date             ,   \
				his_base_should_fee         ,   \
				his_base_fav_fee            ,   \
				his_before_should_fee       ,   \
				his_before_fav_fee          ,   \
				his_base_unpay_fee          ,   \
				his_base_avg_payed_count    ,   \
				his_base_avg_payed_money    ,   \
				his_base_unpay_duration     ,   \
				his_next_life_flag          ,   \
				rn_base_org_id              ,   \
				his_before_org_id           ,   \
				rn_base_dept_code           ,   \
				accord_rate                 ,   \
				change_phone_flag           ,   \
				flea_flag                   ,   \
				rn_card_code                ,   \
				rn_cust_name                ,   \
				his_card_code               ,   \
				his_cust_name               ,   \
				card_code_same_flag         ,   \
				cust_name_same_flag         )   \
			select  \
				[ai_to_date $yyyymmdd2]     AS rn_date                  ,  \
				a.rn_user_id                AS rn_user_id               ,  \
				a.his_user_id               AS his_user_id              ,  \
				a.rn_user_number            AS rn_user_number           ,  \
				a.his_user_number           AS his_user_number          ,  \
				a.rn_base_brand_id          AS rn_base_brand_id         ,  \
				a.rn_base_region_code       AS rn_base_region_code      ,  \
				a.rn_base_county_code       AS rn_base_county_code      ,  \
				a.rn_base_plan_id           AS rn_base_plan_id          ,  \
				a.rn_base_plan_pay_mode     AS rn_base_plan_pay_mode    ,  \
				a.rn_create_date            AS rn_create_date           ,  \
				a.rn_base_should_fee        AS rn_base_should_fee       ,  \
				a.rn_base_fav_fee           AS rn_base_fav_fee          ,  \
				a.rn_next_should_fee        AS rn_next_should_fee       ,  \
				a.rn_next_fav_fee           AS rn_next_fav_fee          ,  \
				a.his_before_brand_id       AS his_before_brand_id      ,  \
				a.his_before_online         AS his_before_online        ,  \
				a.his_before_plan_id        AS his_before_plan_id       ,  \
				a.his_before_plan_pay_mode  AS his_before_plan_pay_mode ,  \
				a.his_create_date           AS his_create_date          ,  \
				a.his_base_should_fee       AS his_base_should_fee      ,  \
				a.his_base_fav_fee          AS his_base_fav_fee         ,  \
				a.his_before_should_fee     AS his_before_should_fee    ,  \
				a.his_before_fav_fee        AS his_before_fav_fee       ,  \
				a.his_base_unpay_fee        AS his_base_unpay_fee       ,  \
				a.his_base_avg_payed_count  AS his_base_avg_payed_count ,  \
				a.his_base_avg_payed_money  AS his_base_avg_payed_money ,  \
				a.his_base_unpay_duration   AS his_base_unpay_duration  ,  \
				a.his_next_life_flag        AS his_next_life_flag       ,  \
				a.rn_base_org_id            AS rn_base_org_id           ,  \
				a.his_before_org_id         AS his_before_org_id        ,  \
				a.rn_base_dept_code         AS rn_base_dept_code        ,  \
				a.accord_rate               AS accord_rate              ,  \
				a.change_phone_flag         AS change_phone_flag        ,  \
				a.flea_flag                 AS flea_flag                ,  \                                                      
				a.rn_card_code              AS rn_card_code             ,  \
				a.rn_cust_name              AS rn_cust_name             ,  \
				a.his_card_code             AS his_card_code            ,  \
				a.his_cust_name             AS his_cust_name            ,  \
				case when a.rn_card_code = a.his_card_code then 1 else 0 end as card_code_same_flag ,  \
				case when a.rn_cust_name = a.his_cust_name then 1 else 0 end as cust_name_same_flag    \
			FROM tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second a   \
		"   
		if [catch { aidb_sql $handle $sql } errmsg ] {
			trace_sql $errmsg 1103
			return -1
		}  
		aidb_commit $conn    
		
		if [catch {aidb_truncate $conn $handle "tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second"} errmsg] {
			trace_sql $errmsg 1001
			return -1
		} 
		
		set sql "   \
			INSERT INTO tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second \
			( rn_date                     ,   \
				rn_user_id                  ,   \
				his_user_id                 ,   \
				rn_user_number              ,   \
				his_user_number             ,   \
				rn_base_brand_id            ,   \
				rn_base_region_code         ,   \
				rn_base_county_code         ,   \
				rn_base_plan_id             ,   \
				rn_base_plan_pay_mode       ,   \
				rn_create_date              ,   \
				rn_base_should_fee          ,   \
				rn_base_fav_fee             ,   \
				rn_next_should_fee          ,   \
				rn_next_fav_fee             ,   \
				his_before_brand_id         ,   \
				his_before_online           ,   \
				his_before_plan_id          ,   \
				his_before_plan_pay_mode    ,   \
				his_create_date             ,   \
				his_base_should_fee         ,   \
				his_base_fav_fee            ,   \
				his_before_should_fee       ,   \
				his_before_fav_fee          ,   \
				his_base_unpay_fee          ,   \
				his_base_avg_payed_count    ,   \
				his_base_avg_payed_money    ,   \
				his_base_unpay_duration     ,   \
				his_before2_should_fee      ,   \
				his_before2_fav_fee         ,   \
				his_before3_should_fee      ,   \
				his_before3_fav_fee         ,   \
				his_next_life_flag          ,   \
				his_before_consumelev       ,   \
				rn_next_consumelev          ,   \
				his_before_onlinelev        ,   \
				flea_onlinelev              ,   \
				rn_base_org_id              ,   \
				his_before_org_id           ,   \
				rn_base_dept_code           ,   \
				accord_rate                 ,   \
				change_phone_flag           ,   \
				flea_flag                   ,   \
				rn_card_code                ,   \ 
				rn_cust_name                ,   \ 
				his_card_code               ,   \ 
				his_cust_name               ,   \ 
				card_code_same_flag         ,   \ 
				cust_name_same_flag         )   \ 
			SELECT  \
				[ai_to_date $yyyymmdd2]     AS rn_date                    ,  \
				a.rn_user_id                AS rn_user_id                 ,  \
				a.his_user_id               AS his_user_id                ,  \
				a.rn_user_number            AS rn_user_number             ,  \
				a.his_user_number           AS his_user_number            ,  \
				a.rn_base_brand_id          AS rn_base_brand_id           ,  \
				a.rn_base_region_code       AS rn_base_region_code        ,  \
				a.rn_base_county_code       AS rn_base_county_code        ,  \
				a.rn_base_plan_id           AS rn_base_plan_id            ,  \
				a.rn_base_plan_pay_mode     AS rn_base_plan_pay_mode      ,  \
				a.rn_create_date            AS rn_create_date             ,  \
				a.rn_base_should_fee        AS rn_base_should_fee         ,  \
				a.rn_base_fav_fee           AS rn_base_fav_fee            ,  \
				a.rn_next_should_fee        AS rn_next_should_fee         ,  \
				a.rn_next_fav_fee           AS rn_next_fav_fee            ,  \
				a.his_before_brand_id       AS his_before_brand_id        ,  \
				a.his_before_online         AS his_before_online          ,  \
				a.his_before_plan_id        AS his_before_plan_id         ,  \
				a.his_before_plan_pay_mode  AS his_before_plan_pay_mode   ,  \
				a.his_create_date           AS his_create_date            ,  \
				a.his_base_should_fee       AS his_base_should_fee        ,  \
				a.his_base_fav_fee          AS his_base_fav_fee           ,  \
				a.his_before_should_fee     AS his_before_should_fee      ,  \
				a.his_before_fav_fee        AS his_before_fav_fee         ,  \
				a.his_base_unpay_fee        AS his_base_unpay_fee         ,  \
				a.his_base_avg_payed_count  AS his_base_avg_payed_count   ,  \
				a.his_base_avg_payed_money  AS his_base_avg_payed_money   ,  \
				a.his_base_unpay_duration   AS his_base_unpay_duration    ,  \
				a.his_before2_should_fee    AS his_before2_should_fee     ,  \
				a.his_before2_fav_fee       AS his_before2_fav_fee        ,  \
				a.his_before3_should_fee    AS his_before3_should_fee     ,  \
				a.his_before3_fav_fee       AS his_before3_fav_fee        ,  \
				a.his_next_life_flag        AS his_next_life_flag         ,  \
				CASE WHEN a.his_before_should_fee IS NULL THEN 0 ELSE b.consumelev end, \
				CASE WHEN a.rn_next_should_fee IS NULL THEN 0 ELSE c.consumelev end, \
				CASE WHEN a.his_before_online IS NULL THEN -1 ELSE d.onlinelev end, \
				CASE WHEN a.his_before_online IS NULL THEN -1 ELSE e.onlinelev end, \
				a.rn_base_org_id            AS rn_base_org_id             ,  \
				a.his_before_org_id         AS his_before_org_id          ,  \
				a.rn_base_dept_code         AS rn_base_dept_code          ,  \
				a.accord_rate               AS accord_rate                ,  \
				a.change_phone_flag         AS change_phone_flag          ,  \
				a.flea_flag                 AS flea_flag                  ,  \
				a.rn_card_code              AS rn_card_code               ,  \ 
				a.rn_cust_name              AS rn_cust_name               ,  \ 
				a.his_card_code             AS his_card_code              ,  \ 
				a.his_cust_name             AS his_cust_name              ,  \ 
				a.card_code_same_flag       AS card_code_same_flag        ,  \ 
				a.cust_name_same_flag       AS cust_name_same_flag           \ 
			FROM tmp_dmrn_user_$city_id\_$yyyymm2\_ms_first a   \
			LEFT OUTER JOIN dmrn_dim_cust_consumelev b on a.his_before_should_fee >= b.min_values AND a.his_before_should_fee < b.max_values \        
			LEFT OUTER JOIN dmrn_dim_cust_consumelev c on a.rn_next_should_fee >= c.min_values AND a.rn_next_should_fee < c.max_values \        
			LEFT OUTER JOIN dmrn_dim_cust_onlinelev d on a.his_before_online >= d.min_values AND a.his_before_online < d.max_values \
			LEFT OUTER JOIN dmrn_dim_flea_onlinelev e on a.his_before_online >= e.min_values AND a.his_before_online < e.max_values \        
		"   
		if [catch { aidb_sql $handle $sql } errmsg ] {
			trace_sql $errmsg 1104
			return -1
		} 
		aidb_commit $conn    
		
		set sql "   \
			INSERT INTO dmrn_user_$city_id\_$yyyymm2    \
			( rn_date                      ,   \
				rn_user_id                  ,   \
				his_user_id                 ,   \
				rn_user_number              ,   \
				his_user_number             ,   \
				rn_base_brand_id            ,   \
				rn_base_region_code         ,   \
				rn_base_county_code         ,   \
				rn_base_plan_id             ,   \
				rn_base_plan_pay_mode       ,   \
				rn_create_date              ,   \
				rn_base_should_fee          ,   \
				rn_base_fav_fee             ,   \
				rn_next_should_fee          ,   \
				rn_next_fav_fee             ,   \
				his_before_brand_id         ,   \
				his_before_online           ,   \
				his_before_plan_id          ,   \
				his_before_plan_pay_mode    ,   \
				his_create_date             ,   \
				his_base_should_fee         ,   \
				his_base_fav_fee            ,   \
				his_before_should_fee       ,   \
				his_before_fav_fee          ,   \
				his_base_unpay_fee          ,   \
				his_base_avg_payed_count    ,   \
				his_base_avg_payed_money    ,   \
				his_base_unpay_duration     ,   \
				his_before2_should_fee      ,   \
				his_before2_fav_fee         ,   \
				his_before3_should_fee      ,   \
				his_before3_fav_fee         ,   \
				his_next_life_flag          ,   \
				his_before_consumelev       ,   \
				rn_next_consumelev          ,   \
				his_before_onlinelev        ,   \
				flea_onlinelev              ,   \
				accord_level                ,   \
				his_base_payed_countlev     ,   \
				his_base_payed_sumlev       ,   \
				his_base_unpay_level        ,   \
				his_base_unpay_duration_level,  \
				rn_base_org_id              ,   \
				his_before_org_id           ,   \
				rn_base_dept_code           ,   \
				accord_rate                 ,   \
				change_phone_flag           ,   \
				flea_flag                   ,   \
				rn_card_code                ,   \ 
				rn_cust_name                ,   \ 
				his_card_code               ,   \ 
				his_cust_name               ,   \ 
				card_code_same_flag         ,   \ 
				cust_name_same_flag         )   \ 
			SELECT  \
				[ai_to_date $yyyymmdd2]     AS rn_date                  ,  \
				a.rn_user_id                AS rn_user_id               ,  \
				a.his_user_id               AS his_user_id              ,  \
				a.rn_user_number            AS rn_user_number           ,  \
				a.his_user_number           AS his_user_number          ,  \
				a.rn_base_brand_id          AS rn_base_brand_id         ,  \
				a.rn_base_region_code       AS rn_base_region_code      ,  \
				a.rn_base_county_code       AS rn_base_county_code      ,  \
				a.rn_base_plan_id           AS rn_base_plan_id          ,  \
				a.rn_base_plan_pay_mode     AS rn_base_plan_pay_mode    ,  \
				a.rn_create_date            AS rn_create_date           ,  \
				a.rn_base_should_fee        AS rn_base_should_fee       ,  \
				a.rn_base_fav_fee           AS rn_base_fav_fee          ,  \
				a.rn_next_should_fee        AS rn_next_should_fee       ,  \
				a.rn_next_fav_fee           AS rn_next_fav_fee          ,  \
				a.his_before_brand_id       AS his_before_brand_id      ,  \
				a.his_before_online         AS his_before_online        ,  \
				a.his_before_plan_id        AS his_before_plan_id       ,  \
				a.his_before_plan_pay_mode  AS his_before_plan_pay_mode ,  \
				a.his_create_date           AS his_create_date          ,  \
				a.his_base_should_fee       AS his_base_should_fee      ,  \
				a.his_base_fav_fee          AS his_base_fav_fee         ,  \
				a.his_before_should_fee     AS his_before_should_fee    ,  \
				a.his_before_fav_fee        AS his_before_fav_fee       ,  \
				a.his_base_unpay_fee        AS his_base_unpay_fee       ,  \
				a.his_base_avg_payed_count  AS his_base_avg_payed_count ,  \
				a.his_base_avg_payed_money  AS his_base_avg_payed_money ,  \
				a.his_base_unpay_duration   as his_base_unpay_duration  ,  \
				a.his_before2_should_fee    AS his_before2_should_fee   ,  \
				a.his_before2_fav_fee       AS his_before2_fav_fee      ,  \
				a.his_before3_should_fee    AS his_before3_should_fee   ,  \
				a.his_before3_fav_fee       AS his_before3_fav_fee      ,  \
				a.his_next_life_flag        AS his_next_life_flag       ,  \
				a.his_before_consumelev     AS his_before_consumelev    ,  \
				a.rn_next_consumelev        AS rn_next_consumelev       ,  \
				a.his_before_onlinelev      AS his_before_onlinelev     ,  \
				a.flea_onlinelev            AS flea_onlinelev           ,  \
				CASE WHEN a.accord_rate IS NULL THEN 0 ELSE e.accord_level end, \
				CASE WHEN a.his_base_avg_payed_count IS NULL THEN 0 ELSE b.payed_count_level end , \
				CASE WHEN a.his_base_avg_payed_money IS NULL THEN 0 ELSE c.payed_sum_level end , \
				CASE WHEN a.his_base_unpay_fee IS NULL THEN 0 ELSE d.unpay_level end , \
				CASE WHEN a.his_base_unpay_duration IS NULL THEN 0 ELSE f.durationlev end , \   
				a.rn_base_org_id            AS rn_base_org_id           ,  \
				a.his_before_org_id         as his_before_org_id        ,  \
				a.rn_base_dept_code         AS rn_base_dept_code        ,  \
				a.accord_rate               AS accord_rate              ,  \
				a.change_phone_flag         AS change_phone_flag        ,  \
				a.flea_flag                 AS flea_flag                ,  \                                                       
				a.rn_card_code              AS rn_card_code             ,  \ 
				a.rn_cust_name              AS rn_cust_name             ,  \ 
				a.his_card_code             AS his_card_code            ,  \ 
				a.his_cust_name             AS his_cust_name            ,  \ 
				a.card_code_same_flag       AS card_code_same_flag      ,  \ 
				a.cust_name_same_flag       AS cust_name_same_flag         \ 
			FROM tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second a   \
			LEFT OUTER JOIN dmrn_dim_payed_count_level b on a.his_base_avg_payed_count >= b.min_values AND a.his_base_avg_payed_count < b.max_values \        
			LEFT OUTER JOIN dmrn_dim_payed_sum_level c on a.his_base_avg_payed_money >= c.min_values AND a.his_base_avg_payed_money < c.max_values \        
			LEFT OUTER JOIN dmrn_dim_unpay_level d  on a.his_base_unpay_fee >= d.min_values AND a.his_base_unpay_fee < d.max_values \        
			LEFT OUTER JOIN dmrn_dim_accord_level e  on a.accord_rate >= e.min_values AND a.accord_rate < e.max_values \
			LEFT OUTER JOIN dmrn_dim_unpay_durationlev f  on a.his_base_unpay_duration >= f.min_values AND a.his_base_unpay_duration < f.max_values \
		"   
		
		if [catch { aidb_sql $handle $sql } errmsg ] {
			trace_sql $errmsg 1105
			return -1
		}  
		aidb_commit $conn
		
		
		
		if [catch { aidb_sql $handle "DROP TABLE tmp_dmrn_user_$city_id\_$yyyymm2\_ms_first" } errmsg ] {
			trace_sql $errmsg 1108
			return -1
		}  
		aidb_commit $conn
		
		if [catch { aidb_sql $handle "DROP TABLE tmp_dmrn_user_$city_id\_$yyyymm2\_ms_second" } errmsg ] {
			trace_sql $errmsg 1109
			return -1
		}  
		aidb_commit $conn
	
		if [catch { aidb_sql $handle "DELETE FROM dmrn_user_ms WHERE rn_date = [ai_to_date $yyyymmdd2] and rn_base_region_code = '$city_id' " } errmsg ] {
			trace_sql $errmsg 1106
			return -1
		}  
		aidb_commit $conn
    	
		if [catch { aidb_sql $handle "INSERT INTO dmrn_user_ms SELECT * FROM dmrn_user_$city_id\_$yyyymm2 " } errmsg ] {
			trace_sql $errmsg 1107
			return -1
		}  
		aidb_commit $conn
    	
#		if [catch { aidb_sql $handle "DELETE FROM dmrn_user_list_ms WHERE rn_date = [ai_to_date $yyyymmdd2] and rn_base_region_code = '$city_id' " } errmsg ] {
#			trace_sql $errmsg 1108
#			return -1
#		}  
#		aidb_commit $conn
#
#		set sql "   \
#		insert into DMRN_USER_LIST_MS ( \
#			RN_DATE                   , \
#			RN_USER_ID                , \
#			HIS_USER_ID               , \
#			RN_USER_NUMBER            , \
#			HIS_USER_NUMBER           , \
#			RN_BASE_BRAND_ID          , \
#			RN_BASE_BRAND_NAME        , \
#			RN_BASE_REGION_CODE       , \
#			RN_BASE_REGION_NAME       , \
#			RN_BASE_COUNTY_CODE       , \
#			RN_BASE_COUNTY_NAME       , \
#			RN_BASE_PLAN_ID           , \
#			RN_BASE_PLAN_NAME         , \
#			RN_CREATE_DATE            , \
#			HIS_BEFORE_BRAND_ID       , \
#			HIS_BEFORE_BRAND_NAME     , \
#			HIS_BEFORE_ONLINE         , \
#			HIS_BEFORE_PLAN_ID        , \
#			HIS_BEFORE_PLAN_NAME      , \
#			HIS_CREATE_DATE           , \
#			HIS_BASE_UNPAY_FEE        , \
#			HIS_BASE_AVG_PAYED_COUNT  , \
#			HIS_BASE_AVG_PAYED_MONEY  , \
#			HIS_NEXT_LIFE_FLAG        , \
#			HIS_BEFORE_CONSUMELEV     , \
#			HIS_BEFORE_CONSUME_NAME   , \
#			RN_NEXT_CONSUMELEV        , \
#			RN_NEXT_CONSUME_NAME      , \
#			HIS_BEFORE_ONLINELEV      , \
#			FLEA_ONLINELEV            , \
#			RN_BASE_DEPT_CODE         , \
#			RN_BASE_DEPT_NAME         , \
#			ACCORD_LEVEL              , \
#			FLEA_FLAG                 , \
#			CHANGE_PHONE_FLAG         ) \
#		select \
#			a.RN_DATE                    ,  \
#			a.RN_USER_ID                 ,  \
#			a.HIS_USER_ID                ,  \
#			a.RN_USER_NUMBER             ,  \
#			a.HIS_USER_NUMBER            ,  \
#			a.RN_BASE_BRAND_ID           ,  \
#			value(b.TRADEMARK_DESC,'不详'), \
#			a.RN_BASE_REGION_CODE        ,  \
#			value(c.REGION_NAME,'不详')  ,  \
#			a.RN_BASE_COUNTY_CODE        ,  \
#			value(d.COUNTY_NAME,'不详')  ,  \
#			a.RN_BASE_PLAN_ID            ,  \
#			value(e.PLAN_NAME,'不详')    ,  \
#			a.RN_CREATE_DATE             ,  \
#			a.HIS_BEFORE_BRAND_ID        ,  \
#			value(f.TRADEMARK_DESC,'不详'), \
#			a.HIS_BEFORE_ONLINE          ,  \
#			a.HIS_BEFORE_PLAN_ID         ,  \
#			value(g.PLAN_NAME,'不详')    ,  \
#			a.HIS_CREATE_DATE            ,  \
#			a.HIS_BASE_UNPAY_FEE         ,  \
#			a.HIS_BASE_AVG_PAYED_COUNT   ,  \
#			a.HIS_BASE_AVG_PAYED_MONEY   ,  \
#			a.HIS_NEXT_LIFE_FLAG         ,  \
#			a.HIS_BEFORE_CONSUMELEV      ,  \
#			value(h.LEVEL_NAME,'不详')   ,  \
#			a.RN_NEXT_CONSUMELEV         ,  \
#			value(i.LEVEL_NAME,'不详')   ,  \
#			a.HIS_BEFORE_ONLINELEV       ,  \
#			a.FLEA_ONLINELEV             ,  \
#			a.RN_BASE_DEPT_CODE          ,  \
#			value(j.DEPT_NAME,'不详')    ,  \
#			a.ACCORD_LEVEL               ,  \
#			a.FLEA_FLAG                  ,  \
#			a.CHANGE_PHONE_FLAG             \
#		from dmrn_user_$city_id\_$yyyymm2 a \
#		left outer join DMRN_DIM_TRADEMARK b on a.RN_BASE_BRAND_ID=b.code \
#		left outer join DMRN_DIM_PUB_CITY c on a.RN_BASE_REGION_CODE= c.region_code \
#		left outer join DMRN_DIM_PUB_COUNTY d on a.RN_BASE_REGION_CODE= d.region_code and a.RN_BASE_COUNTY_CODE=d.county_code \
#		left outer join DMRN_DIM_FEE_PLAN_MS e on a.RN_BASE_REGION_CODE = e.region_code and a.RN_BASE_PLAN_ID = e.plan_id \
#		left outer join DMRN_DIM_TRADEMARK f on a.RN_BASE_BRAND_ID=f.code \
#		left outer join DMRN_DIM_FEE_PLAN_MS g on a.RN_BASE_REGION_CODE = g.region_code and a.HIS_BEFORE_PLAN_ID = g.plan_id \
#		left outer join DMRN_DIM_CUST_CONSUMELEV h on a.HIS_BEFORE_CONSUMELEV=h.CONSUMELEV \
#		left outer join DMRN_DIM_CUST_CONSUMELEV i on a.RN_NEXT_CONSUMELEV=i.CONSUMELEV \
#		left outer join DMRN_DIM_DEPT_CODE_MS j on a.RN_BASE_REGION_CODE=j.REGION_CODE and a.RN_BASE_DEPT_CODE=j.DEPT_CODE "
#
#		if [catch { aidb_sql $handle $sql } errmsg ] {
#			trace_sql $errmsg 1109
#			return -1
#		}  
#		aidb_commit $conn
#	}
	
	puts "DMRN_USER_MS_M FINISH !"
	
	return 0
 
}
