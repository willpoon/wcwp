
set sql_buff "insert into bass1.G_S_04002_DAY_THIS
						(
						 time_id
						,product_no
						,roam_locn
						,roam_type_id
						,apnni
						,start_time
						,call_duration
						,up_flows
						,down_flows
						,all_fee
						,mns_type
						,imei
						,service_code
						 )
				 select
					  $timestamp
					  ,product_no
					  ,COALESCE(char(substr(VPLMN1,3)),'852') as roam_locn
					  ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(ROAM_TYPE)),'500') as roam_type_id
					  ,apn_ni
					  ,replace(char(date(start_time)),'-','')||replace(char(time(start_time)),'.','') as start_time
					  ,char(sum(duration))        as call_duration
					  ,char(sum(data_flow_up1+data_flow_up2))   as up_flows
					  ,char(sum(data_flow_down1+data_flow_down2)) as down_flows
					  ,char(sum(charge1+charge2+charge3+charge4)/10) as all_fee
					  ,'0' as mns_type
					  ,' '     as imei
					  ,case when upper(apn_ni)<>'CMWAP' then ''
					   else '4000000001' end      as service_code
				from bass2.cdr_gprs_l_$timestamp
				where drtype_id not in (8307)
				  and bigint(product_no) not between 14734500000 
				  and 14734999999 and apn_ni <> 'JF.XZ.IP.MOBILE.LAN.CHINAMOBILE'
				group by product_no
					  ,COALESCE(char(substr(VPLMN1,3)),'852') 
					  ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(ROAM_TYPE)),'500') 
					  ,apn_ni
					  ,start_time
					  ,case when upper(apn_ni)<>'CMWAP' then ''
					   else '4000000001' end 
					with ur
					 "
		exec_sql $sql_buff