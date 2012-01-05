   select * from         G_S_02024_DAY  a 
WHERE USER_ID
IN (
'89160000878180'            
,'89160000878007'            
,'89160000878136'            
,'89160000878159'            
,'89160000878212'            
,'89160000878281'            
,'89160000878293'            
,'89160000878323'            
,'89160000878855'            
,'89160000878902'            
,'89160000879699'            
,'89160000879705'            
,'89760000859398'            
,'89157332235104'            
,'89157332112431'            
,'89460000857857'            
,'89760000859381'            
,'89160000878376'            
,'89160000878383'            
,'89160000878483'            
,'89160000878497'            
,'89160000878949'            
,'89160000878970'            
,'89160000879035'            
,'89160000879088'            
,'89160000879323'            
,'89160000879346'            
,'89160000879367'            
,'89160000879527'            
,'89160000879573'            
,'89160000879585'            
,'89760000859353'            
,'89160000878345'            
)

TIME_ID	USER_ID	BASE_PKG_ID	CHANNEL_ID	REC_DT	VALID_DT
20111231	89157332235104      	QW_QQT_JC_SL88                	11111115                                	20111231	20120101

select * from G_I_02022_DAY
where user_id = '89157332235104'
AND TIME_ID <= 20111229


TIME_ID	USER_ID	BASE_PKG_ID	VALID_DT
20111229	89157332235104      	QW_QQT_JC_SL388               	20110801


select * from bass2.dwd_product_test_phone_20111229 b 
				where b.USER_ID  = '89157332235104'
				and b.sts = 1
null


				



        select count(distinct a.user_id) VALID_CNT,count(distinct b.user_id ) REC_CNT, count(distinct b.user_id )*1.00/count(distinct a.user_id) RATE
        from 
        (
        select * from   
        G_I_02022_DAY  a
        where time_id = 20120103 
	except 
	select * from   
        G_I_02022_DAY  a
        where time_id = 20120102
        ) a
        left join (
                        select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
                from (
                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                     ) a 
                left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
                ) b on a.USER_ID = b.USER_ID and a.BASE_PKG_ID= b.BASE_PKG_ID
        with ur
	