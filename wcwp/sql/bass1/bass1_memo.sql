ailknfjdbass99##%%
D2008ABCDE
>大文件 21003

#export命令将使系统在创建每一个新的shell时定义这个变量的一个拷贝。这个过程称之为变量输出。


校验文件：
?	接口数据文件名称|?	数据日期|?	文件的生成日期|?	文件的大小（字节数）|?	文件中包含的记录数
|?	文件中单行记录的长度|?	期待日期
2.3.5.2.2	业务逻辑校验：RXXX
2.3.5.2.2.2	业务指标校验

9点接口：



渠道
集团-修复
考核

E:\bass1\中国移动一级经营分析系统省级数据接口规范(V1.7.1)\一级经营分析系统省级数据接口及时性时限要求1.7.1.xls


/bassapp/backapp/data/bass1/report/report_201102

/bassapp/backapp/data/bass1/export


ls 日期显示：
-rw-r--r--   1 app      appdb    22969750  3??  2è? 13:05 i_13100_201102_02049_01_001.dat
-rw-r--r--   1 app      appdb        104  3??  2è? 13:05 i_13100_201102_02049_01.verf


执行方法                 : bass1_export 模式名.表名  YYYY-MM[-DD]

key:搞清楚依赖

UNIX 系统上的 LANG 变量

大多数 UNIX 系统使用 LANG 变量指定期望的语言环境。然而，不同的 UNIX 操作系统需要不同的语言环境名称来指定相同的语言。请确保使用您正在使用的 UNIX 操作系统所支持的 LANG 值。

要获取 UNIX 系统的语言环境名称，请输入以下内容：
locale -a

1.测试短信
2.测试指标
3.测试校验r
int_main.tcl:set program_name $arg(-s)
                                      

for { set i 0 } {$i < [expr $argc - 1 ]} {incr i} {
	set flag [lindex $argv $i]
	set value [lindex $argv [expr $i + 1] ]
	if { [string index $flag 0] == "-" } {
		set arg($flag) $value
	}
}

set program_name $arg(-s)
set ddh $arg(-u)
set rwh $arg(-v)for { set i 0 } {$i < [expr $argc - 1 ]} {incr i} {
	set flag [lindex $argv $i]
	set value [lindex $argv [expr $i + 1] ]
	if { [string index $flag 0] == "-" } {
		set arg($flag) $value
	}
}

set program_name $arg(-s)
set ddh $arg(-u)
set rwh $arg(-v)                                      for { set i 0 } {$i < [expr $argc - 1 ]} {incr i} {
	set flag [lindex $argv $i]
	set value [lindex $argv [expr $i + 1] ]
	if { [string index $flag 0] == "-" } {
		set arg($flag) $value
	}
}

set program_name $arg(-s)
set ddh $arg(-u)
set rwh $arg(-v)


344	取消原业务指标校验文件及相关校验报告机制	1.5.9	2009-5-20	日接口自日数据日期20090601日起终止上传；
月接口自数据日期200905起终止上传；


cc_flag = 2 :不需要执行调度


getpid()
strrchr()
sprintf()



	set sql_buff "delete  from bass1.G_A_02059_DAY a where exists
								(
									select * from
									(
									select time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date,count(*)
									from bass1.G_A_02059_DAY
									where time_id = $timestamp
									group by time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date
									having count(*)>1
									) as b
								where a.time_id = b.time_id
								  and a.enterprise_id = b.enterprise_id
								  and a.user_id = b.user_id
								  and a.enterprise_busi_type = b.enterprise_busi_type
								  and a.manage_mode = b.manage_mode
								  and a.order_date = b.order_date
								  and a.time_id=$timestamp
								 )"



1.取消02059 1040 全量修复代码
2.备份02059 tcl

标准术语:集团订购关系

存在问题：01002 ：set sex_id ='1'？还是 set sex_id ='2' ？

  set handle [aidb_open $conn]
	set sql_buff "update  bass1.g_a_01002_day
                  set sex_id ='1'
                  where 
                    time_id=$timestamp and  card_type = '101' and 
                    length(card_code) = 18                    and 
                    substr(card_code,17,1) in ('2','4','5','8','0') "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle

CREATE FUNCTION BASS1.FN_GET_ALL_DIM(GID VARCHAR(20),DID VARCHAR(20)) 
RETURNS VARCHAR(10) DETERMINISTIC NO EXTERNAL ACTION LANGUAGE SQL 
BEGIN ATOMIC RETURN 
	SELECT BASS1_VALUE 
	FROM BASS1.ALL_DIM_LKP 
	WHERE BASS1_TBID = GID 
	AND XZBAS_VALUE = DID; 
	END


去重一法:
 select distinct a.enterprise_id from
   (select time_id,enterprise_id,cust_statu_typ_id from G_A_01004_DAY where time_id <= ${timestamp} ) a,
   (select enterprise_id,max(time_id) as time_id  from G_A_01004_DAY 
                                                 where time_id<=${timestamp}  
                                              group by enterprise_id)b
where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20'

更有效的一法：

select distinct t.enterprise_id
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= ${timestamp}   
) t where t.rn = 1 and  cust_statu_typ_id = '20'


EPOCH:

CREATE FUNCTION EPOCH (DB2TIME TIMESTAMP) \
RETURNS INTEGER \
LANGUAGE SQL \
CONTAINS SQL \
DETERMINISTIC \
RETURN CAST (DAYS(DB2TIME) - DAYS('1970-01-01') AS INTEGER) * 86400 + MIDNIGHT_SECONDS(DB2TIME) 
values epoch (current timestamp)


code mod:
02062 :test | fix 
--02054 fix
--02061 : fix 

--td 数据 超标
select a.user_id,call_duration_m  from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_20110316
where MNS_TYPE=1
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_20110316 where (td_call_mark =1
            or td_gprs_mark =1
            or td_addon_mark=1)
and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9)
and test_mark=0 ) b
on a.user_id=b.user_id 
order by call_duration_m desc

select * from bass2.dw_cust_20110316
where cust_id in
('89102999606167'
,'89103000351204'
,'89103001246538'
,'89103001345719'
,'89103001385579'
,'89103001397437'
,'89103001405597'
,'89103001419261'
,'89103001422002'
,'89103001432717'
,'89103001462079'
,'89103001462973'
,'89103001468028'
,'89103001472114'
,'89103001489911'
,'89103001497535'
,'89103001509522'
,'89103001516985'
,'89103001517803'
,'89103001527146'
,'89103001545645'
,'89103001547288'
,'89103001552042'
,'89103001557903'
,'89103001562754'
,'89103001564041'
,'89103001566045'
,'89103001579025'
,'89103001591016'
,'89103001611814'
,'89103001629021'
,'89160000049132'
,'89160000082895'
,'89160000099010'
,'89160000113502'
,'89160000140593'
,'89160000151569'
,'89160000157640'
,'89160000178619'
,'89160000185091'
,'89160000203492'
,'89160000235117'
,'89160000249193'
,'89160000257335'
,'89160000267185'
,'89160000271155'
,'89160000271384'
,'89160000271591'
,'89160000290183'
,'89160000316164'
,'89160000337360'
,'89160000339409'
,'89160000349980'
,'89160000357178'
,'89160000360284'
,'89160000369411'
,'89160000375348'
,'89160000385956'
,'89160000389268'
,'89160000392332'
,'89160000452947'
,'89160000465467'
,'89160000472349'
,'89160000472517'
,'89160000491753'
,'89160000492591'
,'89160000496262'
,'89160000509758'
,'89160000511582'
,'89160000521425'
,'89160000552112'
,'89160000560946'
,'89160000561144'
,'89160000562463'
,'89160000563182'
,'89160000569568'
,'89160000570118'
,'89160000573651'
,'89160000574154'
,'89160000581440'
,'89160000585242'
,'89160000591485'
,'89160000594576'
,'89160000595950'
,'89160000595960'
,'89160000596559'
,'89160000598156'
,'89160000598167'
,'89160000602571'
,'89160000604436')


--离网客户数一致性 研究

-->>注释两个脚本


$ ./bass1_report
校验报告处理程序开始!
用户名:app,密码:app,数据库名:BASSDB,数据文件目录:/bassapp/backapp/data/bass1/report
mkdir: 创建目录 "/bassapp/backapp/data/bass1/report/report" 失败；文件已经存在
下载检验报告文件!
/data1/asiainfo/interface/report/
/bassapp/backapp/data/bass1/report/report
Interactive mode off.
Local directory now /bassapp/backapp/data/bass1/report/report
*.verf: 无此文件或目录
获取检验报告信息!
/bassapp/backapp/data/bass1/report/report/f_?_13100_????????_*_??_???.verf: 无此文件或目录
/bassapp/backapp/data/bass1/report/report/r_?_13100_????????_*_??.verf: 无此文件或目录
/bassapp/backapp/data/bass1/report/report/o_?_13100_????????_*_??.verf: 无此文件或目录
/bassapp/backapp/data/bass1/report/report/b_13100_????????_??.verf: 无此文件或目录
/bassapp/backapp/data/bass1/report/report/f_?_13100_??????_*_??_???.verf: 无此文件或目录
/bassapp/backapp/data/bass1/report/report/r_?_13100_??????_*_??.verf: 无此文件或目录
/bassapp/backapp/data/bass1/report/report/o_?_13100_??????_*_??.verf: 无此文件或目录
/bassapp/backapp/data/bass1/report/report/b_13100_??????_p_??.verf: 无此文件或目录
取所有报告信息结束[文件数:0]

校验报告处理程序结束,系统退出!
$ pwd
/bassapp/backapp/bin/bass1_report


1324 zsched
->
2778 /usr/lib/inet/inetd start
->
21040 /usr/sbin/in.telnetd
->
21056 -sh
->
21163 ksh
->
11485 ps -f


autoftp(){
HOME=/bassapp/bass2/panzw2
export HOME
ftp -v 172.16.9.25
HOME=/bassapp/bass2
export HOME
echo $$
}

autoftp(){
HOME=/bassapp/bihome/panzw
export HOME
ftp -v 172.16.9.25
HOME=/bassapp/bass1
export HOME
echo $$
}



putdatfile(){
FTPHOST=172.16.9.25
REMOTE_DIR=data
LOCAL_DIR=/bassapp/bihome/panzw
HOME=/bassapp/bihome/panzw
export HOME

ftp_mac_put_dat_file=${HOME}/put_dat.mac.ftp
ftped_file_list=${HOME}/ftped_dat.lst
#生成ftp命令文件
echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_dat_file}
echo "bin" > ${ftp_mac_put_dat_file}
echo "prompt off" > ${ftp_mac_put_dat_file}
echo "mput *.dat" > ${ftp_mac_put_dat_file}
echo "ls -lrt" >> ${ftp_mac_put_dat_file}
echo "dir *.dat ${ftped_file_list}" >> ${ftp_mac_put_dat_file}
#上传
ftp -v ${FTPHOST} < ${ftp_mac_put_dat_file}

#与本地校验：文件数|文件名|文件大小


#恢复$HOME
HOME=/bassapp/bass1
export HOME
echo $$
}



putverffile(){
FTPHOST=172.16.9.25
REMOTE_DIR=data
LOCAL_DIR=/bassapp/bihome/panzw
HOME=/bassapp/bihome/panzw
export HOME

ftp_mac_put_verf_file=${HOME}/put_verf.mac.ftp


#生成ftp命令文件
echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_verf_file}
echo "bin" >> ${ftp_mac_put_verf_file}
echo "prompt off" >> ${ftp_mac_put_verf_file}
echo "mput *.verf" >> ${ftp_mac_put_verf_file}
echo "ls -lrt" >> ${ftp_mac_put_verf_file}
#上传
if [ -f ${ftp_mac_put_dat_file} ];then 
ftp -v ${FTPHOST} <  ${ftp_mac_put_verf_file}
rm ${ftp_mac_put_dat_file}
else 
echo "dat 文件未上传，请先上传dat，再传verf!!"
return 1
fi

#恢复$HOME
HOME=/bassapp/bass1
export HOME
echo $$
}



优先级:
1.9点接口
1.1 9点报错接口
2.


问题：
记录级返回影响到回复。
02012 取数 更新
9点接口校验
app用户跑程序问题

集团下发清单
维表导入
自动上传

INTERFACE_LEN_02059="1 15,16 17,18 31,32 32,33 46,47 47,48 53"
len_val=${INTERFACE_LEN_02059}
DB2_SQLCOMM="db2 \"load client from ${WORK_PATH}/${datafilename} of asc modified by timestampformat=\\\"YYYYMMDDHHMMSS\\\" dateformat=\\\"YYYYMMDD\\\"  timeformat=\\\"HHMMSS\\\" method L (${len_val}) insert into ${table_name} nonrecoverable\""


INTERFACE_LEN_99008="1 10,11 30,31 34,35 42"

XZBOSS    XZBOSS              204 
929000    2êD??DD?            300 
1860      admin-1860          204 

 89102999523639      89157331866928      1140320080530113100  


ls sort 


ls -l *.dat|sort -k5,5 -k9,9



for dir in export_201101??
do 
cd  $dir
ls -l *.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l
cd ..
done
      19
      19
      20
      17
      18
      19
      17
      19
      19
      18
      19
      18
      19
      18
      20
      19
      18
      19
      17
      19
      18
      19
      20
      19
      19
      19
      19
      17
      19
      19
      17

for dir in export_201102??
do 
cd  $dir
ls -l *.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l
cd ..
done      
      18
      19
      20
      20
      20
      20
      20
      19
      19
      19
      19
      17
      19
      19
      18
      17
      18
      18
      19
      19
      17
      19
      19
      19
      18
      19
      19
      17
      
for dir in export_201103??
do 
cd  $dir
ls -l *.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l
cd ..
done  

      19
      17
      19
      19
      19
      20
      20
      20
      20
      20
      18
      17
      18
      18
      18
      17
      15
      16
      20
      19
      17
      16
      18
      
for dir in export_201012??
do 
cd  $dir
ls -l *.dat|sort -k5,5 -k9,9|grep "appdb          0  "|wc -l
cd ..
done             


ailknfjdbass99##%%

db2 "select * from app.sch_control_before " > sch_control_before.txt

grep -i BASS1_INT_0400810_YYYYMM.tcl sch_control_before.txt

db2 "
select * from  app.sch_control_alarm \
where alarmtime >=  current timestamp - 60 days \
and control_code = 'BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl' \
order by alarmtime desc with ur
"

查找分隔符的个数
zcat I1700220110308000000.AVL.Z | head -1 | awk -F'$' '{print NF}'
查找分隔符不正确的数目的行数
zcat I1700220110308000000.AVL.Z | head -1 | awk -F'$' '{if(NF!=18) print NR}'
显示指定的行
zcat I1700220110308000000.AVL.Z | sed -n '12,13p'


--改代码
--imei
--107/108
--bass1_list
优化校验程序
05001，05002 局方提供


db2 runstats on table bass1.g_i_02021_month_temp1 with distribution and detailed indexes all


db2 list application


db2 connect to bassdb user bass2 using bass2
db2 "export to /bassapp/bass2/bak_xufr/sms_sender891.txt of del select distinct product_no from dw_newbusi_sms_201010 a where a.call
type_id = 0 and not exists (select 1 from (select user_id from tmp_black_user_20101105 union select distinct product_instance_id use
r_id from ODS_PRODUCT_INS_PROD_RED_20101031) b where a.user_id = b.user_id) and a.city_id = '891' order by product_no"
db2 terminate



alias bin='cd /bassdb2/etl/E/boss/java/crm_interface/bin'
alias pzh='cd /bassdb2/etl/E/panzw2'
alias lo='cd /bassdb2/etl/L/boss'
alias cfg='cd /bassdb2/etl/E/boss/java/crm_interface/bin/config/BOSS'
alias bak='cd /bassdb2/etl/L/boss/backup'
alias vlog='/bassdb2/etl/E/panzw2/ViewLoadLog_bassdb46.sh'

接口重传规则：
1.记录级返回错误：直接重送。一般扣分。
2.记录级未返回：申告重送。不能直接重传。可以避免扣分。

06002	校园位置信息
#问题记录：西藏没此业务，因此接口暂时送空文件

G_A_06001_DAY.tcl:#问题记录：西藏没此业务，因此接口暂时送空文件
G_I_02017_MONTH.tcl:#问题记录：西藏没此业务，因此接口暂时送空文件
G_I_06002_MONTH.tcl:#问题记录：西藏没此业务，因此接口暂时送空文件
G_S_22401_MONTH.tcl:#问题记录：西藏没此业务，因此接口暂时送空文件
G_S_22403_DAY.tcl:#问题记录：西藏没此业务，因此接口暂时送空文件




月数据：
3号前接口：
ls -alrt *02049*dat *02053*dat *03001*dat *03002*dat *03003*dat *06011*dat *06012*dat *06029*dat

5号前接口：
ls -alrt *01005*dat *02005*dat *02014*dat *02015*dat *02016*dat *02047*dat *06021*dat *06022*dat *06023*dat *22009*dat *22101*dat *22103*dat *22105*dat *22106*dat *06002*dat
8号前接口：
ls -alrt *02006*dat *02007*dat *02052*dat *03004*dat *03005*dat *03012*dat \
*03015*dat *03016*dat *03017*dat *03018*dat *21003*dat *21006*dat *21008*dat \
*21011*dat *21012*dat *21020*dat *22204*dat *22036*dat *22040*dat *22072*dat \
*22303*dat *22304*dat *22305*dat *22306*dat *22307*dat *02017*dat *22401*dat 

10号前接口：
ls -alrt  *03007*dat *21010*dat *21013*dat *21014*dat *21015*dat *05001*dat \
*05002*dat *05003*dat *22013*dat *22021*dat *22025*dat *22032*dat *22033*dat \
*22039*dat *22041*dat *22042*dat *22043*dat  

15号前接口：
ls -lart *22049*dat *22050*dat *22052*dat *22055*dat *22056*dat *22061*dat \
*22062*dat *22063*dat *22064*dat *22065*dat  

ls -lart *22049*verf *22050*verf *22052*verf *22055*verf *22056*verf *22061*verf \
*22062*verf *22063*verf *22064*verf *22065*verf  



ls -lrt *01002*dat *01004*dat *02004*dat *02008*dat *02011*dat *02053*dat *06031*dat *06032*dat|awk '{print $9,$8,$5}'|sort



--记录数监控
--前置完善
--调度标志




/bassapp/bass1/trace


select * from app.g_file_report where filename like '%20110401%02013%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04002%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04003%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04006%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04007%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04015%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04016%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04018%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%22038%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%22073%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%22102%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%22104%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04008%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04009%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04010%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04011%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%04012%' and err_code = '00'
select * from app.g_file_report where filename like '%20110401%06001%' and err_code = '00'



-rw-r--r--   1 app      appdb      35112  4月  1日 12:07 i_13100_201103_06029_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  1日 12:07 i_13100_201103_06029_00.verf
-rw-r--r--   1 app      appdb       3422  4月  1日 12:08 i_13100_201103_06011_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  1日 12:08 i_13100_201103_06011_00.verf
-rw-r--r--   1 app      appdb       5336  4月  1日 12:08 i_13100_201103_06012_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  1日 12:08 i_13100_201103_06012_00.verf
-rw-r--r--   1 app      appdb    48398985  4月  1日 12:10 i_13100_201103_02053_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  1日 12:10 i_13100_201103_02053_00.verf
-rw-r--r--   1 app      appdb          0  4月  1日 12:12 s_13100_201103_22040_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  1日 12:12 s_13100_201103_22040_00.verf
-rw-r--r--   1 app      appdb          0  4月  1日 12:17 s_13100_201103_22042_00_001.dat
-rw-r--r--   1 app      appdb          0  4月  1日 12:17 s_13100_201103_21010_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  1日 12:17 s_13100_201103_22042_00.verf
-rw-r--r--   1 app      appdb        104  4月  1日 12:17 s_13100_201103_21010_00.verf
-rw-r--r--   1 app      appdb    67769352  4月  1日 12:19 i_13100_201103_03003_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  1日 12:19 i_13100_201103_03003_00.verf
-rw-r--r--   1 app      appdb    445401774  4月  2日 06:14 i_13100_201103_03002_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  2日 06:14 i_13100_201103_03002_00.verf
-rw-r--r--   1 app      appdb    22125250  4月  2日 07:03 i_13100_201103_02049_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  2日 07:03 i_13100_201103_02049_00.verf
-rw-r--r--   1 app      appdb         95  4月  2日 12:06 s_13100_201103_22105_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  2日 12:06 s_13100_201103_22105_00.verf
-rw-r--r--   1 app      appdb        145  4月  2日 12:27 s_13100_201103_22101_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  2日 12:27 s_13100_201103_22101_00.verf
-rw-r--r--   1 app      appdb        110  4月  2日 12:35 s_13100_201103_22103_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  2日 12:35 s_13100_201103_22103_00.verf
-rw-r--r--   1 app      appdb        110  4月  2日 12:35 s_13100_201103_22106_00_001.dat
-rw-r--r--   1 app      appdb        104  4月  2日 12:35 s_13100_201103_22106_00.verf


1.月调度：
按调度依赖excel
按生成接口-校验-导出的顺序。
监控调度的执行情况，长时间不跑要检查依赖。
--2号12个接口。
--02053 校验 时间



route add -p 10.233.0.0 mask 255.255.0.0 10.233.20.158
route add -p 10.10.0.0 mask 255.255.0.0 10.233.20.158
route add -p 172.16.0.0 mask 255.255.0.0 10.233.20.158




select count(0),count(distinct product_no) 
,sum(value(int(FLOWUP),0)/1024/1024+value(int(FLOWDOWN),0)/1024/1024)
from   bass1.G_S_04003_DAY
where time_id between  20101201 and 20101231

#task加入ignore，非9点接口延迟时，跳过。
#task监控记录级返回情况
#--分块统计记录级返回
#监控记录数
#监控批次处理的量
#检查日状态：如果8：30前还没上传接口，告警，月数据也可以提醒。

splfile s_13100_201103_21003_01_001 7300000 &



db2 "load from /dev/null of del terminate into  bass1.T_GS05001M"  
db2 "load from /dev/null of del terminate into  bass1.T_GS05002M"  

bass1.T_GS05002M

写监控脚本；看新的考核文档



  puts "插入到短信信息表里头"
        set sqlbuf "insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) 
                                                        select '数据日期${timestamp}报告------一经接口总数:${RESULT_VAL1}个,生成9点接口数:${RESULT_VAL3}个,今日总共生成的接口数:${RESULT_VAL2}个。',
                                                        phone_id 
                                                        from BASS2.ETL_SEND_MESSAGE where MODULE='BASS1'"
        exec_sql $sqlbuf
  puts "插入到短信信息表里头成功"
  
  
   panzw 13737  6979   0 17:13:42 pts/31      0:00 sh bass1_mon.sh
   nohup sh bass1_mon.sh > bass1_mon.sh.out 2>&1 &
   
   
	puts [ exec date +%H%M%S ]

   
   
		#set sql_buff "
		#			create table bass1.G_S_22062_MONTH_TMP_1bak like session.G_S_22062_MONTH_TMP_1
		#			DATA CAPTURE NONE
		#			IN TBS_APP_BASS1
		#			INDEX IN TBS_INDEX
		#			PARTITIONING KEY (CHANNEL_ID,ACCEPT_TYPE) USING HASHING
		#  "
    #puts $sql_buff
    #exec_sql $sql_buff  

		#set sql_buff "
		#			insert into  bass1.G_S_22062_MONTH_TMP_1bak select * from session.G_S_22062_MONTH_TMP_1
		#  "
    #puts $sql_buff
    #exec_sql $sql_buff      

 统计分析>Brio本地分析报表>集团客户部>2010年集团客户业务统计报表
--优化tcl
--修复渠道数据

int -s G_S_03018_MONTH.tcl > /bassapp/bihome/panzw/tmp/tclrunlog/G_S_03018_MONTH.tcl.`date +%Y%m%d%H%M%S`        


--增加校验！目录！

--应急方案(空接口，全量接口,导出程序)
--预防方案


/bassapp/backapp/data/bass1/export/ftplog

/bassapp/bass1/trace/G_S_03017_MONTH.trace

/bassapp/bass1/log/G_S_03018_MONTH.log



02053 日，月接口号重复！
ailknfjdbass99##%%


a_13100_20110417_01004_00_001.dat 02:57 9362400
a_13100_20110417_01002_00_001.dat 03:09 477704
a_13100_20110417_02053_00_001.dat 03:11 12895680
a_13100_20110417_02011_00_001.dat 03:14 386022
a_13100_20110417_02004_00_001.dat 03:28 325501
a_13100_20110417_02008_00_001.dat 03:28 1237702
i_13100_20110417_06031_00_001.dat 05:08 6024
i_13100_20110417_06032_00_001.dat 05:08 945


a_13100_20110418_01002_00_001.dat 02:55 500070
a_13100_20110418_01004_00_001.dat 02:58 9362400
a_13100_20110418_02053_00_001.dat 02:58 12898720
a_13100_20110418_02011_00_001.dat 02:59 422772
a_13100_20110418_02004_00_001.dat 03:08 356870
a_13100_20110418_02008_00_001.dat 03:08 1252356
i_13100_20110418_06032_00_001.dat 05:07 945
i_13100_20110418_06031_00_001.dat 05:07 6024


充值优惠
xiaofeixianjing


for file in *.tcl
do 
echo "`wc -l $file` ""`grep -i "#" $file | wc -l|awk -F' ' '{print $1}' ` $file">>/bassapp/bihome/panzw/tmp/tmp$$.txt
done



              if [  ${exp_cnt} -eq  56 ];then 
                      MESSAGE_CONTENT="`date +%H:%M:%S` | 56个接口全部导出!"
                      echo ${MESSAGE_CONTENT}
                      sendalarmsms ${MESSAGE_CONTENT}
              fi
              
              
              

while read x y
do
if [ $y = "d" ];then 
echo "日接口"
dt=yyyymmdd
echo ${x}_${dt}
fi

if [ $y = "m" ];then 
echo "月接口"
dt=yyyymm
echo ${x}_${dt}
fi
done<t.txt




while read filename
do
coarse=`echo $filename|awk -F'_' '{print $1}'`
code=`echo $filename|awk -F'_' '{print $4}'`
dt_type=`echo $filename|awk -F'_' '{print length($3)}'`
if [ $dt_type -eq 8 ];then
tabname=g_${coarse}_${code}_day
else
tabname=g_${coarse}_${code}_month
fi
echo $tabname
done<<!
s_XXXXX_yyyymmdd_22080_XX_XXX.dat
s_XXXXX_yyyymm_22081_XX_XXX.dat
s_XXXXX_yyyymmdd_22082_XX_XXX.dat
s_XXXXX_yyyymm_22083_XX_XXX.dat
s_XXXXX_yyyymmdd_22084_XX_XXX.dat
s_XXXXX_yyyymm_22085_XX_XXX.dat
i_XXXXX_yyyymmdd_02022_XX_XXX.dat
i_XXXXX_yyyymmdd_02023_XX_XXX.dat
!
              
              
D:\Databases\IBM\SQLLIB\samples\c


sprintf
strcpy
strncpy

由于sprintf 跟printf 在用法上几乎一样，只是打印的目的地不同而已，前者打印到字符串中，
后者则直接在命令行上输出。这也导致sprintf 比printf 有用得多。所以本文着重介绍sprintf，有时
也穿插着用用pritnf。
sprintf 是个变参函数，定义如下：
int sprintf( char *buffer, const char *format [, argument] ... );
除了前两个参数类型固定外，后面可以接任意多个参数。而它的精华，显然就在第二个参数：
格式化字符串上。



sprintf 最常见的应用之一莫过于把整数打印到字符串中，所以，spritnf 在大多数场合可以替代
itoa。如：
//把整数123 打印成一个字符串保存在s 中。
sprintf(s, "%d", 123); //产生"123"
可以指定宽度，不足的左边补空格：
sprintf(s, "%8d%8d", 123, 4567); //产生：" 123 4567"
当然也可以左对齐：
sprintf(s, "%-8d%8d", 123, 4567); //产生："123 4567"
也可以按照16 进制打印：
sprintf(s, "%8x", 4567); //小写16 进制，宽度占8 个位置，右对齐
sprintf(s, "%-8X", 4568); //大写16 进制，宽度占8 个位置，左对齐

strrchr(str,'/') 相当于 substr(str,n)       n 是 / 的位置。


不明白 sqlca.sqlcode = 0 是什么意思，搜了搜，记录一下sqlca.sqlcode的各种取值的意义：

    0 ——最近一次sql语句执行成功 

   -1 ——最近一次sql语句执行失败 

100 ——最近一次sql语句没有返回数据


http://xiongfeng.iteye.com/blog/627405

memset  （初始化函数）
1。void *memset(void *s,int c,size_t n) 　　
总的作用：将已开辟内存空间 s 的首 n 个字节的值设为值 c。 
http://baike.baidu.com/view/982208.htm#sub982208




 http://132.32.22.9/public/cmschema/N10_New_WXKH/WXKH_ViewMonth_Total.asp?Date=2010-7


user:    chinamobile\cs891   
pwd:   abcd@1234 


route add 132.32.22.0 mask 255.255.255.0 10.233.20.113 

alter table TABLENAME ALTER column columnName SET DATA TYPE varchar(10000)

TABLENAME:表名

columnName:列名

free_res_val1
：空的就是收费的，非空就是免费
is null 收费
is not null 免费


不占用套餐流量的免费流量， free_res_val1 is null 

套餐内:
免费
is not null 

套餐外:
--------------------------------------
1.免费流量：
1.1套餐内流量：free_res_val1 is not null and sum(charge1+charge2+charge3+charge4)/10 = 0  
1.2非套餐内的免费流量：free_res_val1 is  null 
2.收费流量：
2.1无套餐流量：sum(charge1+charge2+charge3+charge4)/10 > 0 
2.2套餐外流量：
--------------------------------------

2、通信费：客户使用手机报业务产生的通信费（如短信上行费用、 GPRS费用等），按现有资费标准执行。 
4、为什么接收手机报彩信的时候产生了GPRS流量费？ 
答：正常情况下，客户在国内接收手机报彩信产品是不会产生GPRS流量费的，但是如果客户出国时接收了手机报彩信，则会产生国际漫游 GPRS流量费，客户可以在出发前拨打10086进行退订或暂停。客户出国后，会收到相关提醒短信，如果不希望在国外接收手机报彩信，可按照短信提示内容进行操作或者拨打免费客服热线+8613800100186进行处理。 


WITH TEST(NAME_TEST, BDAY_TEST) AS   
(   
VALUES ('张三','1997-7-1'),('李四','1949-10-1')   
)   
SELECT NAME_TEST FROM TEST WHERE BDAY_TEST='1949-10-1'  
WITH TEST(NAME_TEST, BDAY_TEST) AS
(
VALUES ('张三','1997-7-1'),('李四','1949-10-1')
)
SELECT NAME_TEST FROM TEST WHERE BDAY_TEST='1949-10-1' 

--22085 不能传空的。

while read filename
do
touch $filename.tcl
done<<!
G_S_22080_DAY
G_S_22081_MONTH
G_S_22082_DAY
G_S_22083_MONTH
G_S_22084_DAY		
G_S_22085_MONTH
G_I_02022_DAY
G_I_02023_DAY
!
drwxr-xr-x    2 500      503          4096 Apr 01 09:13 sample




/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_02007_MONTH 2011-01 &
/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_02007_MONTH 2011-02 &
/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_02007_MONTH 2011-03 &




db2 connect to bassdb user bass2 using bass2

db2 "load client from /bassdb2/etl/L/boss/error/I0202520110503000000.AVL of del \
modified by coldel$   nochardel timestampformat=\"YYYYMMDDHHMMSS\" \
fastparse anyorder warningcount 1000 \
messages ./msg/ODS_AS_WORK_ACCEPT_20110503.log \
replace into ODS_AS_WORK_ACCEPT_20110503"


