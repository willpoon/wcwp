--1. �鿴�澯
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc

--2.һ���Լ�� 
select * from   table( bass1.chk_same(0) ) a order by 2

--3. �ļ������ؼ�飺
select *
from 
(
select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'
) t where rn = 1


--4. ��¼�����ؼ��
select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1


--5. �����Լ��

select * from   table( bass1.chk_wave(0) ) a order by 2


