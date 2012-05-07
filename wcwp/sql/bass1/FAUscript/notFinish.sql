select       
case  when date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 then 'δ���' 
      when   b.flag=1   then 'ִ����'
      when   b.flag=-1  then 'ִ�г���'
      when   date(current timestamp)=date(b.begintime) and  b.flag=0 then '���'
      else  'δ֪'
end,
b.*
,a.FUNCTION_DESC
from  APP.SCH_CONTROL_TASK a,
      APP.SCH_CONTROL_RUNLOG  b,
      APP.SCH_CONTROL_MOGRPINFO  c
where  a.CONTROL_CODE=b.CONTROL_CODE  
   and a.MO_GROUP_CODE = c.MO_GROUP_CODE 
   and a.deal_time in (1,3)
   and MO_GROUP_DESC like '%һ��%'
   and (date(current timestamp)<>date(b.begintime)  and  b.flag=0 or b.flag=-2 )
order by c.sort_id,begintime asc
with ur;

