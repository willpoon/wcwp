#Shell 计算明天和昨天日期的函数
#-
#返回月份的天数
get_mon_days()
{
        Y=`expr substr $1 1 4`
        M=`expr substr $1 5 2`

        r1=`expr $Y % 4`
        r2=`expr $Y % 100`
        r3=`expr $Y % 400`

        case $M in
        01|03|05|07|08|10|12) days=31;;
        04|06|09|11) days=30;;
        esac
        if [ $M -eq 02 ]
        then
                if [ r1 -eq 0 -a r2 -ne 0 -o r3 -eq 0 ]
                then
                        days=29
                else
                        days=28
                fi
        fi
        echo $days
}
#＃返回昨天日期
get_before_date()
{
 Y=`expr substr $1 1 4`
 M=`expr substr $1 5 2`
 D=`expr substr $1 7 2`
 YY=`expr $Y - 1`
 MM=`expr $M - 1`
 DD=`expr $D - 1`
 MM=`printf "%02d" $MM`
 DD=`printf "%02d" $DD`
 dd=$Y$MM
 dad=`get_mon_days $dd`
 be_date=$Y$M$DD
 if [ $D -eq 01 ]
 then
 if [ $M -ne 01 ]
 then
 be_date=$Y$MM$dad
 fi
 if [ $M -eq 01 ]
                then
                        be_date=$YY"1231"
                fi
   fi
        echo $be_date

}
#返回明天日期
get_next_date()
{
 Y=`expr substr $1 1 4`
 M=`expr substr $1 5 2`
 D=`expr substr $1 7 2`
 YY=`expr $Y + 1`
 MM=`expr $M + 1`
 DD=`expr $D + 1`
 MM=`printf "%02d" $MM`
 DD=`printf "%02d" $DD`
 r1=`expr $Y % 4`
 r2=`expr $Y % 100`
 r3=`expr $Y % 400`

        next_date=$Y$M$DD

        if [ $D -eq 30 ]
        then
                case $M in
                04|06|09|11) next_date=$Y$MM"01";;
                esac
        fi
        if [ $D -eq 31 ]
        then
                next_date=$Y$MM"01"
                case $M in
                12) next_date=$YY"0101";;
                esac
        fi
        if [ $M -eq 02 ]
        then
                if [ r1 -eq 0 -a r2 -ne 0 -o r3 -eq 0 ]
                then
                        if [ $D -eq 29 ]
                        then
                                next_date=$Y$MM"01"
                        fi
                else
                        if [ $D -eq 28 ]
                        then
                                next_date=$Y$MM"01"
                        fi
                fi
        fi
        echo $next_date
}

#本人在HP Unix下测试通过
