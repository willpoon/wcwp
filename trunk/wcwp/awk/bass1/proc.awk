#step1:copy 28*p  data
#step2:format data 0.000
#step3:upload unix
#step4:run the following program!
#step5:re-paste to excel (do some checksum)
#step6:load data into tmp-table
#step7:insert data into g_s_05001_month/g_s_05002_month
nawk 'BEGIN{FS=",";OFS="\t"}{print $1,$2,$4*1000,$3*1000}'  voice_201012.csv >fmt_voice_201012.csv
nawk 'BEGIN{FS=",";OFS="\t"}{print $5,$6,$8*1000,$7*1000}'  voice_201012.csv >>fmt_voice_201012.csv
nawk 'BEGIN{FS=",";OFS="\t"}{print $9,$10,$12*1000,$11*1000}'  voice_201012.csv >>fmt_voice_201012.csv
echo " \n">>fmt_voice_201012.csv
echo " \n">>fmt_voice_201012.csv
echo " \n">>fmt_voice_201012.csv
echo " \n">>fmt_voice_201012.csv
echo " \n">>fmt_voice_201012.csv
nawk 'BEGIN{FS=",";OFS="\t"}{if($14 != 0){print $14,$13,$16*1000,$15*1000}}'  voice_201012.csv >>fmt_voice_201012.csv
