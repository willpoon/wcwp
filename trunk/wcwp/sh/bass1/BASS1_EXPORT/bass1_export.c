static char sqla_program_id[162] = 
{
 0,42,68,65,76,65,73,65,66,65,83,83,49,95,69,88,75,66,121,67,
 75,80,69,97,48,49,49,49,49,32,50,32,0,8,65,80,80,32,32,32,
 32,32,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
 0,0
};

#include "sqladef.h"

static struct sqla_runtime_info sqla_rtinfo = 
{{'S','Q','L','A','R','T','I','N'}, sizeof(wchar_t), 0, {' ',' ',' ',' '}};


static const short sqlIsLiteral   = SQL_IS_LITERAL;
static const short sqlIsInputHvar = SQL_IS_INPUT_HVAR;


#line 1 "bass1_export.sqc"
/************************************************************************************************************
*�ļ���:   bass1_export.sqc
*������:   ����
*����ʱ��: 2007-05-18
*��������: ʵ�ֽӿ������ļ����ӿ�У���ļ����Լ�ҵ��ָ�������ļ���ҵ��ָ��У���ļ��ĵ������ϴ�����
*��ע:     ִ�з���                 : bass1_export ģʽ��.����  YYYY-MM[-DD]
*          APP.G_UNIT_INFO        : �ӿڵ�Ԫ���ã�������ݱ���ȡ��������Ϣ
*          APP.G_RUNLOG           : ���򵼳���ɺ��ӿ���Ϣ������У������ظ�������Ὣ�ظ����ֶμ�1
*          APP.SCH_CONTROL_ALARM  ��������ִ���쳣������ڴ˱��в���澯��Ϣ�����������ɵ���ִ�У���ͬʱ�Ὣ 
*                                     APP.SCH_CONTROL_RUNLOG��Flag�ֶ���Ϊ�����־[-1]
*************************************************************************************************************/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <math.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include<dirent.h>

#include <sql.h>
#include <sqlenv.h>
#include <sqlda.h>
#include <sqlca.h>



/*
EXEC SQL INCLUDE SQLCA ;
*/

/* SQL Communication Area - SQLCA - structures and constants */
#include "sqlca.h"
struct sqlca sqlca;


#line 35 "bass1_export.sqc"



//�ӿ����ȵ�Ԫ����:�ֱ���գ��£���ʼ��
#define COARSE_DAY  0   
#define COARSE_MON  1 
#define COARSE_INI  9

//�Ƿ������ϴ��ļ�: �ֱ�Ϊ���ԣ�������
#define  PUT_REJECT 0
#define  PUT_MAY    1

//�Ƿ������ϴ����ļ����ֱ�Ϊ���ԣ�������
#define  PUT_EMPTY_REJECT 0
#define  PUT_EMPTY_MAY    1

//��ȡ���ݱ�־
#define  FLAG_INIT       "z"
#define  FLAG_ADD        "a"
#define  FLAG_NEW_ADD    "s"
#define  FLAG_FULL       "i"

//���������ļ����ͣ������ļ�,ҵ��ָ���ļ�
#define  DATA_TYPE 0
#define  BUSI_TYPE 1

//�Ƿ�ͨ����֤
#define NOT_PASS_CHECK 0
#define     PASS_CHECK 1

//�������ݿ�
int Connect_DB(char* rsDBName,char* rsUserName,char* rsPassWD);
//�Ͽ������ݵ�����
int Close_DB();
//ȡ�õ�ǰʱ�䣬��Ϊ�ļ�����ʱ��
char *get_filetime(void);
//ȡ���ļ��ļ�¼����
char *get_fileline(char *rsFileName);
//ȡ���ļ��Ĵ�С
char *get_filesize(char *rsFileName);
//ȡ���ļ���������
int get_num(char *rsDate,char *rsUnitcode,int rnCoarse,char *OutRnum);
//���ַ�������ָ��λ�����ַ������ո����
char *get_snum(char *rsVal,int rnlen);
//����У���ļ�
int create_verf(char *strDataFileNameDIR,char *strVerfFileNameDIR,char *strDataFileName,char *rsDate);
//���ر��Ĳ�ѯ�﷨
int create_select(char *rsTableName,char *rsCreator,char *rsDate,char *OutSQL);
//�������ݵ��ļ���
int export_data(char *rsFileName,char *rsSQL,char *OutErrorInfo,int &OutRecNum);
//ȡ�������͵����ڣ�ͬʱ�����ڸ�ʽ������
int get_digit_date(char *rsDate,char *OutDate,char *OutErrorInfo);
//��¼������־
int write_log(char *rsDate,char *rsUnitCode,int nCoarse,char *rsDataFileName,char *rsVerfFileName,char *OutErrorInfo);
//����澯��Ϣ
int insert_alarm(char *rsDealCode,char *rsVal1,char *rsVal2,char *rsVal3,char *rsErrorInfo);
//ȡ�õ������������Ϣ
int get_unit_info(char *rsTableName,char *OutUnitCode,char *OutFlag,int &OutCoarse,int &OutIfPut,int &OutIfEmptyPut,char *OutErrorInfo);
//ȡ������Ϣ
void ReadString(const char *IniFile,char *Result,const char *Section,const char *Name,const char *Default);
//ȡ��֤��־
int get_check_flag(char *rsDate,char *rsUnitCode,int nCoarse);
//�ж��Ƿ����������ļ��Ƿ�ȫ��ͨ����֤�������ϴ�ҵ��ָ���ļ�֮ǰ���ж�,ֻ�����ļ��Ƿ�ȫ��ͨ����֤�����ϴ�ҵ��ָ���ļ���
int if_all_datafile_pass(int nCoarse);

int main(int argc,char *argv[])
{
     char strCreatorName[1024];            //ģʽ
     char strTableName[1024];              //����
     char strCreatorTableName[1024];       //��ģʽ�ı���
     char strExportDate[1024];             //���ݵ�������
     char strUnitCode[1024];               //�ӿڴ���  
     char strFlag[1024];                   //��ʼ�����������ӣ�ȫ����־
     int  nCoarse;                         //�ӿ�����
     int  nIfPut;                          //�Ƿ������ϴ�
     int  nIfEmptyPut;                     //�Ƿ������ϴ����ļ� 
     char strDealCode[1024];               //������
     char strVal1[1024];                   //����ֵ1
     char strVal2[1024];                   //����ֵ2
     char strVal3[1024];                   //����ֵ3
     char  *program;                       //������
     int  nImportType;                     //���������ļ�����
     char strRnum[1024];                   //���ʹ���
     char strDataFileName[1024];           //����Ŀ¼�������ļ���
     char strVerfFileName[1024];           //����Ŀ¼��У���ļ���
     char strExecHome[1024];               //ִ�г���Ŀ¼<�����ļ����ϴ���FTP SHELL���򶼷�����Ŀ¼��>
     char strDataHome[1024];               //��������Ŀ¼<�������ݵĸ�Ŀ¼>
     char strExportDIR[1024];              //��������Ŀ¼<strDataHome+"/"+export> 
     char strBakDIR[1024];                 //��������Ŀ¼<strDataHome+"/"+export_YYYYMM>
     char strDataFileNameExportDIR[1024];  //������Ŀ¼�������ļ���[strExportDIR+"/"+strDataFileName]  
     char strVerfFileNameExportDIR[1024];  //������Ŀ¼��У���ļ���[strExportDIR+"/"+strVerfFileName] 
     char strSQL[10240];                   //��ѯSQL
     char strUserName[1024];               //���ݿ��û���
     char strPassWord[1024];               //���ݿ�����
     char strDBName[1024];                 //���ݿ���
     char strErrorInfo[2048];              //�澯������Ϣ
     char strCommand[1024];                //������� 
     int  nExportNum=0;                    //������¼��

     //�����Ϸ����ж�
     if(argc != 3)
     {
    	printf("Invalid Parameter1\n");
    	printf("Usage: bass1_export tablename yyyy-mm-dd[yyyy-mm]\n");
    	exit(0);
     }
     if ( strcasecmp("bass1.g_bus_check_all_day",argv[1])!=0  &&  strcasecmp("bass1.g_bus_check_bill_month",argv[1])!=0 ){
            nImportType = DATA_TYPE ;
            if( strlen(argv[1])!=6+15 && strlen(argv[1])!=6+13  )
            {
    	        printf("Invalid Parameter2\n");
    	        printf("Usage: bass1_export creator.tablename yyyy-mm-dd[yyyy-mm]\n");
    	        exit(0);
            }
     }else{
            nImportType = BUSI_TYPE ;  
     }      
     if((strlen(argv[2])!=7)&&(strlen(argv[2])!=10))
     {
    	printf("Invalid Parameter3\n");
    	printf("Usage: bass1_export creator.tablename yyyy-mm-dd[yyyy-mm]\n");
    	exit(0);
     }
     
     //ȡ��û��Ŀ¼�ĳ�����
     program = strrchr(argv[0], '/');
     if ( program )  program++;
     else  program = argv[0]; 
     
     //����ִ�г�������Ŀ¼,�����ƶ���ע���޸Ĵ�ֵ
     sprintf(strExecHome,"/bassapp/backapp/bin/bass1_export");
     
     //ȡ���ݿ��û��������룬���ݿ�������������Ŀ¼
     sprintf(strCommand,"%s/bass1_export.ini",strExecHome);
     ReadString(strCommand,strUserName,"DATABASE","USER_ID","*");
     ReadString(strCommand,strPassWord,"DATABASE","PASS_WD","*");
     ReadString(strCommand,strDBName  ,"DATABASE","DB_NAME","*");
     ReadString(strCommand,strDataHome,"OTHER"   ,"DATA_HOME","*");
     printf("�û���:%s,����:%s,���ݿ���:%s,�����ļ�Ŀ¼:%s\n",strUserName,strPassWord,strDBName,strDataHome);
     
     if ( Connect_DB(strDBName,strUserName,strPassWord)!=0 ){
         printf("�������ݿ���������˳�!\n");
    	 exit(0);
     }
     
     //���ù�������������
     strcpy(strDealCode,program);
     strcpy(strVal1,argv[1]);
     strcpy(strVal2,argv[2]);
     strcpy(strVal3,"-");
     
     //ȡ��ģʽ��������������
     memset(strCreatorName,'\0'  ,1024  );
     memset(strTableName,'\0'  ,1024  );
     strncpy(strCreatorName,  argv[1]  ,5                 );
     strncpy(strTableName,argv[1]+6,strlen(argv[1])-6 );
     strcpy(strCreatorTableName,argv[1]);
     if ( get_digit_date(argv[2],strExportDate,strErrorInfo)!=0 ){
         printf("���ڲ�������:%s\n",strErrorInfo);
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�������ڲ���");
         Close_DB();
    	 exit(0);
     }
     
     //��������Ŀ¼������Ŀ¼
     sprintf(strExportDIR,"%s/export",strDataHome)              ; sprintf(strCommand,"mkdir %s",strExportDIR); system(strCommand);
     sprintf(strBakDIR,"%s/export_%s",strDataHome,strExportDate); sprintf(strCommand,"mkdir %s",strBakDIR   ); system(strCommand);
     
     //ȡ�ӿ������Ϣ�����ж��Ƿ���ȷ
     if ( get_unit_info(strCreatorTableName,strUnitCode,strFlag,nCoarse,nIfPut,nIfEmptyPut,strErrorInfo)!=0 ){
         printf("ȡ�ӿڴ�����Ϣ����:%s\n",strErrorInfo);
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"ȡ�ӿڴ�����Ϣ����");
         Close_DB();
    	 exit(0);
     }
     if ( nCoarse!=COARSE_DAY && nCoarse!=COARSE_MON && nCoarse!=COARSE_INI ){
         printf("���ȴ������\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"���ȴ������");
         Close_DB();
    	 exit(0);
     }
     if ( nCoarse==COARSE_DAY && strlen(strExportDate)!=8 ){
         printf("�˱�Ϊ������,���ڲ�������ȷ\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�˱�Ϊ������,���ڲ�������ȷ");
         Close_DB();
    	 exit(0);
     }
     if ( nCoarse==COARSE_MON && strlen(strExportDate)!=6 ){
         printf("�˱�Ϊ������,���ڲ�������ȷ\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�˱�Ϊ������,���ڲ�������ȷ");
         Close_DB();
    	 exit(0);
     }
     if ( strcmp(strFlag,FLAG_INIT)==0 && strcmp(strFlag,FLAG_ADD)==0 && strcmp(strFlag,FLAG_NEW_ADD)==0 && strcmp(strFlag,FLAG_FULL)==0 ){
         printf("��ȡ��ʽ�������:%s\n",strFlag);
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"��ȡ��ʽ�������");
         Close_DB(); 
    	 exit(0);
     }       
     if ( nIfPut!=PUT_REJECT && nIfPut!=PUT_MAY ){
         printf("�ϴ���־�������\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�ϴ���־�������");
         Close_DB(); 
    	 exit(0);
     }
     if ( nIfPut==PUT_REJECT ){
         printf("�˽ӿڲ���Ҫ�ϴ�\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�˽ӿڲ���Ҫ�ϴ�");
         Close_DB(); 
    	 exit(0);
     }
     if ( nIfEmptyPut!=PUT_EMPTY_REJECT && nIfEmptyPut!=PUT_EMPTY_MAY ){
         printf("�ϴ����ļ���־�������\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�ϴ����ļ���־�������");
         Close_DB(); 
    	 exit(0);     
     }
     if ( get_num(strExportDate,strUnitCode,nCoarse,strRnum)!=0 ){
         printf("ȡ�ش�����ֵ����\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"ȡ�ش�����ֵ����");
         Close_DB(); 
    	 exit(0);     
     }
     if ( atoi(strRnum)>90 ){
         printf("ȡ�ش�����ֵ̫��\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"ȡ�ش�����ֵ̫��");
         Close_DB(); 
    	 exit(0);     
     }
     
     //�ж��ļ��Ƿ��Ѿ��ϴ���ͨ����֤
     if ( get_check_flag(strExportDate,strUnitCode,nCoarse)==PASS_CHECK ){
         printf("�ӿ��Ѿ��ϴ���ͨ����֤,�����ظ��ϴ�\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�ӿ��Ѿ��ϴ���ͨ����֤,�����ظ��ϴ�");
         Close_DB(); 
    	 exit(0);     
     }
/*     
     //�ж���ҵ��ָ���ļ��Ƿ�����ϴ�
     if ( strcmp(strUnitCode,"00000")==0 ){
         if ( if_all_datafile_pass(COARSE_DAY) < 0 ){
              printf("�ϴ����������ļ�û��ȫ��ͨ����֤,�ݲ����ϴ���ҵ��ָ���ļ�\n");
              insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�ϴ����������ļ�û��ȫ��ͨ����֤,�ݲ����ϴ���ҵ��ָ���ļ�");
              Close_DB(); 
    	      exit(0);     
         }     
     } 
     
     //�ж���ҵ��ָ���ļ��Ƿ�����ϴ�
     if ( strcmp(strUnitCode,"99999")==0 ){
         if ( if_all_datafile_pass(COARSE_MON) < 0 ){
              printf("�ϴ����������ļ�û��ȫ��ͨ����֤,�ݲ����ϴ���ҵ��ָ���ļ�\n");
              insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�ϴ����������ļ�û��ȫ��ͨ����֤,�ݲ����ϴ���ҵ��ָ���ļ�");
              Close_DB(); 
    	      exit(0);     
         }     
     } 
*/
     //�����ļ���
     if ( nImportType==DATA_TYPE ){
         sprintf(strDataFileName,"%s_13100_%s_%s_%s_001.dat",strFlag,strExportDate,strUnitCode,strRnum);
         sprintf(strVerfFileName,"%s_13100_%s_%s_%s.verf"  ,strFlag,strExportDate,strUnitCode,strRnum);
     }else{
         if ( strcasecmp(strTableName,"g_bus_check_all_day")==0 ){
               sprintf(strDataFileName,"s_13100_%s_00000_%s_001.dat",strExportDate,strRnum);
               sprintf(strVerfFileName,"s_13100_%s_00000_%s.verf"   ,strExportDate,strRnum);
         }else{
               sprintf(strDataFileName,"s_13100_%s_99999_%s_001.dat",strExportDate,strRnum);
               sprintf(strVerfFileName,"s_13100_%s_99999_%s.verf"   ,strExportDate,strRnum);
         }
     }  
     sprintf(strDataFileNameExportDIR,"%s/%s",strExportDIR,strDataFileName);  
     sprintf(strVerfFileNameExportDIR,"%s/%s",strExportDIR,strVerfFileName); 
     
     //���ɵ��������﷨
     if ( create_select(strTableName,strCreatorName,strExportDate,strSQL)!=0 ){
         printf("���ɲ�ѯ�﷨����\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"���ɲ�ѯ�﷨����");
         Close_DB(); 
    	 exit(0);     
     }
     printf("���ɲ�ѯ�﷨�ɹ�!\n");
     
     //�������ݵ��ļ���
     if ( export_data(strDataFileNameExportDIR,strSQL,strErrorInfo,nExportNum)!=0 ){
         printf("���������ļ�������%s\n",strErrorInfo);
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"���������ļ�����");
         Close_DB(); 
    	 exit(0);     
     } 
     printf("���������ļ��ɹ�[%s]!\n",strSQL);
     
     //��û�����ݼ�¼�����Ҳ����ϴ����ļ����򱨴�
     if ( nIfEmptyPut==PUT_EMPTY_REJECT && nExportNum==0 ){
         printf("�����ϴ����ļ�\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"�����ϴ����ļ�");
         Close_DB(); 
    	 exit(0);     
     } 
     
     //����У���ļ�
     if ( create_verf(strDataFileNameExportDIR,strVerfFileNameExportDIR,strDataFileName,strExportDate)!=0 ){
         printf("����У���ļ�����\n");
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"����У���ļ�����");
         Close_DB(); 
    	 exit(0);     
     }
     printf("������У���ļ��ɹ�!\n");
     
     //�ϴ��ļ�
     sprintf(strCommand,"%s/ftp_bass1_export.sh %s",strExecHome,strDataFileNameExportDIR);system(strCommand);
     sprintf(strCommand,"%s/ftp_bass1_export.sh %s",strExecHome,strVerfFileNameExportDIR);system(strCommand);
     printf("�ϴ��ļ�!\n");
     
     //�����ļ�
     sprintf(strCommand,"mv %s %s",strDataFileNameExportDIR,strBakDIR);system(strCommand);
     sprintf(strCommand,"mv %s %s",strVerfFileNameExportDIR,strBakDIR);system(strCommand);
     printf("�����ļ�!\n");
     
     //��¼��־
     if ( write_log(strExportDate,strUnitCode,nCoarse,strDataFileName,strVerfFileName,strErrorInfo)!=0 ){
         printf("��¼������־ʧ��:%s\n",strErrorInfo);
         insert_alarm(strDealCode,strVal1,strVal2,strVal3,"��¼������־ʧ��,���ֹ��������ݵ�����־[APP.G_RUNLOG]");
         Close_DB(); 
    	 exit(0);     
     }
     printf("��¼��־�ɹ�!\n");
     Close_DB();
     return 0;
}

//ȡ�õ�ǰʱ�䣬��Ϊ�ļ�����ʱ��
char *get_filetime(void)
{
    struct tm *loctime;
    time_t   curTime;
    char    *timeBuff;
    timeBuff=(char *)malloc(20);
    curTime=time(NULL);
    loctime=localtime(&curTime);
    strftime(timeBuff,20,"%Y%m%d%H%M%S",loctime);
    return timeBuff;
}

//ȡ���ļ��ļ�¼����
char *get_fileline(char *rsFileName)
{
    char *sFileline;
    char strCommand[200];
    char buf[1024];
    char tmp[1024];
    
    sFileline=(char *)malloc(20);
    sprintf(strCommand,"wc -l %s",rsFileName);
    
    FILE *fp;
    fp=popen(strCommand,"r");
    if(fp!=NULL)
    {
    	while(fgets(buf,1024,fp)!=NULL)
    	{
    	    sscanf(buf,"%s %s",sFileline,tmp);
    	}
    }else{
    	sprintf(sFileline,"%s","0");
    }	
    pclose(fp);
    return sFileline;
}

//ȡ���ļ��Ĵ�С
char *get_filesize(char *rsFileName)
{
    char *sFilesize;
    char strCommand[200];
    char buf[1024];
    char s1[100];
    char s2[100];
    char s3[100];
    char s4[100];
    
    sFilesize=(char *)malloc(20);
    sprintf(strCommand,"ls -l %s",rsFileName);
    
    FILE *fp;
    fp=popen(strCommand,"r");
    if(fp!=NULL)
    {
    	while(fgets(buf,1024,fp)!=NULL)
    	{
    	    sscanf(buf,"%s %s %s %s %s",s1,s2,s3,s4,sFilesize);
    	}
    }else{
    	sprintf(sFilesize,"%s","0");
    }	
    pclose(fp);
    return sFilesize;
}

//ȡ���ļ���������
int get_num(char *rsDate,char *rsUnitcode,int rnCoarse,char *OutRnum)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 437 "bass1_export.sqc"

    sqlint16 nRowNum;
    sqlint32 nExportNum;
    sqlint32 nCoarse;
    char strDate[20];
    char strUnitcode[10];

/*
EXEC SQL END DECLARE SECTION;
*/

#line 443 "bass1_export.sqc"

    
    strcpy(strDate,rsDate);
    strcpy(strUnitcode,rsUnitcode);
    nCoarse = rnCoarse;

    
/*
EXEC SQL select count(*) into :nRowNum 
             from APP.G_RUNLOG 
             where time_id=integer(:strDate) and unit_code=:strUnitcode and coarse=:nCoarse;
*/

{
#line 451 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 451 "bass1_export.sqc"
  sqlaaloc(2,3,1,0L);
    {
      struct sqla_setdata_list sql_setdlist[3];
#line 451 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 20;
#line 451 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)strDate;
#line 451 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 451 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 10;
#line 451 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)strUnitcode;
#line 451 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 451 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 496; sql_setdlist[2].sqllen = 4;
#line 451 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)&nCoarse;
#line 451 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 451 "bass1_export.sqc"
      sqlasetdata(2,0,3,sql_setdlist,0L,0L);
    }
#line 451 "bass1_export.sqc"
  sqlaaloc(3,1,2,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 451 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 500; sql_setdlist[0].sqllen = 2;
#line 451 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)&nRowNum;
#line 451 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 451 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 451 "bass1_export.sqc"
  sqlacall((unsigned short)24,1,2,3,0L);
#line 451 "bass1_export.sqc"
  sqlastop(0L);
}

#line 451 "bass1_export.sqc"

    if( sqlca.sqlcode==0 ){
        if ( nRowNum==0 ){ 
    	     sprintf(OutRnum,"%s","00");
    	     return 0;
    	}    
    }else{
        return sqlca.sqlcode;  
    }
  
    
/*
EXEC SQL select export_num+1 into :nExportNum 
             from APP.G_RUNLOG 
             where time_id=integer(:strDate) and  unit_code=:strUnitcode and coarse=:nCoarse;
*/

{
#line 463 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 463 "bass1_export.sqc"
  sqlaaloc(2,3,3,0L);
    {
      struct sqla_setdata_list sql_setdlist[3];
#line 463 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 20;
#line 463 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)strDate;
#line 463 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 463 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 10;
#line 463 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)strUnitcode;
#line 463 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 463 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 496; sql_setdlist[2].sqllen = 4;
#line 463 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)&nCoarse;
#line 463 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 463 "bass1_export.sqc"
      sqlasetdata(2,0,3,sql_setdlist,0L,0L);
    }
#line 463 "bass1_export.sqc"
  sqlaaloc(3,1,4,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 463 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 496; sql_setdlist[0].sqllen = 4;
#line 463 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)&nExportNum;
#line 463 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 463 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 463 "bass1_export.sqc"
  sqlacall((unsigned short)24,2,2,3,0L);
#line 463 "bass1_export.sqc"
  sqlastop(0L);
}

#line 463 "bass1_export.sqc"

    if( sqlca.sqlcode==0){
        if ( nExportNum<=9 ) 
     	     sprintf(OutRnum,"0%d",nExportNum);
     	else 
     	     sprintf(OutRnum,"%d",nExportNum);     
    }else{
        return sqlca.sqlcode;
    }
  
    return 0;
}

//���ַ�������ָ��λ�����ַ������ո����
char *get_snum(char *rsVal,int rnlen)
{
    char *sVal = (char *)malloc(rnlen+1);
    int   nLen = strlen(rsVal);
    
    sprintf(sVal,"%s",rsVal);
    for(int i=0;i<rnlen-nLen;i++){
    	sprintf(sVal,"%s ",sVal);
    }
    sVal[rnlen]='\0';
    return sVal;
}

//����У���ļ�
int create_verf(char *strDataFileNameDIR,char *strVerfFileNameDIR,char *strDataFileName,char *rsDate)
{
    char sFilesize[20];
    char sFileline[20];
    char sFiletime[15];
    
    sprintf(sFiletime,get_filetime());
    sprintf(sFilesize,get_filesize(strDataFileNameDIR));
    sprintf(sFileline,get_fileline(strDataFileNameDIR));
    
    FILE *fp;
    if((fp=fopen(strVerfFileNameDIR,"w"))==NULL) return -1;
    fprintf(fp,"%s",get_snum(strDataFileName,40));
    fprintf(fp,"%s",get_snum(sFilesize,20));
    fprintf(fp,"%s",get_snum(sFileline,20));
    fprintf(fp,"%s",get_snum(rsDate,8));
    fprintf(fp,"%s",get_snum(sFiletime,14));
    fprintf(fp,"%s","\r\n");
    fclose(fp);
    
    return 0;
}

//���ر��Ĳ�ѯ�﷨
int create_select(char *rsTableName,char *rsCreator,char *rsDate,char *OutSQL)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 517 "bass1_export.sqc"

    sqlint32 nNum=0;
    char sTableName[1024];
    char sCreator[1024];
    char sCol[1024];

/*
EXEC SQL END DECLARE SECTION;
*/

#line 522 "bass1_export.sqc"


    strcpy(sTableName,rsTableName);
    strcpy(sCreator,rsCreator);
    OutSQL[0]='\0'; 
      
    
/*
EXEC SQL select count(*) into :nNum 
             from SYSIBM.SYSCOLUMNS 
             where upper(tbname)=upper(:sTableName) and upper(tbcreator)=upper(:sCreator);
*/

{
#line 530 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 530 "bass1_export.sqc"
  sqlaaloc(2,2,5,0L);
    {
      struct sqla_setdata_list sql_setdlist[2];
#line 530 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 1024;
#line 530 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)sTableName;
#line 530 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 530 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 1024;
#line 530 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)sCreator;
#line 530 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 530 "bass1_export.sqc"
      sqlasetdata(2,0,2,sql_setdlist,0L,0L);
    }
#line 530 "bass1_export.sqc"
  sqlaaloc(3,1,6,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 530 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 496; sql_setdlist[0].sqllen = 4;
#line 530 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)&nNum;
#line 530 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 530 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 530 "bass1_export.sqc"
  sqlacall((unsigned short)24,3,2,3,0L);
#line 530 "bass1_export.sqc"
  sqlastop(0L);
}

#line 530 "bass1_export.sqc"

    if ( sqlca.sqlcode!= 0 ) return sqlca.sqlcode;          
    if ( nNum==0 ) return -1;
    
    
/*
EXEC SQL DECLARE c1 CURSOR FOR select name 
                                   from SYSIBM.SYSCOLUMNS 
                                   where upper(tbname)=upper(:sTableName) and upper(tbcreator)=upper(:sCreator) 
                                   order by colno;
*/

#line 537 "bass1_export.sqc"

    
/*
EXEC SQL OPEN c1;
*/

{
#line 538 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 538 "bass1_export.sqc"
  sqlaaloc(2,2,7,0L);
    {
      struct sqla_setdata_list sql_setdlist[2];
#line 538 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 1024;
#line 538 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)sTableName;
#line 538 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 538 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 1024;
#line 538 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)sCreator;
#line 538 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 538 "bass1_export.sqc"
      sqlasetdata(2,0,2,sql_setdlist,0L,0L);
    }
#line 538 "bass1_export.sqc"
  sqlacall((unsigned short)26,4,2,0,0L);
#line 538 "bass1_export.sqc"
  sqlastop(0L);
}

#line 538 "bass1_export.sqc"

    nNum=0;
    for(;;)
    {
    	
/*
EXEC SQL fetch c1 into :sCol;
*/

{
#line 542 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 542 "bass1_export.sqc"
  sqlaaloc(3,1,8,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 542 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 1024;
#line 542 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)sCol;
#line 542 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 542 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 542 "bass1_export.sqc"
  sqlacall((unsigned short)25,4,0,3,0L);
#line 542 "bass1_export.sqc"
  sqlastop(0L);
}

#line 542 "bass1_export.sqc"

    	if(sqlca.sqlcode==100) break;
    	if(sqlca.sqlcode<0){
    		
/*
EXEC SQL CLOSE c1;
*/

{
#line 545 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 545 "bass1_export.sqc"
  sqlacall((unsigned short)20,4,0,0,0L);
#line 545 "bass1_export.sqc"
  sqlastop(0L);
}

#line 545 "bass1_export.sqc"

    		return -2;
    	}
    	nNum++;
    	if( nNum==1 )
            continue;
    	else if( nNum==2 )
    	    sprintf(OutSQL,"select %s",sCol);
    	else
    	    sprintf(OutSQL,"%s||%s",OutSQL,sCol);
    }
    
/*
EXEC SQL CLOSE c1;
*/

{
#line 556 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 556 "bass1_export.sqc"
  sqlacall((unsigned short)20,4,0,0,0L);
#line 556 "bass1_export.sqc"
  sqlastop(0L);
}

#line 556 "bass1_export.sqc"

    
    sprintf(OutSQL,"%s from %s.%s where time_id=%s",OutSQL,rsCreator,rsTableName,rsDate);
    
    return 0;
}

//�������ݵ��ļ���
int export_data(char *rsFileName,char *rsSQL,char *OutErrorInfo,int &OutRecNum)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 566 "bass1_export.sqc"

    char sSQL[10240];
    char sData[10240];

/*
EXEC SQL END DECLARE SECTION;
*/

#line 569 "bass1_export.sqc"

    
    char buf[1024];
    OutRecNum=0;
    sprintf(sSQL,rsSQL);
   
    FILE *fp1;
    if((fp1=fopen(rsFileName,"w"))==NULL)
    {
    	sprintf(OutErrorInfo,"export_data:open %s file err!\n",rsFileName);
    	return -1;
    }
    
/*
EXEC SQL prepare c2 from :sSQL;
*/

{
#line 581 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 581 "bass1_export.sqc"
  sqlastls(0,sSQL,0L);
#line 581 "bass1_export.sqc"
  sqlacall((unsigned short)27,5,0,0,0L);
#line 581 "bass1_export.sqc"
  sqlastop(0L);
}

#line 581 "bass1_export.sqc"

    
/*
EXEC SQL declare c2 cursor for c2;
*/

#line 582 "bass1_export.sqc"

    
/*
EXEC SQL    OPEN c2;
*/

{
#line 583 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 583 "bass1_export.sqc"
  sqlacall((unsigned short)26,5,0,0,0L);
#line 583 "bass1_export.sqc"
  sqlastop(0L);
}

#line 583 "bass1_export.sqc"

    if( sqlca.sqlcode<0 )
    {
        sprintf(OutErrorInfo,"export_data:open cursor err[sqlstate=%c%c%c%c%c,%s]\n",sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4],sSQL);
        
/*
EXEC SQL CLOSE c2;
*/

{
#line 587 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 587 "bass1_export.sqc"
  sqlacall((unsigned short)20,5,0,0,0L);
#line 587 "bass1_export.sqc"
  sqlastop(0L);
}

#line 587 "bass1_export.sqc"

        return -2;
    }
    for(;;)
    {
    	
/*
EXEC SQL fetch c2 into :sData;
*/

{
#line 592 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 592 "bass1_export.sqc"
  sqlaaloc(3,1,9,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 592 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 10240;
#line 592 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)sData;
#line 592 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 592 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 592 "bass1_export.sqc"
  sqlacall((unsigned short)25,5,0,3,0L);
#line 592 "bass1_export.sqc"
  sqlastop(0L);
}

#line 592 "bass1_export.sqc"

    	if(sqlca.sqlcode==100) break;
    	if(sqlca.sqlcode<0)
    	{
            sprintf(OutErrorInfo,"export_data:open cursor err[sqlstate=%c%c%c%c%c,%s]\n",sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4],sSQL);
    	    
/*
EXEC SQL CLOSE c2;
*/

{
#line 597 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 597 "bass1_export.sqc"
  sqlacall((unsigned short)20,5,0,0,0L);
#line 597 "bass1_export.sqc"
  sqlastop(0L);
}

#line 597 "bass1_export.sqc"

    	    return -3;
    	}
    	OutRecNum++;
    	sprintf(buf,"%d",OutRecNum);
    	strcpy(buf,get_snum(buf,8));
    	fprintf(fp1,"%s%s\r\n",buf,sData);
    }
    
/*
EXEC SQL CLOSE c2;
*/

{
#line 605 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 605 "bass1_export.sqc"
  sqlacall((unsigned short)20,5,0,0,0L);
#line 605 "bass1_export.sqc"
  sqlastop(0L);
}

#line 605 "bass1_export.sqc"

    fclose(fp1);
    return 0;
}

int get_digit_date(char *rsDate,char *OutDate,char *OutErrorInfo)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 612 "bass1_export.sqc"

    char in_Date[20];
    char out_Date[20];
    sqlint32  nCount=-1; 

/*
EXEC SQL END DECLARE SECTION;
*/

#line 616 "bass1_export.sqc"


     sprintf(in_Date,rsDate);
     if ( strlen(in_Date)==7 ) 
     {
          
/*
EXEC SQL  select  substr(replace(char(date(:in_Date||'-01'),ISO),'-',''),1,6)  into :out_Date from sysibm.sysdummy1 with ur;
*/

{
#line 621 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 621 "bass1_export.sqc"
  sqlaaloc(2,1,10,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 621 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 20;
#line 621 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_Date;
#line 621 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 621 "bass1_export.sqc"
      sqlasetdata(2,0,1,sql_setdlist,0L,0L);
    }
#line 621 "bass1_export.sqc"
  sqlaaloc(3,1,11,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 621 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 20;
#line 621 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)out_Date;
#line 621 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 621 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 621 "bass1_export.sqc"
  sqlacall((unsigned short)24,6,2,3,0L);
#line 621 "bass1_export.sqc"
  sqlastop(0L);
}

#line 621 "bass1_export.sqc"

     }else{
          
/*
EXEC SQL  select  replace(char(date(:in_Date),ISO),'-','') into :out_Date from sysibm.sysdummy1 with ur;
*/

{
#line 623 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 623 "bass1_export.sqc"
  sqlaaloc(2,1,12,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 623 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 20;
#line 623 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_Date;
#line 623 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 623 "bass1_export.sqc"
      sqlasetdata(2,0,1,sql_setdlist,0L,0L);
    }
#line 623 "bass1_export.sqc"
  sqlaaloc(3,1,13,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 623 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 20;
#line 623 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)out_Date;
#line 623 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 623 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 623 "bass1_export.sqc"
  sqlacall((unsigned short)24,7,2,3,0L);
#line 623 "bass1_export.sqc"
  sqlastop(0L);
}

#line 623 "bass1_export.sqc"

     }          
     if ( sqlca.sqlcode!=0 ) 
         sprintf(OutErrorInfo,"����%s��ʽ����:%c%c%c%c%c\n",rsDate,sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4]);
     else           
         sprintf(OutDate,out_Date);  
     return sqlca.sqlcode;
}

int write_log(char *rsDate,char *rsUnitCode,int nCoarse,char *rsDataFileName,char *rsVerfFileName,char *OutErrorInfo)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 634 "bass1_export.sqc"

    char      in_ExportDate[20];   
    char      in_UnitCode[20];
    sqlint32  in_Coarse;
    char      in_DataFileName[1024];
    char      in_VerfFileName[1024];
    sqlint32  nRowLog; 

/*
EXEC SQL END DECLARE SECTION;
*/

#line 641 "bass1_export.sqc"

     
    sprintf(in_ExportDate,rsDate);
    sprintf(in_UnitCode,rsUnitCode);
    in_Coarse = nCoarse ;
    sprintf(in_DataFileName,rsDataFileName);
    sprintf(in_VerfFileName,rsVerfFileName); 
    
    
/*
EXEC SQL select count(*) into :nRowLog 
             from APP.G_RUNLOG 
             where time_id=integer(:in_ExportDate) and unit_code=:in_UnitCode and coarse=:in_Coarse ;
*/

{
#line 651 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 651 "bass1_export.sqc"
  sqlaaloc(2,3,14,0L);
    {
      struct sqla_setdata_list sql_setdlist[3];
#line 651 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 20;
#line 651 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_ExportDate;
#line 651 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 651 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 20;
#line 651 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)in_UnitCode;
#line 651 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 651 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 496; sql_setdlist[2].sqllen = 4;
#line 651 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)&in_Coarse;
#line 651 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 651 "bass1_export.sqc"
      sqlasetdata(2,0,3,sql_setdlist,0L,0L);
    }
#line 651 "bass1_export.sqc"
  sqlaaloc(3,1,15,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 651 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 496; sql_setdlist[0].sqllen = 4;
#line 651 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)&nRowLog;
#line 651 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 651 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 651 "bass1_export.sqc"
  sqlacall((unsigned short)24,8,2,3,0L);
#line 651 "bass1_export.sqc"
  sqlastop(0L);
}

#line 651 "bass1_export.sqc"

    if ( sqlca.sqlcode==0 ){
         if ( nRowLog==0 ){
               
/*
EXEC SQL insert into APP.G_RUNLOG(time_id,unit_code,coarse,export_time,export_num,return_flag,data_file,verf_file)
                        values(integer(:in_ExportDate),:in_UnitCode,:in_Coarse,current timestamp,0,0,:in_DataFileName,:in_VerfFileName);
*/

{
#line 655 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 655 "bass1_export.sqc"
  sqlaaloc(2,5,16,0L);
    {
      struct sqla_setdata_list sql_setdlist[5];
#line 655 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 20;
#line 655 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_ExportDate;
#line 655 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 655 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 20;
#line 655 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)in_UnitCode;
#line 655 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 655 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 496; sql_setdlist[2].sqllen = 4;
#line 655 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)&in_Coarse;
#line 655 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 655 "bass1_export.sqc"
      sql_setdlist[3].sqltype = 460; sql_setdlist[3].sqllen = 1024;
#line 655 "bass1_export.sqc"
      sql_setdlist[3].sqldata = (void*)in_DataFileName;
#line 655 "bass1_export.sqc"
      sql_setdlist[3].sqlind = 0L;
#line 655 "bass1_export.sqc"
      sql_setdlist[4].sqltype = 460; sql_setdlist[4].sqllen = 1024;
#line 655 "bass1_export.sqc"
      sql_setdlist[4].sqldata = (void*)in_VerfFileName;
#line 655 "bass1_export.sqc"
      sql_setdlist[4].sqlind = 0L;
#line 655 "bass1_export.sqc"
      sqlasetdata(2,0,5,sql_setdlist,0L,0L);
    }
#line 655 "bass1_export.sqc"
  sqlacall((unsigned short)24,9,2,0,0L);
#line 655 "bass1_export.sqc"
  sqlastop(0L);
}

#line 655 "bass1_export.sqc"
  
               if ( sqlca.sqlcode!=0 )
               {
                  sprintf(OutErrorInfo,"������־��¼ʧ��:%c%c%c%c%c\n",sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4]);
                  return sqlca.sqlcode;
               }   
         }else{
               
/*
EXEC SQL  update APP.G_RUNLOG 
                         set export_num=export_num+1,
                              data_file=:in_DataFileName,
                              verf_file=:in_VerfFileName  
                         where time_id=integer(:in_ExportDate) and unit_code=:in_UnitCode and coarse=:in_Coarse ;
*/

{
#line 666 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 666 "bass1_export.sqc"
  sqlaaloc(2,5,17,0L);
    {
      struct sqla_setdata_list sql_setdlist[5];
#line 666 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 1024;
#line 666 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_DataFileName;
#line 666 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 666 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 1024;
#line 666 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)in_VerfFileName;
#line 666 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 666 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 460; sql_setdlist[2].sqllen = 20;
#line 666 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)in_ExportDate;
#line 666 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 666 "bass1_export.sqc"
      sql_setdlist[3].sqltype = 460; sql_setdlist[3].sqllen = 20;
#line 666 "bass1_export.sqc"
      sql_setdlist[3].sqldata = (void*)in_UnitCode;
#line 666 "bass1_export.sqc"
      sql_setdlist[3].sqlind = 0L;
#line 666 "bass1_export.sqc"
      sql_setdlist[4].sqltype = 496; sql_setdlist[4].sqllen = 4;
#line 666 "bass1_export.sqc"
      sql_setdlist[4].sqldata = (void*)&in_Coarse;
#line 666 "bass1_export.sqc"
      sql_setdlist[4].sqlind = 0L;
#line 666 "bass1_export.sqc"
      sqlasetdata(2,0,5,sql_setdlist,0L,0L);
    }
#line 666 "bass1_export.sqc"
  sqlacall((unsigned short)24,10,2,0,0L);
#line 666 "bass1_export.sqc"
  sqlastop(0L);
}

#line 666 "bass1_export.sqc"

               if ( sqlca.sqlcode!=0 )
               {
                  sprintf(OutErrorInfo,"������־��¼ʧ��:%c%c%c%c%c\n",sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4]);
                  return sqlca.sqlcode;
               }   
         }      
    }else{
        sprintf(OutErrorInfo,"��ѯ��־��¼ʧ��:%c%c%c%c%c\n",sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4]);
        return -1;
    }         
    return 0;
}

int insert_alarm(char *rsDealCode,char *rsVal1,char *rsVal2,char *rsVal3,char *rsErrorInfo)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 682 "bass1_export.sqc"

     char in_ControlcCode[300];
     char in_CMDLine[300];
     char in_ErrorInfo[600];

/*
EXEC SQL END DECLARE SECTION;
*/

#line 686 "bass1_export.sqc"
 

    sprintf(in_CMDLine,"%s %s",rsDealCode,rsVal1);
    sprintf(in_ErrorInfo,"%s",rsErrorInfo);
    
    
/*
EXEC SQL select max(CONTROL_CODE) into :in_ControlcCode 
             from APP.SCH_CONTROL_TASK
             where upper(replace(replace(replace(CMD_LINE,'YESTERDAY()',''),'LASTMONTH()',''),' ',''))=upper(replace(:in_CMDLine,' ',''));
*/

{
#line 693 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 693 "bass1_export.sqc"
  sqlaaloc(2,1,18,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 693 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 300;
#line 693 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_CMDLine;
#line 693 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 693 "bass1_export.sqc"
      sqlasetdata(2,0,1,sql_setdlist,0L,0L);
    }
#line 693 "bass1_export.sqc"
  sqlaaloc(3,1,19,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 693 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 300;
#line 693 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_ControlcCode;
#line 693 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 693 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 693 "bass1_export.sqc"
  sqlacall((unsigned short)24,11,2,3,0L);
#line 693 "bass1_export.sqc"
  sqlastop(0L);
}

#line 693 "bass1_export.sqc"

    if ( sqlca.sqlcode!=0 ){
         printf("��ѯ��������ʧ��[%s %s %s]:%c%c%c%c%c\n",rsDealCode,rsVal1,rsVal2,sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4]);
         return sqlca.sqlcode;
    }
    
    sprintf(in_CMDLine,"%s %s %s",rsDealCode,rsVal1,rsVal2);
    
    
/*
EXEC SQL insert into APP.SCH_CONTROL_ALARM(CONTROL_CODE,CMD_LINE,GRADE,CONTENT,ALARMTIME,flag)
             values(:in_ControlcCode,:in_CMDLine,1,:in_ErrorInfo,current timestamp,-1);
*/

{
#line 702 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 702 "bass1_export.sqc"
  sqlaaloc(2,3,20,0L);
    {
      struct sqla_setdata_list sql_setdlist[3];
#line 702 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 300;
#line 702 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_ControlcCode;
#line 702 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 702 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 300;
#line 702 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)in_CMDLine;
#line 702 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 702 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 460; sql_setdlist[2].sqllen = 600;
#line 702 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)in_ErrorInfo;
#line 702 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 702 "bass1_export.sqc"
      sqlasetdata(2,0,3,sql_setdlist,0L,0L);
    }
#line 702 "bass1_export.sqc"
  sqlacall((unsigned short)24,12,2,0,0L);
#line 702 "bass1_export.sqc"
  sqlastop(0L);
}

#line 702 "bass1_export.sqc"

    if ( sqlca.sqlcode!=0 ) printf("����澯��Ϣ����[%s]:%c%c%c%c%c\n",in_CMDLine,sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4]);
    return sqlca.sqlcode;
}

int get_unit_info(char *rsTableName,char *OutUnitCode,char *OutFlag,int &OutCoarse,int &OutIfPut,int &OutIfEmptyPut,char *OutErrorInfo)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 709 "bass1_export.sqc"

    char in_TableName[1024];
    char out_UnitCode[1024];
    char out_Flag[1024];
    sqlint32  out_Coarse;
    sqlint32  out_IfPut;
    sqlint32  out_IfEmptyPut;
    sqlint32  nUnitRow;

/*
EXEC SQL END DECLARE SECTION;
*/

#line 717 "bass1_export.sqc"


   sprintf(in_TableName,rsTableName);
   
/*
EXEC SQL select count(*) into :nUnitRow from  APP.G_UNIT_INFO where upper(table_name)=upper(:in_TableName);
*/

{
#line 720 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 720 "bass1_export.sqc"
  sqlaaloc(2,1,21,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 720 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 1024;
#line 720 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_TableName;
#line 720 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 720 "bass1_export.sqc"
      sqlasetdata(2,0,1,sql_setdlist,0L,0L);
    }
#line 720 "bass1_export.sqc"
  sqlaaloc(3,1,22,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 720 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 496; sql_setdlist[0].sqllen = 4;
#line 720 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)&nUnitRow;
#line 720 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 720 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 720 "bass1_export.sqc"
  sqlacall((unsigned short)24,13,2,3,0L);
#line 720 "bass1_export.sqc"
  sqlastop(0L);
}

#line 720 "bass1_export.sqc"

   if ( sqlca.sqlcode!=0 ){
       sprintf(OutErrorInfo,"��ѯ���������ü�¼������:%c%c%c%c%c\n",sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4]);
       return sqlca.sqlcode;
   }
   if ( nUnitRow==0 ){
        sprintf(OutErrorInfo,"������������Ϣ������");   
        return -1;  
   }else{  
        
/*
EXEC SQL select unit_code,substr(table_name,6+3,1),coarse,put_flag,put_zero 
                  into :out_UnitCode,:out_Flag,:out_Coarse,:out_IfPut,:out_IfEmptyPut  
                 from APP.G_UNIT_INFO
                 where upper(table_name)=upper(:in_TableName);
*/

{
#line 732 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 732 "bass1_export.sqc"
  sqlaaloc(2,1,23,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 732 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 1024;
#line 732 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_TableName;
#line 732 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 732 "bass1_export.sqc"
      sqlasetdata(2,0,1,sql_setdlist,0L,0L);
    }
#line 732 "bass1_export.sqc"
  sqlaaloc(3,5,24,0L);
    {
      struct sqla_setdata_list sql_setdlist[5];
#line 732 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 1024;
#line 732 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)out_UnitCode;
#line 732 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 732 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 1024;
#line 732 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)out_Flag;
#line 732 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 732 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 496; sql_setdlist[2].sqllen = 4;
#line 732 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)&out_Coarse;
#line 732 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 732 "bass1_export.sqc"
      sql_setdlist[3].sqltype = 496; sql_setdlist[3].sqllen = 4;
#line 732 "bass1_export.sqc"
      sql_setdlist[3].sqldata = (void*)&out_IfPut;
#line 732 "bass1_export.sqc"
      sql_setdlist[3].sqlind = 0L;
#line 732 "bass1_export.sqc"
      sql_setdlist[4].sqltype = 496; sql_setdlist[4].sqllen = 4;
#line 732 "bass1_export.sqc"
      sql_setdlist[4].sqldata = (void*)&out_IfEmptyPut;
#line 732 "bass1_export.sqc"
      sql_setdlist[4].sqlind = 0L;
#line 732 "bass1_export.sqc"
      sqlasetdata(3,0,5,sql_setdlist,0L,0L);
    }
#line 732 "bass1_export.sqc"
  sqlacall((unsigned short)24,14,2,3,0L);
#line 732 "bass1_export.sqc"
  sqlastop(0L);
}

#line 732 "bass1_export.sqc"

        if ( sqlca.sqlcode!=0 ){
              sprintf(OutErrorInfo,"��ѯ���������ó���:%c%c%c%c%c\n",sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4]);
              return sqlca.sqlcode;
        }
        sprintf(OutUnitCode,out_UnitCode);
        sprintf(OutFlag,out_Flag);
        OutCoarse=out_Coarse;      
        OutIfPut=out_IfPut;       
        OutIfEmptyPut=out_IfEmptyPut;  
   }     
   return 0;
  
} 

void ReadString(const char *IniFile,char *Result,const char *Section,const char *Name,const char *Default)
{
    #define MAX_INI_NUM    512
    #define INISECTION     1
    #define ININAME        2
    #define INIVALUE       3 
     
    char c,section[MAX_INI_NUM],name[MAX_INI_NUM],value[MAX_INI_NUM];
    int i,IniSort,SectionExists,NameExists;
    FILE *fp;
  
    i = 0;
    IniSort = 0;
    SectionExists = 0;
    NameExists = 0;
    memset(section,0,sizeof(MAX_INI_NUM));
    memset(name,0,sizeof(MAX_INI_NUM));
    memset(value,0,sizeof(MAX_INI_NUM));
    if ((fp = fopen(IniFile,"r")) == NULL) {
       strcpy(Result,Default);
       return;
    }
    while ((c = getc(fp)) != EOF) {
       switch(c) {
         case '[':
              IniSort = INISECTION;
              value[i] = '\0';
              i = 0;
              if ((SectionExists == 1) && (NameExists == 1)) {
                 strcpy(Result,value);
                 fclose(fp);
                 return;
              }
              break;
         case ']':
              IniSort = ININAME;
              section[i] = '\0';
              if (strcmp(section,Section) == 0) {
                 SectionExists = 1;
              }
              i = 0;
              break;
         case '=':
              IniSort = INIVALUE;
              name[i] = '\0';
              if (strcmp(name,Name) == 0) {
                 NameExists = 1;
              }
              i = 0;
              break;
         case ' ':
              break;
         case '\t':
              break;
         case '\n':
	      if (strcmp(name,Name) == 0) {
         	  NameExists = 1;
	      }
	      IniSort = ININAME;
              value[i] = '\0';
              if ((SectionExists == 1) && (NameExists == 1)) {
                 strcpy(Result,value);
                 fclose(fp);
                 return;
              }
              i = 0;
              break;
         case '\0':
         default:
              if (IniSort == INISECTION) {
                 section[i] = c;
                 i++;
              } else {
                 if (IniSort == ININAME) {
                    name[i] = c;
                    i++;
                 } else {
                    if (IniSort == INIVALUE) {
                       value[i] = c;
                       i++;
                    }
                 }
              }
              break;
       } 
    }
    fclose(fp);
    value[i] = '\0';
    if ((SectionExists == 1) && (NameExists == 1)) {
       strcpy(Result,value);
    } else {
       strcpy(Result,Default);
    }
    return;
}
//�������ݿ�
int Connect_DB(char* rsDBName,char* rsUserName,char* rsPassWD)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 845 "bass1_export.sqc"

    char dbname[20];
    char username[20];
    char password[20];

/*
EXEC SQL END DECLARE SECTION;
*/

#line 849 "bass1_export.sqc"

     strcpy (dbname,rsDBName);
     strcpy (username,rsUserName );
     strcpy (password,rsPassWD); 
     
/*
EXEC SQL CONNECT TO :dbname USER :username USING :password ;
*/

{
#line 853 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 853 "bass1_export.sqc"
  sqlaaloc(2,3,25,0L);
    {
      struct sqla_setdata_list sql_setdlist[3];
#line 853 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 20;
#line 853 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)dbname;
#line 853 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 853 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 20;
#line 853 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)username;
#line 853 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 853 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 460; sql_setdlist[2].sqllen = 20;
#line 853 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)password;
#line 853 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 853 "bass1_export.sqc"
      sqlasetdata(2,0,3,sql_setdlist,0L,0L);
    }
#line 853 "bass1_export.sqc"
  sqlacall((unsigned short)29,5,2,0,0L);
#line 853 "bass1_export.sqc"
  sqlastop(0L);
}

#line 853 "bass1_export.sqc"
 
     return sqlca.sqlcode;
}

//�Ͽ������ݵ�����
int Close_DB()
{    
     
/*
EXEC SQL COMMIT WORK;
*/

{
#line 860 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 860 "bass1_export.sqc"
  sqlacall((unsigned short)21,0,0,0,0L);
#line 860 "bass1_export.sqc"
  sqlastop(0L);
}

#line 860 "bass1_export.sqc"

     
/*
EXEC SQL CONNECT RESET ;
*/

{
#line 861 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 861 "bass1_export.sqc"
  sqlacall((unsigned short)29,3,0,0,0L);
#line 861 "bass1_export.sqc"
  sqlastop(0L);
}

#line 861 "bass1_export.sqc"

     return 0;
}
int get_check_flag(char *rsDate,char *rsUnitCode,int nCoarse)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 866 "bass1_export.sqc"

    char      in_Date_1[1024];
    char      in_UnitCode_1[1024];
    sqlint32  in_Coarse_1;
    sqlint32  out_row_count;

/*
EXEC SQL END DECLARE SECTION;
*/

#line 871 "bass1_export.sqc"

   
   strcpy(in_Date_1,rsDate);
   strcpy(in_UnitCode_1,rsUnitCode);
   in_Coarse_1=nCoarse;
   
   
/*
EXEC SQL  select count(*)  into :out_row_count
             from APP.G_RUNLOG 
             where  time_id=integer(:in_Date_1) and unit_code=:in_UnitCode_1 
                and coarse=:in_Coarse_1 and return_flag=1;
*/

{
#line 880 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 880 "bass1_export.sqc"
  sqlaaloc(2,3,26,0L);
    {
      struct sqla_setdata_list sql_setdlist[3];
#line 880 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 460; sql_setdlist[0].sqllen = 1024;
#line 880 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)in_Date_1;
#line 880 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 880 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 460; sql_setdlist[1].sqllen = 1024;
#line 880 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)in_UnitCode_1;
#line 880 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 880 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 496; sql_setdlist[2].sqllen = 4;
#line 880 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)&in_Coarse_1;
#line 880 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 880 "bass1_export.sqc"
      sqlasetdata(2,0,3,sql_setdlist,0L,0L);
    }
#line 880 "bass1_export.sqc"
  sqlaaloc(3,1,27,0L);
    {
      struct sqla_setdata_list sql_setdlist[1];
#line 880 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 496; sql_setdlist[0].sqllen = 4;
#line 880 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)&out_row_count;
#line 880 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 880 "bass1_export.sqc"
      sqlasetdata(3,0,1,sql_setdlist,0L,0L);
    }
#line 880 "bass1_export.sqc"
  sqlacall((unsigned short)24,15,2,3,0L);
#line 880 "bass1_export.sqc"
  sqlastop(0L);
}

#line 880 "bass1_export.sqc"

   if ( sqlca.sqlcode==0 )
   {
        if  ( out_row_count==0 )
            return NOT_PASS_CHECK;
        else
            return PASS_CHECK;
   }               
   return -1;
}

int if_all_datafile_pass(int nCoarse)
{

/*
EXEC SQL BEGIN DECLARE SECTION;
*/

#line 893 "bass1_export.sqc"

    sqlint32  in_coarse_day_mon;
    sqlint32  in_need_num;
    sqlint32  in_fact_num;
    sqlint32  in_Coarse_day;

/*
EXEC SQL END DECLARE SECTION;
*/

#line 898 "bass1_export.sqc"


    in_coarse_day_mon   = nCoarse ;
    in_Coarse_day       = COARSE_DAY;

    
/*
EXEC SQL  select need_num,fact_num  into :in_need_num,:in_fact_num
              from  (   
                        select count(*) need_num  
                        from   APP.G_UNIT_INFO 
                        where  put_flag=1  and coarse=:in_coarse_day_mon
       	                   and unit_code<>case when coarse=:in_Coarse_day then '00000' else '99999' end                    
                    ) a,
                    (  
                         select count(*) fact_num  
       	                 from  APP.G_RUNLOG     
       	                 where  return_flag=1 and coarse=:in_coarse_day_mon 
                           and unit_code<>case when coarse=:in_Coarse_day then '00000' else '99999' end   		    
                    ) b;
*/

{
#line 915 "bass1_export.sqc"
  sqlastrt(sqla_program_id, &sqla_rtinfo, &sqlca);
#line 915 "bass1_export.sqc"
  sqlaaloc(2,4,28,0L);
    {
      struct sqla_setdata_list sql_setdlist[4];
#line 915 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 496; sql_setdlist[0].sqllen = 4;
#line 915 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)&in_coarse_day_mon;
#line 915 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 915 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 496; sql_setdlist[1].sqllen = 4;
#line 915 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)&in_Coarse_day;
#line 915 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 915 "bass1_export.sqc"
      sql_setdlist[2].sqltype = 496; sql_setdlist[2].sqllen = 4;
#line 915 "bass1_export.sqc"
      sql_setdlist[2].sqldata = (void*)&in_coarse_day_mon;
#line 915 "bass1_export.sqc"
      sql_setdlist[2].sqlind = 0L;
#line 915 "bass1_export.sqc"
      sql_setdlist[3].sqltype = 496; sql_setdlist[3].sqllen = 4;
#line 915 "bass1_export.sqc"
      sql_setdlist[3].sqldata = (void*)&in_Coarse_day;
#line 915 "bass1_export.sqc"
      sql_setdlist[3].sqlind = 0L;
#line 915 "bass1_export.sqc"
      sqlasetdata(2,0,4,sql_setdlist,0L,0L);
    }
#line 915 "bass1_export.sqc"
  sqlaaloc(3,2,29,0L);
    {
      struct sqla_setdata_list sql_setdlist[2];
#line 915 "bass1_export.sqc"
      sql_setdlist[0].sqltype = 496; sql_setdlist[0].sqllen = 4;
#line 915 "bass1_export.sqc"
      sql_setdlist[0].sqldata = (void*)&in_need_num;
#line 915 "bass1_export.sqc"
      sql_setdlist[0].sqlind = 0L;
#line 915 "bass1_export.sqc"
      sql_setdlist[1].sqltype = 496; sql_setdlist[1].sqllen = 4;
#line 915 "bass1_export.sqc"
      sql_setdlist[1].sqldata = (void*)&in_fact_num;
#line 915 "bass1_export.sqc"
      sql_setdlist[1].sqlind = 0L;
#line 915 "bass1_export.sqc"
      sqlasetdata(3,0,2,sql_setdlist,0L,0L);
    }
#line 915 "bass1_export.sqc"
  sqlacall((unsigned short)24,16,2,3,0L);
#line 915 "bass1_export.sqc"
  sqlastop(0L);
}

#line 915 "bass1_export.sqc"

   if ( sqlca.sqlcode!=0 ){
        printf("ȡ�ϴ���ͨ����֤��ʧ��:%c%c%c%c%c\n",sqlca.sqlstate[0],sqlca.sqlstate[1],sqlca.sqlstate[2],sqlca.sqlstate[3],sqlca.sqlstate[4]);
        return -1;
   } 
   return in_fact_num-in_need_num;
}