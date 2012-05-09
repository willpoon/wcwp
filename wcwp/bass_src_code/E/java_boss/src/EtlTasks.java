import java.io.File;
import java.io.FileWriter;
import java.sql.*;

public class EtlTasks implements Runnable{
	
	private String strInTaskID = "";
	private String strInCycle = "";

	public EtlTasks(String inTaskID,String inCycle) {
		ETLMain.iTreadcnt--;
		this.strInTaskID = inTaskID;
		this.strInCycle = inCycle;
	}
	
	public EtlTasks(String inTaskID) {
		ETLMain.iTreadcnt--;
		this.strInTaskID = inTaskID;
	}
	
	public void run() {
		
		try {
			
			System.out.println("当前时间====>"+new Timestamp(System.currentTimeMillis()));
			
			//String inTaskID = this.strInTaskID;
			//String inCycle = this.strInCycle;
			

			
			//获取接口配置参数
			TaskConfig tc = new TaskConfig(strInTaskID);
			String strSql = tc.getRunSql();
			String strAreaParam = tc.getAreaParam();
			String strCycle = "";
			if(this.strInCycle.equalsIgnoreCase("")) {
				strCycle = tc.getCycle();
			} else {
				strCycle = this.strInCycle;
			}
			String strRunType = tc.getRuntype();
			String strDateCycle = tc.getDateCycle();
			String strTaskID = tc.getTaskID();
			String strModule = tc.getModule();
			String strDataParam = tc.getDateParam();
			String strReplaceRule = tc.getReplaceRule();
			String owner1=tc.getOwner1();
			String owner2=tc.getOwner2();
			
			//下周期抽取时间
			String strNextCycle = "";
			
			if(strSql == null || strSql.equalsIgnoreCase(null) || strSql.equalsIgnoreCase("")) {
				System.out.println("获取配置文件SQL语句信息错误！");
				return;
			}
			
			if(strAreaParam == null || strAreaParam.equalsIgnoreCase(null) || strAreaParam.equalsIgnoreCase("")) {
				System.out.println("获取配置文件地区编码信息错误！");
				return;
			}
			
			if(strCycle == null || strCycle.equalsIgnoreCase(null) || strCycle.equalsIgnoreCase("")) {
				System.out.println("获取配置文件运行周期替换字符串信息错误！");
				return;
			}
			
			if(strDateCycle == null || strDateCycle.equalsIgnoreCase(null) || strDateCycle.equalsIgnoreCase("")) {
				System.out.println("获取配置文件执行周期信息错误！");
				return;
			}
			
			if(strRunType == null || strRunType.equalsIgnoreCase(null) || strRunType.equalsIgnoreCase("")) {
				System.out.println("获取配置文件运行类型信息错误！");
				return;
			}
			
			if(strTaskID == null || strTaskID.equalsIgnoreCase(null) || strTaskID.equalsIgnoreCase("")) {
				System.out.println("获取配置文件接口编号信息错误！");
				return;
			}
			
			if(strModule == null || strModule.equalsIgnoreCase(null) || strModule.equalsIgnoreCase("")) {
				System.out.println("获取配置文件访问模式信息错误！");
				return;
			}
			
			if(strReplaceRule == null || strReplaceRule.equalsIgnoreCase(null) || strReplaceRule.equalsIgnoreCase("")) {
				System.out.println("未配置日期替换规则,默认为前周期!");
				strReplaceRule = "CURRENTMONTH";
			}
			if(owner1 == null || owner1.equalsIgnoreCase(null) || owner1.equalsIgnoreCase("")) {
				System.out.println("未配置owner1");
				owner1 = "DEFAULTOWNER";
			}
			if(owner2 == null || owner2.equalsIgnoreCase(null) || owner2.equalsIgnoreCase("")) {
				System.out.println("未配置owner2");
				owner2 = "DEFAULTOWNER";
			}					
			ETLWrite ew = new ETLWrite();

			//替换抽取语句中的时间参数
			DateHelp dh = new DateHelp();
			
			
			if(strDateCycle.equalsIgnoreCase("D")) {
				
				int year = Integer.parseInt(strCycle.substring(0, 4));
				int month = Integer.parseInt(strCycle.substring(4, 6))-1;
				int day = Integer.parseInt(strCycle.substring(6, 8));
				dh.set(year, month, day, 0, 0, 0); 
				
				if(strDataParam.equalsIgnoreCase("$YYYYMMDD$")) {
					strSql = strSql.replaceAll("\\$YYYYMMDD\\$", strCycle);
					strSql = strSql.replaceAll("\\$YYYYMM\\$", dh.getCurrentMonth("yyyyMM"));
				} else {
					strSql = strSql.replaceAll("\\$YYYYMM\\$", dh.getCurrentMonth("yyyyMM"));
				}

				System.out.println(strCycle);
				strNextCycle = dh.getNextDay();
			} else if(strDateCycle.equalsIgnoreCase("M")) {
				//strSql = strSql.replaceAll("\\$YYYYMM\\$", strCycle.substring(0,6));
				int year = Integer.parseInt(strCycle.substring(0, 4));
				int month = Integer.parseInt(strCycle.substring(4, 6))-1;
				int day = 1;
				dh.set(year, month, day, 0, 0, 0);
				strNextCycle = dh.getNextMonth();
				//如果日期替换规则为CURRENTMONTH,YYYYMM替换为本月,规则为NEXTMONTH,YYYYMM替换为下月
				if (strReplaceRule.equalsIgnoreCase("CURRENTMONTH")) {
					strSql = strSql.replaceAll("\\$YYYYMM\\$", strCycle.substring(0,6));
				} else if (strReplaceRule.equalsIgnoreCase("NEXTMONTH")) {
					strSql = strSql.replaceAll("\\$YYYYMM\\$", strNextCycle);
				}
				
			}
			
			CommonConfig cc = new CommonConfig();
			String strDataPath = cc.getProperties("dataPath");
			
			//如果接口需要分地州分表进行抽取,替换抽取语句中的地区参数,使用多线程处理
			if(strRunType.equalsIgnoreCase("C")) {
				String[] arrAreaParam = strAreaParam.split(",");
				for(int i = 0; i < arrAreaParam.length; i++) {
					System.out.println(arrAreaParam[i]);
					EtlTask et = new EtlTask(strSql,arrAreaParam[i],strTaskID,strModule,strCycle,owner1,owner2);
					Thread subt = new Thread(et);
					subt.start();
				}
				
				while(true) {
					Thread.sleep(10000);
					boolean successFlag = true;
					for(int i = 0; i < arrAreaParam.length; i++) {
						String filename = strDataPath + strTaskID + strCycle + "000" + arrAreaParam[i] + ".AVL.T";
						File f = new File(filename);
						if(!f.exists()) {
							successFlag = false;
							System.out.println(strTaskID + arrAreaParam[i] + "======>文件未生成,不能CAT!" );
						}
					}
					if(successFlag) {
						String pattenName = strDataPath + strTaskID + strCycle + "000";
						//合并文件
						String[] commands = {"sh","-c","cat " + pattenName +"*.AVL.T > " + pattenName + "000.AVL"};
						Process  p  = Runtime.getRuntime().exec(commands);
						p.waitFor();
						//压缩文件
						commands = new String[]{"sh","-c","compress -f " + pattenName + "000.AVL"};
						p  = Runtime.getRuntime().exec(commands);
						p.waitFor();
						File f = new File(pattenName+"000.AVL.Z");
						if(!f.exists()) {
							System.out.println("创建接口文件"+strTaskID+"失败!");
						} else {
							//删除临时文件
							commands = new String[]{"sh","-c","rm " + pattenName +"*.T"};
							p = Runtime.getRuntime().exec(commands);
							p.waitFor();
							break;
						}
					}
				}
						//从配置文件获取各地州接口文件记录条数
						int iCnt = 0;
						for(int i = 0; i < arrAreaParam.length; i++) {
							TaskConfig tcCnt = new TaskConfig(strInTaskID);
							iCnt += Integer.parseInt(tcCnt.getProperties("CNT"+arrAreaParam[i]));
						}
						ew.writeChkFile(strTaskID, strCycle, iCnt);

			} else {
				
				//写接口文件,并返回记录条数
				int cnt = ew.writeInterfaceFile(strSql, "000",strTaskID,strModule,strCycle);
				//写校验文件
				ew.writeChkFile(strTaskID, strCycle, cnt);
			}
			
			//更新下周期抽取时间
			tc.setCycle(strNextCycle);
			ETLMain.iTreadcnt++;
			System.out.println(new Timestamp(System.currentTimeMillis()));
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
