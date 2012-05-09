import java.io.File;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class ETLMain {
	
	private static String strInCycle = "";
	private static String strTaskListConfig = "";

	public static int iTreadcnt = 3;

	public static void main(String[] args) {
			
		
			TaskListConfig tlc = null;
		
			if( args.length > 2) {
				System.out.println("��Ҫ����������߲����������࣬�澯!");
				return;
			}
			
			if(args.length == 1) {
				System.out.println("��ָ�����ڳ�ȡ,���������ļ��г�ȡ����CYCLE");
				strInCycle = args[0];
				tlc = new TaskListConfig();
			}
			
			if(args.length == 2) {
				System.out.println("��ָ�����ڳ�ȡ,��ָ����ȡ�����ļ���,���������ļ��г�ȡ����CYCLE");
				strInCycle = args[0];
				strTaskListConfig = args[1];
				tlc = new TaskListConfig(strTaskListConfig);
			}
			
			if(args.length == 0) {
				System.out.println("�������ļ��洢���ڽ��г�ȡ����ȡ��ɺ󣬸��³�ȡ����Ϊ������");
				tlc = new TaskListConfig();
			}
			
			//TaskListConfig tlc = new TaskListConfig();
			
			
			if ( tlc == null ) {
				System.out.println("��ȡ��ȡ�����ļ�����Ϊ��!!!");
				return;
			}
			
			Vector v = tlc.getTaskList();
			
			CommonConfig cc = new CommonConfig();
			String strDataPath = cc.getProperties("dataPath");
			
			while(true) {
				
				String filename = strDataPath + "stop_java_crminterface";
				File f = new File(filename);
				if(f.exists()) {
					break;
				}
				
				if (!v.isEmpty()) {
					for(int i = 0; i < v.size(); i++) {
						if(iTreadcnt >= 0) {
							TaskInfoBean tib = (TaskInfoBean)v.get(i);
							String strInterfaceID = tib.getTaskId();
							String strPlanTime = tib.getPlanTime();
							String currentDay = new Timestamp(System.currentTimeMillis()).toString().substring(8,10);
							String currentHour = new Timestamp(System.currentTimeMillis()).toString().substring(11,13);
							String currentMinute = new Timestamp(System.currentTimeMillis()).toString().substring(14,16);
							
							
							System.out.println(strInterfaceID);
							
							System.out.println(Integer.parseInt(strPlanTime));
							if (strPlanTime.length() == 4) {
								String currentTime = currentHour+currentMinute;
								System.out.println("currentTime ==================>" + currentTime);
								System.out.println(Integer.parseInt(currentTime));
								if(Integer.parseInt(currentTime) > Integer.parseInt(strPlanTime)) {
									EtlTasks ets = null;
									if(ETLMain.strInCycle.equalsIgnoreCase("")) {
										ets = new EtlTasks(strInterfaceID);
									} else { 
										ets = new EtlTasks(strInterfaceID,ETLMain.strInCycle);
									}
									Thread t = new Thread(ets);
									t.start();
									System.out.println(iTreadcnt);
									v.remove(i);
								}
							} else if (strPlanTime.length() == 6) {
								String currentTime = currentDay+currentHour+currentMinute;
								System.out.println("currentTime ==================>" + currentTime);
								System.out.println(Integer.parseInt(currentTime));
								if(Integer.parseInt(currentTime) > Integer.parseInt(strPlanTime)) {
									EtlTasks ets = null;
									if(ETLMain.strInCycle.equalsIgnoreCase("")) {
										ets = new EtlTasks(strInterfaceID);
									} else {
										ets = new EtlTasks(strInterfaceID,ETLMain.strInCycle);
									}
									Thread t = new Thread(ets);
									t.start();
									System.out.println(iTreadcnt);
									v.remove(i);
								}
							}
						}
					}
				} else {
					break;
				}
				
				/*
				if(em.hasMoreElements()) {
					if(iTreadcnt >= 0) {
						TaskInfoBean tib = (TaskInfoBean)em.nextElement();
						String strInterfaceID = tib.getTaskId();
						String strPlanTime = tib.getPlanTime();
						String currentHour = new Timestamp(System.currentTimeMillis()).toString().substring(11,13);
						String currentMinute = new Timestamp(System.currentTimeMillis()).toString().substring(14,16);
						String currentTime = currentHour+currentMinute;
						System.out.println(strInterfaceID);
						
						if(Integer.getInteger(currentTime).intValue() > Integer.getInteger(strPlanTime).intValue()) {
							EtlTasks ets = null;
							if(ETLMain.strInCycle.equalsIgnoreCase("")) {
								ets = new EtlTasks(strInterfaceID);
							} else {
								ets = new EtlTasks(strInterfaceID,ETLMain.strInCycle);
							}
							Thread t = new Thread(ets);
							t.start();
							System.out.println(iTreadcnt);
						}
					}
				} else {
					break;
				}
				*/
				
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				

				
				
			}
			
		
	}
}
