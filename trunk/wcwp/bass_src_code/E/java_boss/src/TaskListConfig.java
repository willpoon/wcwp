import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;


public class TaskListConfig {
	
	public String strTaskConfigFileName;
	
	public TaskListConfig() {
		strTaskConfigFileName = "taskList.properties";
	}
	
	public TaskListConfig(String strTaskConfigFileName) {
		this.strTaskConfigFileName = strTaskConfigFileName;
	}
	
	public String getProperties(String strKey) {
		
		CommonConfig cc = new CommonConfig();
		String strConfigPath = cc.getProperties("configPath");
		
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(strConfigPath+this.strTaskConfigFileName);
		Properties p = new Properties();
		try {
			p.load(inputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return p.getProperty(strKey);
		
	}
	
	public Vector getTaskList() {
		
		String strTaskInfo = this.getProperties("taskIdList");
		String[] arrTaskInfo = strTaskInfo.split(",");
		Vector v = new Vector();
		for(int i = 0 ; i < arrTaskInfo.length ; i++) {
			TaskInfoBean tib = new TaskInfoBean();
			String singleTaskInfo = arrTaskInfo[i];
			String[] arrSingleTaskInfo = singleTaskInfo.split("\\$");
			
			tib.setTaskId(arrSingleTaskInfo[0]);
			tib.setPlanTime(arrSingleTaskInfo[1]);
			v.add(tib);

		}
		
		return v;
	}
	
	public static void main(String[] args) {
		TaskListConfig tlc = new TaskListConfig();
		Vector v = tlc.getTaskList();
		Enumeration em = v.elements();
		while(em.hasMoreElements()) {
			TaskInfoBean tib = (TaskInfoBean)em.nextElement();
			String strInterfaceID = tib.getTaskId();
			String strPlanTime = tib.getPlanTime();
			System.out.println(strInterfaceID);
			System.out.println(strPlanTime);
		}
		System.out.println(new Timestamp(System.currentTimeMillis()).toString());
		System.out.println(new Timestamp(System.currentTimeMillis()).toString().substring(8,10));
		String currentDay = new Timestamp(System.currentTimeMillis()).toString().substring(11,13);
		String currentHour = new Timestamp(System.currentTimeMillis()).toString().substring(11,13);
		String currentMinute = new Timestamp(System.currentTimeMillis()).toString().substring(14,16);
		String currentTime = currentHour+currentMinute;
		System.out.println("currentTime ==================>" + currentTime);
		System.out.println(Integer.parseInt(currentTime));
		
	}
	
}
