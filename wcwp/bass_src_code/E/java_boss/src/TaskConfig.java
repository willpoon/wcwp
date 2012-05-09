import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;


public class TaskConfig {
	protected String strProName;
	protected String strTaskID;
	
	public TaskConfig(String strTaskID) {
		this.strTaskID = strTaskID;
		this.strProName = strTaskID + ".properties";
	}

	public String getProperties(String strKey) {
		
		CommonConfig cc = new CommonConfig();
		String strConfigPath = cc.getProperties("configPath");
		
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(strConfigPath+strProName);
		Properties p = new Properties();
		try {
			p.load(inputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return p.getProperty(strKey);
		
	}
	
	public int setProperties(String strKey, String strValue) {
		
		CommonConfig cc = new CommonConfig();
		String strConfigPath = cc.getProperties("configPath");
		
		String strAllConfigPath = cc.getProperties("allConfigPath");
		
		
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(strConfigPath+strProName);
		Properties p = new Properties();
		try {
			
			p.load(inputStream);
			p.setProperty(strKey, strValue);
			//String filename = System.getProperty("user.dir")+"\\bin\\"+strProName;
			String filename = strAllConfigPath+strProName;
			FileOutputStream out = new FileOutputStream(filename);
			p.store(out,"");
			out.flush();
			out.close();
			inputStream.close();
		
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		return 0;
	}
	
	public String getTaskID() {
		return this.getProperties("TASK_ID");
	}
	
	public String getCycle() {
		return this.getProperties("CYCLE");
	}
	
	public String getRuntype() {
		return this.getProperties("RUNTYPE");
	}
	
	public String getDateParam() {
		return this.getProperties("DATEPARAM");
	}
	
	public String getAreaParam() {
		return this.getProperties("AREAPARAM");
	}
	
	public String getRunSql() {
		return this.getProperties("RUNSQL");
	}
	
	public String getDateCycle() {
		return this.getProperties("DATECYCLE");
	}
	
	public int setCycle(String strValue) {
		return this.setProperties("CYCLE", strValue);
	}
	
	public String getModule() {
		return this.getProperties("MODULE");
	}
	
	public String getReplaceRule() {
		return this.getProperties("REPLACERULE");
	}
	
	public String getOwner1() {
		return this.getProperties("OWNER1");
	}
	
	public String getOwner2() {
		return this.getProperties("OWNER2");
	}	
	
	public static void main(String[] args) {
		TaskConfig tc = new TaskConfig("A01209");
		System.out.println(tc.getAreaParam());
		System.out.println(tc.getCycle());
		tc.setCycle("20090114");
		tc.setProperties("A891", "999");
		System.out.println(tc.getCycle());
		
	}
	
}
