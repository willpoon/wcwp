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
	
	public TaskConfig(String strProName) {
		this.strProName = strProName;
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
	
	public String getServer() {
		return this.getProperties("SERVER");
	}
	
	public String getUser() {
		return this.getProperties("USER");
	}
	
	
	public String getPassword() {
		return this.getProperties("PASSWORD");
	}
	
	public String getRemotePath() {
		return this.getProperties("REMOTEPATH");
	}
	
	public String getTmpPath() {
		return this.getProperties("LOCALTMPPATH");
	}
	
	public String getLocalPath() {
		return this.getProperties("LOCALPATH");
	}
	
	public String getRegName() {
		return this.getProperties("REGNAME");
	}
	
	public String getLocalBackupPath() {
		return this.getProperties("LOCALBACKUPPATH");
	}
	
	public String getDatePoint() {
		return this.getProperties("DATEPOINT");
	}
	
	public String getInterfaceCodePoint() {
		return this.getProperties("INTERFACECODEPOINT");
	}
	
	public String getRegex() {
		return this.getProperties("REGEX");
	}
	
	public String getCycle() {
		return this.getProperties("CYCLE");
	}
	
	public String getCodeReg() {
		return this.getProperties("CODEREG");
	}
	
	public String getReportPath() {
		return this.getProperties("REPORTPATH");
	}
	
	public String getRemoteReportPath() {
		return this.getProperties("REMOTEREPORTPATH");
	}
	
	public String getRemoteBackupPath() {
		return this.getProperties("REMOTEBACKUPPATH");
	}
	
	public String getRemoteReportBackupPath() {
		return this.getProperties("REMOTEREPORTBACKUPPATH");
	}
	
	
	public static void main(String[] args) {
		TaskConfig tc = new TaskConfig("A01209");

		
	}
	
}
