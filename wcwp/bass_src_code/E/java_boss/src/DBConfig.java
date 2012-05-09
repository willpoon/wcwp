import java.io.InputStream;
import java.util.Properties;


public class DBConfig {
	
	protected String strKey;
	protected String strDB;
	
	public DBConfig(String strDB) {
		this.strDB = strDB.toLowerCase();
	}

	public String getProperties(String strKey) {
		
		
		CommonConfig cc = new CommonConfig();
		String strConfigPath = cc.getProperties("configPath");
		
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(strConfigPath+"DBConfig.properties");
		Properties p = new Properties();
		try {
			p.load(inputStream);
			inputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return p.getProperty(strKey).trim();
		
	}
	
	//获取数据库访问模式
	public String getSchema() {
		strKey = strDB + "_schema";
		return getProperties(strKey);
	}
	
	//获取服务名称
	public String getServer() {
		strKey = strDB + "_server";
		return getProperties(strKey);
	}
	
	//获取数据库名
	public String getDBName() {
		strKey = strDB + "_dbname";
		return getProperties(strKey);
	}
	
	//获取数据库用户名
	public String getDBUser() {
		strKey = strDB + "_dbuser";
		return getProperties(strKey);
	}
	
	//获取数据库密码
	public String getDBPass() {
		strKey = strDB + "_dbpass";
		return getProperties(strKey);
	}
	
	//获取数据库IP
	public String getDBIP() {
		strKey = strDB + "_dbip";
		return getProperties(strKey);
	}
	
	//获取数据库端口
	public String getDBPort() {
		strKey = strDB + "_dbport";
		return getProperties(strKey);
	}
	
	public static void main(String[] args) {
		DBConfig dbc = new DBConfig("xztest");
		System.out.println(dbc.getSchema());
		System.out.println(dbc.getServer());
		System.out.println(dbc.getDBName());
		System.out.println(dbc.getDBUser());
		System.out.println(dbc.getDBPass());
		System.out.println(dbc.getDBIP());
		System.out.println(dbc.getDBPort());
		
	}
}

