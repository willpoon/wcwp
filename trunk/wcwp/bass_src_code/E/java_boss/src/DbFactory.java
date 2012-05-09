import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
public class DbFactory {
	
	private static DbFactory dbFactory = null;
	
	private DbFactory() {
	}
	
	public static synchronized DbFactory getInstance() {
		if ( dbFactory == null ) {
			return new DbFactory();
		} else {
			return dbFactory;
		}
	}
	
	
	//从配置文件获取数据库连接，只适用于ORACLE库
	public Connection getConnection(String strDB) {
		
		DBConfig dbc = new DBConfig(strDB);
		String port = dbc.getDBPort();
		String server = dbc.getServer();
		String ip = dbc.getDBIP();
		String user = dbc.getDBUser();
		String pass = dbc.getDBPass();
		String url = "jdbc:oracle:thin:@" + ip + ":" + port + ":" + server;
		
		System.out.println(url);
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
			Connection con = java.sql.DriverManager.getConnection(url,user,pass);
			return con;
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	

	
}
