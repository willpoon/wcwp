import java.sql.Connection;
import java.sql.DriverManager;


public class testDB {
	
	public static void main(String[] args) throws Exception {
	String url;
    String userName;
    String userPwd;
    url = "jdbc:db2:DMKDB10";
   // url="jdbc:db2://10.233.23.10:50000/DMKDB";
    userName = "dmkmark";
    userPwd = "dmkmark";
    Connection con;
   
		//com.ibm.db2.jcc.DB2Driver
		 Class.forName("COM.ibm.db2.jdbc.app.DB2Driver").newInstance();
	       con = DriverManager.getConnection(url, userName, userPwd);
	     
	}
}
