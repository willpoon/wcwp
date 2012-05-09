import java.io.FileWriter;
import java.sql.*;

public class EtlTest {
	public static void main(String[] args) {
		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			
			System.out.println(new Timestamp(System.currentTimeMillis()));
			
			//Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
			//Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@10.233.30.60:1521:xztestdb","zk","zk1234");
			conn = DbFactory.getInstance().getConnection("xztest");
			stmt = conn.createStatement();
			
			/*
			String strSql = "select SO_NBR,SERV_ID,REGION_CODE,SERVICE_ID,SPROM_ID," +
					"SPROM_TYPE,BUSI_TYPE,SPROM_PRIOR,PLAN_ID,SPROM_PARA,PAY_MODE," +
					"PROPERTY,TO_CHAR(CREATE_DATE,'YYYYMMDDHH24MISS') CREATE_DATE," +
					"TO_CHAR(VALID_DATE,'YYYYMMDDHH24MISS') VALID_DATE," +
					"TO_CHAR(EXPIRE_DATE,'YYYYMMDDHH24MISS') EXPIRE_DATE," +
					"PSO_NBR,REMARK from zk.cm_user_sprom_his_view where rownum<1000";
			*/
			
			String strSql = "select sql1,sql2,sql3 from mb_if_jyfx_task_sql where task_id='A06004'";
			rs = stmt.executeQuery(strSql);
			
			String interfaceSQL = "";
			if(rs.next()) {
				String interfaceSQL1 = rs.getString(1);
				String interfaceSQL2 = rs.getString(2);
				String interfaceSQL3 = rs.getString(3);
				interfaceSQL = interfaceSQL1 + " " + interfaceSQL2 + " " + interfaceSQL3;
			}
			
			interfaceSQL = interfaceSQL.substring(interfaceSQL.indexOf("/*BEGIN*/")+9,interfaceSQL.indexOf("/*END*/"));
			interfaceSQL = interfaceSQL.replaceAll("\\$YYYYMMDD\\$", "20090106");
			
			System.out.println("SQL========>"+interfaceSQL);
			
			rs = stmt.executeQuery(interfaceSQL);
			FileWriter fw = new FileWriter("D:\\aaa.AVL");
			int iColcnt  = rs.getMetaData().getColumnCount();
			int cnt = 0;
			while(rs.next()) {
				
				String tmpLine = "";
				for(int i = 1; i <= iColcnt; i++) {
					String tmpColumn = rs.getString(i);
					if(tmpColumn == null || tmpColumn.equalsIgnoreCase("null")) {
						tmpColumn = "";
					}
					tmpLine += tmpColumn + "$";
				}
				tmpLine = tmpLine.substring(0, tmpLine.length()-1) + "\n";
				fw.write(tmpLine);
				
				cnt++;
				
				if(cnt % 100000 == 0) {
					fw.flush();
					System.out.println("cnt============>"+cnt);
				}
				
			}
			
			System.out.println("totalcnt============>"+cnt);
			fw.flush();
			
			
			rs.close();
			stmt.close();
			conn.close();
			System.out.println(new Timestamp(System.currentTimeMillis()));
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			try {
			    rs.close();
			    stmt.close();
			    conn.close();
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
