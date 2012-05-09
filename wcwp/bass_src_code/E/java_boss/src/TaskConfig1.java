import java.sql.*;

public class TaskConfig1 {

	protected String strTaskID;

	public TaskConfig1(String strTaskID) {
		this.strTaskID = strTaskID;
	}

	public String getTaskSQL() {
		Sqlca sqlca = new Sqlca("xztest");
		String strSql = "select sql1,sql2,sql3 from mb_if_jyfx_task_sql where task_id='"
				+ strTaskID + "'";
		ResultSet rs = sqlca.executeQuery(strSql);
		String interfaceSQL = "";
		try {
			if (rs.next()) {
				String interfaceSQL1 = rs.getString(1);
				String interfaceSQL2 = rs.getString(2);
				String interfaceSQL3 = rs.getString(3);
				interfaceSQL = interfaceSQL1 + " " + interfaceSQL2 + " "
						+ interfaceSQL3;
			}

			interfaceSQL = interfaceSQL.substring(interfaceSQL
					.indexOf("/*BEGIN*/") + 9, interfaceSQL.indexOf("/*END*/"));
			interfaceSQL = interfaceSQL
					.replaceAll("\\$YYYYMMDD\\$", "20090106");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			sqlca.Close();
		}
		return interfaceSQL;
	}

	public String getRunParam() {
		
		Sqlca sqlca = new Sqlca("xztest");
		String strSql = "select run_param from mb_if_jyfx_task_sql where task_id='"
				+ strTaskID + "'";
		ResultSet rs = sqlca.executeQuery(strSql);
		
		String strParam = "";
		try {
			if(rs.next()) {
				strParam = rs.getString(1);
			}
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			sqlca.Close();
		}
		return strParam;
	}

	public static void main(String[] args) {
		TaskConfig1 tc = new TaskConfig1("A06004");
		System.out.println(tc.getRunParam());
	}
}
