import java.sql.*;

public class Sqlca {
	private Connection conn = null;
	private ResultSet rs = null;
	private Statement stmt = null;

  	public Sqlca(String strDB) {
  			conn = DbFactory.getInstance().getConnection(strDB);
  			try {
				stmt = this.conn.createStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
  	}

  	public ResultSet executeQuery(String strSql) {
    	try {
      		
      		rs = stmt.executeQuery(strSql);
    	} catch (SQLException e) {
    		e.printStackTrace();
    		System.out.println("查询出错:\n"+strSql);
    	}
    	return rs;
  	}

  	public void executeUpdate(String strSql) {
    	try {
      		stmt.executeUpdate(strSql);
    	} catch (Exception e) {
    		e.printStackTrace();
      		System.out.println("更新出错:\n"+strSql);
    	}
  	}

  	public void Close() {
    	try {
    		if(rs != null) {
    			rs.close();
    		}
    		stmt.close();
      		conn.close();
    	} catch (SQLException e) {
      		System.out.println("关闭连接出错!");
    	}finally{
    		try{
        		if(rs != null) {
        			rs.close();
        		}
    			stmt.close();
    			conn.close();
    		}catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
  	}
}