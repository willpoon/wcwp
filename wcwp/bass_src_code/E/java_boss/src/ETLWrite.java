import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ETLWrite {
	
	//生成接口文件
	public int writeInterfaceFile(String strSql, String area, String strTaskID,
			String strModule, String strCycle) {

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		int cnt = 0;
		try {
			
			CommonConfig cc = new CommonConfig();
			String strDataPath = cc.getProperties("dataPath");
			
			System.out.println(new Timestamp(System.currentTimeMillis()));
			conn = DbFactory.getInstance().getConnection(strModule);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(strSql);

			//String filename = "D:\\" + strTaskID + strCycle + "000" + area
			//		+ ".AVL";
			
			 String filename = strDataPath + strTaskID +
			 strCycle + "000" + area + ".AVL";
			FileWriter fw = new FileWriter(filename);
			int iColcnt = rs.getMetaData().getColumnCount();

			while (rs.next()) {

				String tmpLine = "";
				for (int i = 1; i <= iColcnt; i++) {
					String tmpColumn = rs.getString(i);
					if (tmpColumn == null || tmpColumn.equalsIgnoreCase("null")) {
						tmpColumn = "";
					}
					tmpLine += tmpColumn + "$";
				}
				tmpLine = tmpLine.substring(0, tmpLine.length() - 1) + "\n";
				fw.write(tmpLine);

				cnt++;

				// 每10W条记录刷新一次
				if (cnt % 100000 == 0) {
					fw.flush();
					System.out.println("cnt============>" + cnt);
				}

			}

			System.out.println("totalcnt============>" + cnt);
			fw.flush();
			fw.close();
			//compress单地州抽取AVL文件,重命名分地州抽取的接口文件
			if(area.equals("000")) {
				Process p = Runtime.getRuntime().exec("compress -f " + filename);
				p.waitFor();
				File f = new File(filename+".Z");
				if(!f.exists()) {
					System.out.println("文件compress失败!");
				} 
			} else {
				File f = new File(filename);
				f.renameTo(new File(filename+".T"));
			}

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		} finally {
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return cnt;
	}

	//生成校验文件
	public int writeChkFile(String strTaskID, String strCycle, int cnt) {

		CommonConfig cc = new CommonConfig();
		String strDataPath = cc.getProperties("dataPath");
		
		String strWorkPath = strDataPath;
		String AVLFileName = strTaskID + strCycle
				+ "000000.AVL.Z";
		String CHKFileName = strWorkPath + strTaskID + strCycle + "000000.CHK";
		File f = new File(strWorkPath+AVLFileName);

		long iFileSize = f.length();
		Date lastModified = new Date(f.lastModified());
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		String strDate = formatter.format(lastModified);
		System.out.println("strDATE=================================>"+strDate);
		String tmpLine = AVLFileName + "$" + iFileSize + "$" + cnt + "$" + strCycle + "$"
				+ strDate;
		try {
			FileWriter fw = new FileWriter(CHKFileName);
			fw.write(tmpLine);
			fw.flush();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

}
