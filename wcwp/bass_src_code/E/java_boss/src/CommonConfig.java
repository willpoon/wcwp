import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;

public class CommonConfig {

	public String getProperties(String strKey) {

		InputStream inputStream = this.getClass().getClassLoader()
				.getResourceAsStream("common.properties");
		Properties p = new Properties();
		try {
			p.load(inputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return p.getProperty(strKey);

	}

	public int setProperties(String strKey, String strValue) {

		InputStream inputStream = this.getClass().getClassLoader()
				.getResourceAsStream("common.properties");
		Properties p = new Properties();
		try {

			p.load(inputStream);
			p.setProperty(strKey, strValue);

			String filename = "D:\\workspace\\crm_interface\\bin\\config\\common.properties";
			FileOutputStream out = new FileOutputStream(filename);
			p.store(out, "");
			out.flush();
			out.close();
			inputStream.close();

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		return 0;
	}
	
	public static void main(String[] args) {
		CommonConfig cc = new CommonConfig();
		String execPath = cc.getProperties("configPath");
		cc.setProperties("a", "1111");
		System.out.println(execPath);
	}
	
	
}
