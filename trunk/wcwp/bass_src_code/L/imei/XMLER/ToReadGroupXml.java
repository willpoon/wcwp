

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.PrintStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class ToReadGroupXml {

	public ToReadGroupXml() {
	}

	public void readXMLFile(String FileName, String outFilePath) {
		DoExcepiton de = new DoExcepiton();
		
		String time_id = FileName.substring(8, 14);// 取得日期
		FileName = outFilePath+"/"+FileName;

		String realAdd = outFilePath +"/"+ time_id;// 输出路径
		File file = new File(realAdd);
		file.mkdir();// 创建对应目录	
		try {			
			//内存中生成XML树
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(FileName);
			Element root = doc.getDocumentElement();		
			
			
			new DeviceProfile(realAdd,time_id,root).start();
			
			new DeviceInfo(realAdd,time_id,root).start();
			new PropertyValueRange(realAdd,time_id,root).start();
			new SENDER(realAdd,time_id,root).start();			
			new PropertyInfo(realAdd,time_id,root).start();
			
			
			
			

		} catch (Exception ee) {
			//System.out.println("解析xml文件或者生成数据时发生错误:");
			ee.printStackTrace();
		} finally {

		}
	}

}