import org.w3c.dom.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.PrintStream;




/*
 * 解析 CONTROL_INFO.del 文件
 * 包含的节点内容有:
 * SENDER/RECEIVER/SEQ/DATA_EFFC_TIME/CREATE_TIME/
 * OPERATION_CODE/TEST_FLAG * 
 * 
 * 
 */
public class SENDER extends Thread {

	private Element root;
	private String fileName = "/CONTROL_INFO.del";
	private File file;
	private String time_id = "";

	public SENDER(String outPath, String timeID, Element root) {
		this.root = root;
		this.time_id = timeID;
		file = new File(outPath + fileName);// 最后保存的文件
	}

	
	public void run() {
		DoExcepiton de = new DoExcepiton();
		// 创建前先删除文件
		if (file.exists())
			de.deleteFile(file);
		try {
			PrintStream fos = new PrintStream(new BufferedOutputStream(
					new FileOutputStream(file)), true);

			NodeList SENDER = root.getElementsByTagName("SENDER");
			NodeList RECEIVER = root.getElementsByTagName("RECEIVER");

			NodeList SEQ = root.getElementsByTagName("SEQ");

			NodeList DATA_EFFC_TIME = root
					.getElementsByTagName("DATA_EFFC_TIME");

			NodeList CREATE_TIME = root.getElementsByTagName("CREATE_TIME");

			NodeList OPERATION_CODE = root
					.getElementsByTagName("OPERATION_CODE");

			NodeList TEST_FLAG = root.getElementsByTagName("TEST_FLAG");

			StringBuffer sbf = new StringBuffer();
			sbf.append(time_id + ","
					+ de.returnInfo((Element) SENDER.item(0)) + ",");
			sbf.append(de.returnInfo((Element) RECEIVER.item(0)) + ",");
			sbf.append(de.returnInfo((Element) SEQ.item(0)) + ",");
			sbf.append(de.returnInfo((Element) DATA_EFFC_TIME.item(0)) + ",");
			sbf.append(de.returnInfo((Element) CREATE_TIME.item(0)) + ",");
			sbf.append(de.returnInfo((Element) OPERATION_CODE.item(0)) + ",");
			sbf.append(de.returnInfo((Element) TEST_FLAG.item(0)) + "\r\n");
			fos.print(sbf.toString());
			fos.close();// 关闭
			
			//System.out.println("CONTROL_INFO.del done.");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		super.run();
	}

}
