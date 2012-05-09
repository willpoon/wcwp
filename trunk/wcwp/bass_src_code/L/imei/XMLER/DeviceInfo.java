
import java.io.File;

import java.io.FileOutputStream;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/*
 * 解析 DEVICE_INFO.del 
 * 包含的节点内容有:
 * DEVICE_ID/DEVICE_NAME * 
 * 
 * 
 */
public class DeviceInfo extends Thread {

	private Element root;

	private String fileName = "/DEVICE_INFO.del";

	private File file;

	private String time_id = "";

	/*
	 * 
	 */
	public DeviceInfo(String outPath, String timeID, Element root) {
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
			// PrintStream fos = new PrintStream(new BufferedOutputStream(
			// new FileOutputStream(file)), true);
			FileOutputStream fos = new FileOutputStream(file);
			FileChannel fc = fos.getChannel();
			ByteBuffer cb = ByteBuffer.allocateDirect(80);

			NodeList DEVICE_INFO = root.getElementsByTagName("DEVICE_INFO");
//System.out.println("start:");			
			Element DEVICE_INFO_Node = (Element) DEVICE_INFO.item(0);
			
			NodeList DEVICE_ID = DEVICE_INFO_Node
					.getElementsByTagName("DEVICE_ID");
			NodeList DEVICE_NAME = DEVICE_INFO_Node
					.getElementsByTagName("DEVICE_NAME");

			int size = DEVICE_ID.getLength();// 减少cpu的相应周期
			StringBuffer sb = new StringBuffer(80);
			for (int i = 0; i < size; i++) {

				sb
						.append(time_id + ","
								+ de.returnInfo((Element) DEVICE_ID.item(i))
								+ ","
								+ de.returnInfo((Element) DEVICE_NAME.item(i))
								+ "\r\n");
				cb.clear();
				cb.put(sb.toString().getBytes());
				cb.flip();
				sb.replace(0, sb.length(), "");
				fc.write(cb);
			}
			fos.close();
			fc.close();
			sb=null;
			// System.out.println("DEVICE_INFO.del done.");
		} catch (Exception e) {
			e.printStackTrace();
		}

		super.run();
	}

}
