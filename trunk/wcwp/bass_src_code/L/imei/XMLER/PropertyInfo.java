import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/*
 * 
 *
 */
public class PropertyInfo extends Thread {
	private Element root;

	private String fileName = "/PROPERTY_INFO.del";

	private File file;

	private String time_id = "";

	public PropertyInfo(String outPath, String timeID, Element root) {
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

			NodeList PROPERTY_INFO = root.getElementsByTagName("PROPERTY_INFO");
			Element PROPERTY_INFO_Node = (Element) PROPERTY_INFO.item(0);
			NodeList PARENT_PROP_ID = PROPERTY_INFO_Node
					.getElementsByTagName("PARENT_PROP_ID");
			NodeList PROPERTY_ID = PROPERTY_INFO_Node
					.getElementsByTagName("PROPERTY_ID");
			NodeList PROPERTY_NAME = PROPERTY_INFO_Node
					.getElementsByTagName("PROPERTY_NAME");
			NodeList VALUE_TYPE = PROPERTY_INFO_Node
					.getElementsByTagName("VALUE_TYPE");

			int size = PARENT_PROP_ID.getLength();

			StringBuffer sb = new StringBuffer(80);
			for (int i = 0; i < size; i++) {

				sb.append(time_id + ","
						+ de.returnInfo((Element) PROPERTY_ID.item(i)) + ","
						+ de.returnInfo((Element) PARENT_PROP_ID.item(i)) + ","
						+ de.returnInfo((Element) PROPERTY_NAME.item(i)) + ","
						+ de.returnInfo((Element) VALUE_TYPE.item(i)) + "\r\n");

				cb.clear();
				cb.put(sb.toString().getBytes());
				cb.flip();
				sb.replace(0, sb.length(), "");
				fc.write(cb);
			}

			fos.close();
			fc.close();
			sb=null;
			//System.out.println("PROPERTY_INFO.del done.");
		} catch (Exception e) {
			e.printStackTrace();
		}

		super.run();
	}

}
