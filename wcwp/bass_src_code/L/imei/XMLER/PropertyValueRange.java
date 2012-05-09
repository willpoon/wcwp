
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
public class PropertyValueRange extends Thread {
	private Element root;

	private String fileName = "/PROPERTY_VALUE_RANGE.del";

	private File file;

	private String time_id = "";

	public PropertyValueRange(String outPath, String timeID, Element root) {
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
//			PrintStream fos = new PrintStream(new BufferedOutputStream(
//					new FileOutputStream(file)), true);
			
			FileOutputStream fos = new FileOutputStream(file);
			FileChannel fc = fos.getChannel();
			ByteBuffer cb = ByteBuffer.allocateDirect(80);

			NodeList PROPERTY_VALUE_RANGE = root
					.getElementsByTagName("PROPERTY_VALUE_RANGE");
			StringBuffer sb = new StringBuffer(80);
			for (int j = 0; j < PROPERTY_VALUE_RANGE.getLength(); j++) {
				Element tmp = (Element) PROPERTY_VALUE_RANGE.item(j);

				NodeList VALUE_ALLOWED_CODE = tmp
						.getElementsByTagName("VALUE_ALLOWED_CODE");
				NodeList VALUE_ALLOWED = tmp
						.getElementsByTagName("VALUE_ALLOWED");
				int size = VALUE_ALLOWED_CODE.getLength();
			
				for (int i = 0; i < size; i++) {
				
					sb.append(time_id + ",");
					sb.append(de.returnInfo((Element) VALUE_ALLOWED_CODE
							.item(i))
							+ ",");
					sb.append(de.returnInfo((Element) VALUE_ALLOWED.item(i)));
					sb.append("\r\n");
					cb.clear();
					cb.put(sb.toString().getBytes());
					cb.flip();
					sb.replace(0, sb.length(), "");
					fc.write(cb);
				}
			}
			fos.close();
			fc.close();
			sb=null;
		//	System.out.println("PROPERTY_VALUE_RANGE.del done.");

		} catch (Exception e) {
			e.printStackTrace();
		}

		super.run();
	}

}
