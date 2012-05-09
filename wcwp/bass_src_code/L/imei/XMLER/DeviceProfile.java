
import java.io.File;

import java.io.FileOutputStream;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class DeviceProfile extends Thread {
	private Element root;

	private String fileName = "/DEVICE_PROFILE.del";

	private File file;

	private String time_id = "";

	public DeviceProfile(String outPath, String timeID, Element root) {
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
			//ByteBuffer cb = ByteBuffer.allocateDirect(120);
			//modify by zhaolp2 20090218
			ByteBuffer cb = ByteBuffer.allocateDirect(1240);
			NodeList DEVICE_PROFILE = root
					.getElementsByTagName("DEVICE_PROFILE");
			Element DEVICE_PROFILE_root = (Element) DEVICE_PROFILE.item(0);
			NodeList DEVICE_ID_P = DEVICE_PROFILE_root
					.getElementsByTagName("DEVICE_ID");
			NodeList PROPERTY_ID_P = DEVICE_PROFILE_root
					.getElementsByTagName("PROPERTY_ID");
			NodeList VALUE_P = DEVICE_PROFILE_root
					.getElementsByTagName("VALUE");
			NodeList VALUE_DESC_P = DEVICE_PROFILE_root
					.getElementsByTagName("VALUE_DESC");

			int size = VALUE_DESC_P.getLength();
			//StringBuffer sb = new StringBuffer(120);
			//modify by zhaolp2 20090218
			StringBuffer sb = new StringBuffer(1240);
			for (int i = 0; i < size; i++) {

				sb.append(time_id + ",");
				sb.append(de.returnInfo((Element) DEVICE_ID_P.item(i)) + ",");
				sb.append(de.returnInfo((Element) PROPERTY_ID_P.item(i)) + ",");
				sb.append(de.returnInfo((Element) VALUE_P.item(i)) + ",");
				sb.append(de.returnInfo((Element) VALUE_DESC_P.item(i)));
				sb.append("\r\n");

				cb.clear();
				cb.put(sb.toString().getBytes());
				cb.flip();
				sb.replace(0, sb.length(), "");
				fc.write(cb);

			}

			fos.close();
			fc.close();
			sb=null;
			// System.out.println("DEVICE_PROFILE.del done.");
		} catch (Exception e) {
			e.printStackTrace();
		}

		super.run();
	}

}
