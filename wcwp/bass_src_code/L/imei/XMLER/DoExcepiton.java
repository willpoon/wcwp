
import java.io.File;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;

import org.w3c.dom.Element;
import org.w3c.dom.Text;

/**
 * @author Administrator
 * 
 */
public class DoExcepiton {

	public String returnInfo(Element element) {
		String strinfo = "";
		Text textinfo = (Text) element.getFirstChild();
		if (textinfo != null)
			strinfo = textinfo.getNodeValue();
		return strinfo;
	}

	public void deleteFile(File file) {
		if (file.exists())
			file.delete();

	}

}
