import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class XMLER {

	public static void main(String[] args){
		new ToReadGroupXml().readXMLFile(args[0],args[1]);
		
	 }

	
	public static void copy(File source, File dest)  {
		FileChannel in = null, out = null;
		try { 
		in = new FileInputStream(source).getChannel();
		out = new FileOutputStream(dest).getChannel();

		long size = in.size();
		MappedByteBuffer buf = in.map(FileChannel.MapMode.READ_ONLY, 0, size);

		out.write(buf);
		buf.clear();
		buf=null;
		
		in.close();	
		out.close();
		in=null;
		out=null;
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		 finally {
		if (in != null)
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		if (out != null)
			try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	} 

}
