import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

/**
 * ftpclientÏÂÔØÎÄ¼þµÄÀý×Ó
 * 
 * @author ÀÏ×ÏÖñ(JavaÊÀ¼ÍÍø,java2000.net)
 */
public class Test {
	
	
	
		public static String encodin = "GB18030" ;
		public static String encodout = "UTF8" ;
		static void writeOutput(String str) { 
		try { 
		FileOutputStream fos = new FileOutputStream("test.txt"); 
		Writer out = new OutputStreamWriter(fos, encodout); 
		out.write(str); 
		out.close(); 
		} catch (IOException e) { 
		e.printStackTrace(); 
		} 
		} 
		public static String readInput() { 
		StringBuffer buffer = new StringBuffer(); 
		try { 
		FileInputStream fis = new FileInputStream("/bassdb1/etl/L/vgop/backup/i_13100_201005_VGOP-R1.6-13201_00.verf"); 
		InputStreamReader isr = new InputStreamReader(fis, encodin); 
		Reader in = new BufferedReader(isr); 
		int ch; 
		while ((ch = in.read()) > -1) { 
			System.out.print((char)ch);
			System.out.print(ch);
		buffer.append((char)ch); 
		} 
		in.close(); 
		return buffer.toString(); 
		} catch (IOException e) { 
		e.printStackTrace(); 
		return null; 
		} 
		}

	
	
	public static String[] tokenizeToStringArray( String str, String delimiters, boolean trimTokens,
			boolean ignoreEmptyTokens )
	{

		if ( str == null )
		{
			return null;
		}
		StringTokenizer st = new StringTokenizer( str, delimiters );
		List tokens = new ArrayList();
		while ( st.hasMoreTokens() )
		{
			String token = st.nextToken();
			if ( trimTokens )
			{
				token = token.trim();
			}
			if ( !ignoreEmptyTokens || token.length() > 0 )
			{
				tokens.add( token );
			}
		}
		return toStringArray( tokens );
	}	
	
	public static String[] toStringArray( Collection collection )
	{
		if ( collection == null )
		{
			return null;
		}

		return (String[]) collection.toArray( new String[collection.size()] );
	}	
	
	
	public String readFile(String path) throws UnsupportedEncodingException {
		String content = "";
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(path));
			String line;
			while ((line = reader.readLine()) != null) {
				content += line + "\n";
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return content;
	}
	
	

	
	
	



	
	public static void main(String[] args) throws UnsupportedEncodingException {
			/*
			try {
				FileInputStream fin = new FileInputStream("/bassdb1/etl/L/vgop/backup/i_13100_201005_VGOP-R1.6-13201_00.verf") ;
	            BufferedInputStream bis = new BufferedInputStream(fin) ;				
	  	  
	      byte[]   bytes=new   byte[1024];   
	      int   c;   
	      while((c=bis.read(bytes))!=-1)   {
	            System.out.print((char)c);
	      }
	      bis.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
			
			*/
		//String a = new Test().readFile("/bassdb1/etl/L/vgop/backup/i_13100_201005_VGOP-R1.6-13201_00.verf");
		String b = "i_13100_201005_VGOP-R1.6-13201_00_001.dat€11585370€131185€201005€20100609170617";
		String a = readInput();
		System.out.println(a);
		String[] aa = a.split("€");
		System.out.println(aa.length);
		System.out.println(a.split("€")[0]);
		System.out.println(tokenizeToStringArray(a,"€",true,true)[0]);
		System.out.println(tokenizeToStringArray(b,"€",true,true)[0]);
		System.out.println(b.split("€")[0]);
	}
}
