import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.*;


public class test2 {
	
			public static String readInput() { 
		StringBuffer buffer = new StringBuffer(); 
		try { 
		FileInputStream fis = new FileInputStream("/bassdb1/etl/L/vgop/backup/i_13100_201005_VGOP-R1.6-11202_00.verf"); 
		InputStreamReader isr = new InputStreamReader(fis, "UTF8"); 
		Reader in = new BufferedReader(isr); 
		int ch; 
		while ((ch = in.read()) > -1) { 
			System.out.println((char)ch);
			System.out.println(ch);
		buffer.append((char)ch); 
		} 
		in.close(); 
		return buffer.toString(); 
		} catch (IOException e) { 
		e.printStackTrace(); 
		return null; 
		} 
		}
	
	public static void main(String[] args) {
		// 生成文件校验报告
		/*
		String fReportFileName = "/bassdb1/etl/L/vgop/report/1.txt";
		try {
			FileWriter fw = new FileWriter(fReportFileName);
			String tmpLine = "\n";
			fw.write(tmpLine);
			fw.flush();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		//writeOutput("�����");
		readInput();
	}



	
}
