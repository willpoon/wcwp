import java.awt.event.ActionEvent;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.FingerClient;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import sun.net.TelnetInputStream;

public class vgopMain {

	// 加载接口文件
	public int LoadFile(String dataFileName) {
		
		DateFormat df = new SimpleDateFormat("yyyyMMdd");
		String strDoneTime = df.format(new Date());
		//数据仓库加载
		String[] commands = { "sh", "-c",
				"/bassdb1/etl/L/vgop/load_vgop.sh " + dataFileName + " >> /bassdb1/etl/L/vgop/log/loadinfo" + strDoneTime + ".log"};
		Process p;
		try {
			p = Runtime.getRuntime().exec(commands);
			p.waitFor();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//数据集市加载
		String[] dmkCommands = { "sh", "-c",
				"/bassdb1/etl/L/vgop/load_vgop_dmkdb.sh " + dataFileName + " >> /bassdb1/etl/L/vgop/log/loadinfo_dmkdb_" + strDoneTime + ".log"};
		try {
			p = Runtime.getRuntime().exec(dmkCommands);
			p.waitFor();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 0;
	}

	// 生成集团要求的文件及校验,并上传至集团服务器
	public String FileCheck(String strFileName, String configFileName) {
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		String strDoneTime = df.format(new Date());
		String strResultCode = "00";

		TaskConfig tc = new TaskConfig(configFileName);
		String reportPath = tc.getReportPath();
		String localBackupPath = tc.getLocalBackupPath();
		int iDatePoint = Integer.parseInt(tc.getDatePoint());
		int iInterfaceCodePoint = Integer.parseInt(tc.getInterfaceCodePoint());
		String verfFileName = strFileName
				.substring(0, strFileName.length() - 8)
				+ ".verf";
		File backupDataFile = new File(localBackupPath + strFileName);
		File backupVerfFile = new File(localBackupPath + verfFileName);

		// 通过数据文件名分出数据日期，接口代码
		String[] dataColumn = strFileName.split(tc.getRegex());
		// 接口文件名中得到的数据日期
		String strFileDate = dataColumn[iDatePoint];
		// 接口文件名种得到的接口号
		String strFileInterfaceCode = dataColumn[iInterfaceCodePoint].substring(dataColumn[iInterfaceCodePoint].length()-5,dataColumn[iInterfaceCodePoint].length());

		// 通过校验文件名分出数据日期，接口代码
		String[] verfNameColumn = verfFileName.split(tc.getRegex());
		
		// 校验文件名中得到的数据日期
		String strVerfNameDate = verfNameColumn[iDatePoint];
		// 校验文件名中得到的接口号
		String strVerfNameInterfaceCode = verfNameColumn[iInterfaceCodePoint].substring(verfNameColumn[iInterfaceCodePoint].length()-5,verfNameColumn[iInterfaceCodePoint].length());

		// 数据文件大小
		long iFileLength = backupDataFile.length();

		// 获取文件行数(记录数)
		long iFileRows = this.readFileRecord(localBackupPath + strFileName);

		// 如果校验文件存在，读取校验文件中的内容
		long verfDataFileLength = 0;
		String verfDataFileName = "";
		long verfDataFileRows = 0;
		String verfDataFileDate = "";
		String verfDataFileTime = "";
		String[] verfColumn = null;
		System.out.println(strFileName);
		if (backupVerfFile.exists()) {
			String verfContent = readFile(localBackupPath + verfFileName);
			//System.out.println(verfContent);
			verfColumn = verfContent.split("�");
			// 检验文件中存放的各字段内容
			System.out.println(verfColumn.length);
			if (verfColumn.length==5) {
				// 文件名称
				verfDataFileName = verfColumn[0];
				// 文件大小
				verfDataFileLength = Long.parseLong(verfColumn[1]);
				// 文件行数
				verfDataFileRows = Long.parseLong(verfColumn[2]);
				// 数据日期
				verfDataFileDate = verfColumn[3];
				// 文件生成时间
				verfDataFileTime = verfColumn[4];
			}
		}

		// 接口文件名与规则不符01
		if (!verfDataFileName.equals(strFileName)) {
			strResultCode = "01";
		}

		// 接口文件名称不存在02

		if (!backupDataFile.exists()) {
			strResultCode = "02";
		}

		// 接口数据文件无法打开03
		if (!backupDataFile.canRead()) {
			strResultCode = "03";
		}

		// 文件大小不符05
		if (iFileLength != verfDataFileLength) {
			strResultCode = "05";
		}

		// 文件记录数不符合06
		if (iFileRows != verfDataFileRows) {
			strResultCode = "06";
		}

		// 文件数据日期不符 07
		if (!strFileDate.equals(verfDataFileDate)) {
			//System.out.println(strFileDate);
			//System.out.println(verfDataFileDate);
			strResultCode = "07";
		}

		// 数据文件数据日期非法 08
		String datePattern = "";
		if (tc.getCycle().equals("MONTH")) {
			datePattern = "[1-9][0-9]{3}[0-1][0-9]";
		} else if (tc.getCycle().equals("DAY")) {
			datePattern = "[1-9][0-9]{3}[0-1][0-9][0-3][0-9]";
		} else {
			datePattern = "[1-9][0-9]{3}[0-1][0-9][0-3][0-9][0-2][0-9]";
		}
		Pattern pattern = Pattern.compile(datePattern);
		Matcher matcher = pattern.matcher(strFileDate);
		if (!matcher.matches()) {
			strResultCode = "08";
		}

		// 数据文件接口单元编码非法 10
		pattern = Pattern.compile(tc.getCodeReg());
		matcher = pattern.matcher(strFileInterfaceCode);
		if (!matcher.matches()) {
			strResultCode = "10";
		}

		// 数据文件记录非法结束符 11

		// 数据文件大小超过2000000000 12
		if (iFileLength > 2000000000) {
			strResultCode = "12";
		}

		// 接口数据文件重复上传

		// 数据文件数据日期与期待日期不符

		// 校验文件记录格式错误
		if (verfColumn == null || verfColumn.length != 5) {
			strResultCode = "16";
		}

		// 校验文件数据日期与期待日期不符

		// 校验文件重复上传

		// 校验文件接口单元编码非法
		pattern = Pattern.compile(tc.getCodeReg());
		//System.out.println(strVerfNameInterfaceCode);
		matcher = pattern.matcher(strVerfNameInterfaceCode);
		if (!matcher.matches()) {
			strResultCode = "94";
		}

		// 校验文件记录非法结束符(非回车换行)

		// 校验文件数据日期非法
		datePattern = "";
		if (tc.getCycle().equals("MONTH")) {
			datePattern = "[1-9][0-9]{3}[0-1][0-9]";
		} else if (tc.getCycle().equals("DAY")) {
			datePattern = "[1-9][0-9]{3}[0-1][0-9][0-3][0-9]";
		} else {
			datePattern = "[1-9][0-9]{3}[0-1][0-9][0-3][0-9][0-2][0-9]";
		}
		pattern = Pattern.compile(datePattern);
		matcher = pattern.matcher(strFileDate);
		if (!matcher.matches()) {
			//System.out.println(strFileDate);
			strResultCode = "97";
		}

		// 校验文件记录长度不符

		// 校验文件无法打开
		if (!backupVerfFile.canRead()) {
			strResultCode = "99";
		}

		// 生成文件校验报告

		
		
		String fReportFileName = reportPath + "f_"
				+ strFileName.substring(0, strFileName.length() - 4) + ".verf";
				
		String[] commands = { "sh", "-c",
				"/bassdb1/etl/L/vgop/write_file.sh " + " " + fReportFileName + " " + strFileName + " " + strDoneTime + " " + strResultCode};
		Process p;
		try {
			p = Runtime.getRuntime().exec(commands);
			p.waitFor();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*				
		try {
			FileWriter fw = new FileWriter(fReportFileName);
			String tmpLine = strFileName + (char)0x80 + strDoneTime + "�"
					+ strResultCode + "\n";
			fw.write(tmpLine);
			fw.flush();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		// 如果文件校验未通过，多生成一个带error的文件
		if (!strResultCode.equals("00")) {
			String errorReportFileName = reportPath + "f_"
					+ strFileName.substring(0, strFileName.length() - 4)
					+ ".verf" + ".error";
					
								
			String[] commands2 = { "sh", "-c","/bassdb1/etl/L/vgop/write_file.sh " + " " + errorReportFileName + " " + strFileName + " " + strDoneTime + " " + strResultCode};
			try {
				p = Runtime.getRuntime().exec(commands2);
				p.waitFor();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		
		}

		return strResultCode;
	}
	
	
    public boolean uploadFile(String strFileName,String configFileName,int strType){   
        BufferedInputStream inStream = null;   
        boolean success = false;   
		TaskConfig tc = new TaskConfig(configFileName);

		String server = tc.getServer();
		String user = tc.getUser();
		String password = tc.getPassword();
		String remotePath = tc.getRemotePath();
		String reportPath = tc.getReportPath();
		String remoteReportPath = tc.getRemoteReportPath();
		
		String fReportFileName = "f_"
		+ strFileName.substring(0, strFileName.length() - 4) + ".verf";
		String rReportFileName = "r_"
		+ strFileName.substring(0, strFileName.length() - 8) + ".verf";
		 
        try {   
        	FTPClient ftpClient = new FTPClient();

			ftpClient.connect(server, 21);
			ftpClient.setControlEncoding("UTF-8");
			ftpClient.login(user, password);
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			ftpClient.changeWorkingDirectory(remotePath);
		if (strType ==  1) {			
            		inStream = new BufferedInputStream(new FileInputStream(reportPath+fReportFileName));   
            		success = ftpClient.storeFile(remoteReportPath+fReportFileName, inStream);   
            	}
            	if (strType ==  2) {
	            inStream = new BufferedInputStream(new FileInputStream(reportPath+rReportFileName));   
	            success = ftpClient.storeFile(remoteReportPath+rReportFileName, inStream);  
	        }
            ftpClient.logout();
            ftpClient.disconnect();
        } catch (FileNotFoundException e) {            
            e.printStackTrace();               
        } catch (IOException e){   
            e.printStackTrace();   
        }finally{   
            if(inStream != null){   
                try {   
                    inStream.close();   
                } catch (IOException e) {                  
                    e.printStackTrace();   
                }   
            }   

        }   
        return success;   
    }
    
    public boolean moveFile(String strFileName,String configFileName) {
    	
        boolean success = false;   
		TaskConfig tc = new TaskConfig(configFileName);

		String server = tc.getServer();
		String user = tc.getUser();
		String password = tc.getPassword();
		String remotePath = tc.getRemotePath();
		String remoteReportPath = tc.getRemoteReportPath();
		String remoteBackupPath = tc.getRemoteBackupPath();
		String remoteReportBackupPath = tc.getRemoteReportBackupPath();
		String verfFileName = strFileName.substring(0, strFileName.length() - 8)+ ".verf";
		String fReportFileName = "f_"
			+ strFileName.substring(0, strFileName.length() - 4) + ".verf";
		String rReportFileName = "r_"
			+ strFileName.substring(0, strFileName.length() - 8) + ".verf";		
		
        try {   
			FTPClient ftpClient = new FTPClient();
			ftpClient.connect(server, 21);
			ftpClient.setControlEncoding("UTF-8");
			ftpClient.login(user, password);
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			ftpClient.changeWorkingDirectory(remotePath);
			ftpClient.rename(remotePath+strFileName, remoteBackupPath+strFileName);
			ftpClient.rename(remotePath+verfFileName, remoteBackupPath+verfFileName);
			ftpClient.rename(remoteReportPath+fReportFileName, remoteReportBackupPath+fReportFileName);
			ftpClient.rename(remoteReportPath+rReportFileName, remoteReportBackupPath+rReportFileName);
        } catch (FileNotFoundException e) {            
            e.printStackTrace();               
        } catch (IOException e){   
            e.printStackTrace();   
        }finally{   
            
        }   
        return success;  
    }
    

	// 生成集团要求的记录及校验，并上传至集团服务器
	public int RecordCheck(String strFileName, String configFileName) {
		TaskConfig tc = new TaskConfig(configFileName);
		String reportPath = tc.getReportPath();

		String rReportFileName = reportPath + "r_"
				+ strFileName.substring(0, strFileName.length() - 8) + ".verf";
				
		String strLineNum = "-1";
		
		String errorCode = "000000000";
		
		String[] commands = { "sh", "-c",
				"/bassdb1/etl/L/vgop/write_file.sh " + " " + rReportFileName + " " + strFileName + " " + strLineNum + " " + errorCode};
		Process p;
		try {
			p = Runtime.getRuntime().exec(commands);
			p.waitFor();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
		/*
		try {
			FileWriter fw = new FileWriter(rReportFileName);
			String tmpLine = "\n";
			fw.write(tmpLine);
			fw.flush();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		return 0;
	}

	// 读取文件内容
	public String readFile(String path) {
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

	// 读取文件记录条数
	public long readFileRecord(String path) {
		long iCounts = 0;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(path));
			while ((reader.readLine()) != null) {
				iCounts = iCounts + 1;
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
		return iCounts;
	}

	// 从集团服务器通过FTP获取VGOP接口文件
	public int getPutFile(String configFileName) {

		TaskConfig tc = new TaskConfig(configFileName);

		String server = tc.getServer();
		String user = tc.getUser();
		String password = tc.getPassword();
		String remotePath = tc.getRemotePath();
		String regName = tc.getRegName();
		String localPath = tc.getLocalPath();
		String localBackupPath = tc.getLocalBackupPath();
		String reportPath = tc.getReportPath();
		//System.out.println(server);

		try {
			FTPClient ftpClient = new FTPClient();

			ftpClient.connect(server, 21);
			ftpClient.setControlEncoding("UTF-8");
			ftpClient.login(user, password);
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			ftpClient.changeWorkingDirectory(remotePath);

			String[] files = ftpClient.listNames(regName);

			for (int i = 0; i < files.length; i++) {
				//System.out.println(files[i]);
				String dataFileName = files[i];
				String verfFileName = dataFileName.substring(0, dataFileName
						.length() - 8)
						+ ".verf";
				File localDataFile = new File(localPath + dataFileName);
				File backupDataFile = new File(localBackupPath + dataFileName);
				File localVerfFile = new File(localPath + verfFileName);
				File backupVerfFile = new File(localBackupPath + verfFileName);
				if (!localDataFile.exists() && !backupDataFile.exists()) {

					// 获取数据文件
					// File file = new File(localDataFile);
					FileOutputStream dataFos = new FileOutputStream(
							localDataFile);
					ftpClient.retrieveFile(dataFileName, dataFos);
					dataFos.close();
					localDataFile.renameTo(backupDataFile);
					//删除服务器数据文件
					ftpClient.dele(dataFileName);
				}

				if (!localVerfFile.exists() && !backupVerfFile.exists()) {

					// 获取校验文件
					FileOutputStream verfFos = new FileOutputStream(
							localVerfFile);
					ftpClient.retrieveFile(verfFileName, verfFos);
					verfFos.close();
					localVerfFile.renameTo(backupVerfFile);
					//删除服务器校验文件
					ftpClient.dele(verfFileName);
					

				}

				// 如果数据文件和校验文件都存在，执行文件加载操作
				if(backupDataFile.exists() && backupVerfFile.exists()) {
					this.LoadFile(dataFileName); 
				}
				 
				
				String fReportFileName = "f_"
				+ dataFileName.substring(0, dataFileName.length() - 4) + ".verf";
				String rReportFileName = "r_"
				+ dataFileName.substring(0, dataFileName.length() - 8) + ".verf";
				
				File fReportFile = new File(reportPath + fReportFileName);
				File rReportFile = new File(reportPath + rReportFileName);
				
				if (!fReportFile.exists())  {
					// 生成文件级校验
					this.FileCheck(dataFileName, configFileName);
					//上传校验文件
					this.uploadFile(dataFileName, configFileName,1);
				}
				
				if (!rReportFile.exists())  {
					// 生成记录及校验
					this.RecordCheck(dataFileName, configFileName);
					//上传校验文件
					this.uploadFile(dataFileName, configFileName,2);
				}
				
				

				
				//将服务器上文件备份至其他目录
				//this.moveFile(dataFileName, configFileName);
				
			}

			ftpClient.logout();
			ftpClient.disconnect();
			return 0;

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	public static String[] tokenizeToStringArray(String str, String delimiters,
			boolean trimTokens, boolean ignoreEmptyTokens) {

		if (str == null) {
			return null;
		}
		StringTokenizer st = new StringTokenizer(str, delimiters);
		List tokens = new ArrayList();
		while (st.hasMoreTokens()) {
			String token = st.nextToken();
			if (trimTokens) {
				token = token.trim();
			}
			if (!ignoreEmptyTokens || token.length() > 0) {
				tokens.add(token);
			}
		}
		return toStringArray(tokens);
	}

	public static String[] toStringArray(Collection collection) {
		if (collection == null) {
			return null;
		}

		return (String[]) collection.toArray(new String[collection.size()]);
	}

	public static String readInput(String path) {
		StringBuffer buffer = new StringBuffer();
		try {
			FileInputStream fis = new FileInputStream(path);
			InputStreamReader isr = new InputStreamReader(fis, "GB18030");
			Reader in = new BufferedReader(isr);
			int ch;
			while ((ch = in.read()) > -1) {
				//System.out.println((char) ch);
				//System.out.println(ch);
				buffer.append((char) ch);
			}
			in.close();
			return buffer.toString();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) {

		if( args.length > 0 ) {
			System.out.println("the program no parameters");
			return;
		}
		
		vgopMain ftpp = new vgopMain();
		CommonConfig cc = new CommonConfig();
		String[] strTaskLists = cc.getProperties("taskList").split(",");
		//ftpp.getPutFile("vgopMonth.properties");
		// ftpp.FileCheck("aaa", "vgopMonth.properties");
		
		String strWorkPath = cc.getProperties("workPath");
		
		File fStopFlagFile = new File(strWorkPath + "stop_vgop_flag");
		try {
		 	fStopFlagFile.delete();
		} catch (Exception e) { 
		 	// TODOAuto-generated catch block 
		 	e.printStackTrace(); 
		}
		

		
		 while(true) { 
		 	
		 	if(fStopFlagFile.exists()) {
		 		System.out.println("stop_vgop_flag is exists,program exit!");
		 		break;
		 	}
		 	
		 	for(int i = 0 ; i < strTaskLists.length ; i++) { 
		 		String configFileName = strTaskLists[i]; 
		 		ftpp.getPutFile(configFileName); 
		 	} 
		 	try {
		 		System.out.println("程序等待");
		 		Thread.sleep(60000); 
		 	} catch (InterruptedException e) { 
		 		// TODOAuto-generated catch block 
		 		e.printStackTrace(); 
		 	}
		 }
		 
		// String tmpstr="i_13100_201005_VGOP-R1.6-13201_00_001.dat";
		// System.out.println(tmpstr.substring(2,tmpstr.length()-8));
		// String abc =
		// "i_13100_201005_VGOP-R1.6-13201_00_001.dat€11585370€131185€201005€20100609170617";
		// String[] a = abc.split("€");
		//
		// int i = '€';
		// System.out.println(i);
	}
}
