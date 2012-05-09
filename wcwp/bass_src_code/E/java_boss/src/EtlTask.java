import java.io.FileWriter;

class EtlTask implements Runnable {
	private String strSQL;
	private String strArea;
	private String strTaskID;
	private String strModule;
	private String strCycle;
	private String owner1;
	private String owner2;

	public EtlTask(String strSQL, String strArea, String strTaskID,
			String strModule, String strCycle,String owner1,String owner2) {

		//this.strSQL = strSQL;
		this.strArea = strArea;
		this.strSQL = strSQL;
		this.strTaskID = strTaskID;
		this.strModule = strModule;
		this.strCycle = strCycle;
		this.owner1=owner1;
		this.owner2=owner2;
	}

	public void run() {
		// 获取接口配置参数
		ETLWrite ew = new ETLWrite();
		strSQL = strSQL.replaceAll("\\$FEE_AREA\\$", strArea);
		if(strArea.equals("891")||strArea.equals("897"))
			strSQL = strSQL.replaceAll("\\$OWNER\\$", owner1);
		else
			strSQL = strSQL.replaceAll("\\$OWNER\\$", owner2);
		System.out.println("strSQL==============>" + strSQL);
		System.out.println("strArea=========>" + strArea);
		int cnt = ew.writeInterfaceFile(strSQL, strArea, strTaskID, strModule,
				strCycle);
		TaskConfig tc = new TaskConfig(strTaskID);
		synchronized(TaskConfig.class) {
			tc.setProperties("CNT"+strArea, String.valueOf(cnt));
		}
		
		
		
	}
}
