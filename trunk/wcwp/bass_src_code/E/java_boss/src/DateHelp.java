import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Vector;

public class DateHelp {
	
	private Calendar cal = null;
	
	public DateHelp() {
		cal = Calendar.getInstance();
	}
	
	public Calendar set(int year,int month,int day,int hour,int minute,int second) {
		cal.set(year,month,day,hour,minute,second);
		return cal;
	}
	
	public String getCurrentDate() {
		Date today = cal.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		String preTime  = format.format(today);
		return preTime;
	}
	
	public String getCurrentMonth(String strFormat) {
		Date today = cal.getTime();
		SimpleDateFormat format = new SimpleDateFormat(strFormat);
		String currentMonth  = format.format(today);
		return currentMonth;
	}
	
	public Calendar getPreCalendar() {
		cal.add(Calendar.DATE,-cal.getMaximum(cal.DAY_OF_MONTH));
		return cal;
	}
	
	public String getPreMonth() {
		getPreCalendar();
		Date today = cal.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
		String preTime  = format.format(today);
		return preTime;
	}
	
	public String getPreMonth(int i) {
		for(int j = 0 ; j < i ; j++) {
			getPreCalendar();
		}
		Date today = cal.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
		String preTime  = format.format(today);
		return preTime;
	}
	
	public String getNextMonth() {
		cal.add(Calendar.DATE,cal.getMaximum(cal.DAY_OF_MONTH));
		Date today = cal.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMM");
		String nextMonth  = format.format(today);
		return nextMonth;
	}
	
	public String getNextDay() {
		cal.add(Calendar.DATE,1);
		Date today = cal.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		String nextDay  = format.format(today);
		return nextDay;
	}
	
	public static void main(String[] args) {
		DateHelp dh = new DateHelp();
		/*
		System.out.println(dh.getCurrentMonth("yyyyMM"));
		System.out.println(dh.getCurrentDate());
		System.out.println(dh.getNextMonth());
		System.out.println(dh.getPreMonth());
		System.out.println(dh.getNextDay());
		*/
		System.out.println(new Timestamp(System.currentTimeMillis()).toString());
		System.out.println(new Timestamp(System.currentTimeMillis()).toString().substring(11,16).replace(':', ' '));
		System.out.println(new Timestamp(System.currentTimeMillis()).toString().substring(11,13));
		System.out.println(new Timestamp(System.currentTimeMillis()).toString().substring(14,16));
		

	}
	

	
}