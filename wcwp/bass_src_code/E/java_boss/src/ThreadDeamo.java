
public class ThreadDeamo{
	
	public static int tickets = 100;
	 public static void main(String[] args){
	  ThreadTest t = new ThreadTest();
	  new Thread(t).start();
	  new Thread(t).start();
	  new Thread(t).start();
	  new Thread(t).start();
	  while(true) {
		  if(ThreadDeamo.tickets < 50) {
			  System.out.println("aaaaaaaaaaaaaa==========");
		  } else {
			  System.out.println(tickets);
		  }
		  try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	  }
	}
}
	 
	class ThreadTest implements Runnable{
	 public void run(){
	  while (true){
	   if(ThreadDeamo.tickets>0) {
		   System.out.println(Thread.currentThread().getName()+" is saling ticket "+ThreadDeamo.tickets--);
	   } else {
		   break;
	   }
	  }
	 }
	}