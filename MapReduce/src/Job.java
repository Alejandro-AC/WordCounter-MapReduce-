
public abstract class Job implements Runnable {

	@Override
	public void run() {}
	
	public void addJobToInputQueue(Job job) {
		try {
			MapReduce.getInputBlockingQueue().put(job); //blocked if reaches its capacity, until consumer consumes
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void addJobToQueue(Job job) {
		try {
			MapReduce.getJobsBlockingQueue().put(job); //blocked if reaches its capacity, until consumer consumes
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
