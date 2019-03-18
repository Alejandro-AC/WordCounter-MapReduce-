import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;



class MapReduce implements Runnable {

private static String[] filenameArr;

private static ConcurrentMap<String, Integer> resultMap = new ConcurrentHashMap<>();

private static final int CONSUMER_COUNT = Runtime.getRuntime().availableProcessors() ;
private static final int INPUT_BLOCKING_QUEUE_CAPACITY = 10;

private final static BlockingQueue<Job> InputBlockingQueue = new ArrayBlockingQueue<Job>(INPUT_BLOCKING_QUEUE_CAPACITY);
private final static BlockingQueue<Job> JobsBlockingQueue = new LinkedBlockingQueue<Job>();


private boolean isConsumer = false;
private static boolean producerIsDone = false;

private static boolean minimizeMemoryUsage = false;
private static boolean outputPerFile = true;

	MapReduce(boolean consumer) {
	    this.isConsumer = consumer;
	}
	
	MapReduce(String[] filenames) {
		filenameArr = filenames;
		initialize();
	}
	
	public void initialize() {
		
		long startTime = System.nanoTime();
        
        ExecutorService producerPool = Executors.newFixedThreadPool(1);
        producerPool.submit(new MapReduce(false)); // run method is called
        
        // create a pool of consumer threads to take jobs
        // ExecutorService consumerPool = Executors.newFixedThreadPool(CONSUMER_COUNT);
        ExecutorService consumerPool = Executors.newWorkStealingPool();
        
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            consumerPool.submit(new MapReduce(true)); // run method is called
        }

        producerPool.shutdown();
        consumerPool.shutdown();

       
        
        while (!producerPool.isTerminated() && !consumerPool.isTerminated()) {
        }
        
        long endTime = System.nanoTime();
        long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
        System.out.println("\n\nTotal elapsed time: " + elapsedTimeInMillis + " ms");

	}
	
    
	@Override
	public void run() {		
        if (isConsumer) {
            consume();
            
        } else { 
        	for (String filename : filenameArr) {
        		
        		WordSplitter ws = new WordSplitter();
        		ws.split(filename); 
        		
        		try {
					Thread.sleep(50);				
	            	while (!JobsBlockingQueue.isEmpty()) {   
						Thread.sleep(50);					
	            	}
        		} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
        		
        		if (outputPerFile) {
        			printResultMap(filename);
            		resultMap.clear();
        		}
            	        		
        		
        	}    
        	
        	if (!outputPerFile) {
        		printResultMap("Combined");
        	}
            
        }
	}
	
	private void consume() {
        try {
        	
        	
        	while (!producerIsDone || (producerIsDone && !JobsBlockingQueue.isEmpty())) {
        		
		    	Job job;   		    	
		    	
		    	if (minimizeMemoryUsage) {	
		    		
			    	if (JobsBlockingQueue.isEmpty()) {		    		
			    		job = InputBlockingQueue.take();
			    	}else {
			    		job = JobsBlockingQueue.take();
		    		}
			    	
		    	}else {
		    		//System.out.println(JobsBlockingQueue);
		    		//System.out.println(Thread.currentThread().getName() + " takes job");
		    		job = JobsBlockingQueue.take();
		    		//System.out.println(JobsBlockingQueue);
		    	}	    		    	

				
				job.run();
				
        	}

        } catch (Exception e) {
            e.printStackTrace();
        }
                
        
    }
	
    

    
    /*public Map<String, Integer> wordReduceSimple(List<String> wordList){
    	Map<String, Integer> wordMap = new HashMap<>();
    	for (String word : wordList) {
    		// Java 8 elegant way
			wordMap.merge(word, 1, (x, y) -> x + y);
    	}
    	
    	return wordMap;
    }*/
    
    /*public Map<String, Integer> wordReduce(List<List<String>> wordLists){
    	Map<String, Integer> wordMap = new HashMap<>();
    	
    	for (List<String> wordList : wordLists) {
    		for(String word : wordList) {
    			// Java 8 compact way
    			//wordMap.put(word, wordMap.getOrDefault(word, 0) + 1);
    			// Java 8 elegant way
    			wordMap.merge(word, 1, (x, y) -> x + y);
    		}
    	}
    	
    	return wordMap;
    }*/
        

    
    private void printResultMap(String filename) {
    	System.out.println("\n" + filename + ":");
    	
    	resultMap.forEach((key, value) -> {
    	    System.out.println(key + " : " + value);
    	});
    }
    
    
    public static boolean isMinimizeMemoryUsage() {
    	return minimizeMemoryUsage;
    }
    
    
    
    public static BlockingQueue<Job> getJobsBlockingQueue() {
		return JobsBlockingQueue;
	}
    
    public static BlockingQueue<Job> getInputBlockingQueue() {
		return InputBlockingQueue;
	}
    
    
    public static Map<String, Integer> getResultMap() {
		return resultMap;
	}
    
    





}
