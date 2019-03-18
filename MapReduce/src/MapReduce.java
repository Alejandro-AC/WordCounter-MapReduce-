import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



class MapReduce implements Runnable {

private static String[] filenameArr;

private static ConcurrentMap<String, Integer> resultMap = new ConcurrentHashMap<>();


private static final int CONSUMER_COUNT = Runtime.getRuntime().availableProcessors() + 1;
private static final int INPUT_BLOCKING_QUEUE_CAPACITY = 50;
private static final int JOB_BLOCKING_QUEUE_CAPACITY = 15000;

private final static BlockingQueue<Job> InputBlockingQueue = new ArrayBlockingQueue<Job>(INPUT_BLOCKING_QUEUE_CAPACITY);
private final static BlockingQueue<Job> JobsBlockingQueue = new ArrayBlockingQueue<Job>(JOB_BLOCKING_QUEUE_CAPACITY);


private boolean isConsumer = false;
private static boolean producerIsDone = false;

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
        ExecutorService consumerPool = Executors.newFixedThreadPool(CONSUMER_COUNT);
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
        	System.out.println("Consume");
            consume();
            
        } else {        	
        	
        	for (String filename : filenameArr) {
        		
        		WordSplitter ws = new WordSplitter();
        		ws.split(filename);      		

            	while (!JobsBlockingQueue.isEmpty()) {            		
            	}
        		
        		printResultMap(filename);
        		resultMap.clear();
        	}
        	
        	while (!JobsBlockingQueue.isEmpty()) {            		
        	}
        	
        	producerIsDone = true;
            
        }
	}
	
	private void consume() {
        try {
        	while (!producerIsDone || (producerIsDone && !JobsBlockingQueue.isEmpty())) {
		    	Job job;   
		    	/*
		    	if (JobsBlockingQueue.isEmpty()) {		    		
		    		job = InputBlockingQueue.take();
		    	}else {
		    		job = JobsBlockingQueue.take();
		    	}*/
		    	//System.out.println(JobsBlockingQueue);
		    	
		    	job = JobsBlockingQueue.take();
				job.run();
        	}

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        System.out.println(Thread.currentThread().getName() + " consumer is done");
        
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
    	System.out.println("\n\n" + filename + ":");
    	
    	resultMap.forEach((key, value) -> {
    	    System.out.println(key + " : " + value);
    	});
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
