import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

class MapReduce implements Runnable {

private static String[] filenameArr;

private static Map<String, Integer> resultMap = new HashMap<>();


private static final int CONSUMER_COUNT = Runtime.getRuntime().availableProcessors();
private static final int BLOCKING_QUEUE_CAPACITY = 50;
private final static BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<String>(BLOCKING_QUEUE_CAPACITY);

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
        
        // create a pool of consumer threads to parse the lines read
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
            consume();
        } else {
        	
        	
        	for (String filename : filenameArr) {
        		InputReader ir = new InputReader();
        		producerIsDone = ir.readInputFile(filename);
        		printResultMap(filename);
        		resultMap.clear();
        	}
            
        }
	}
	
	private void consume() {
        try {
            while (!producerIsDone || (producerIsDone && !blockingQueue.isEmpty())) {
                String lineToProcess = blockingQueue.take();
                List<String> wordList = wordMap(lineToProcess);
                Map<String, Integer> wordMap = wordReduceSimple(wordList);
                wordReduce(wordMap);                
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        //System.out.println(Thread.currentThread().getName() + " consumer is done");
        
    }
	
    
    public List<String> wordMap(String line) {
    	List<String> wordMap = new ArrayList<>();
    	
    	for (String word : line.toLowerCase().split(" ") ) {
    		wordMap.add(word);
    	}
    	
    	return wordMap;
    }
    
    public Map<String, Integer> wordReduceSimple(List<String> wordList){
    	Map<String, Integer> wordMap = new HashMap<>();
    	for (String word : wordList) {
    		// Java 8 elegant way
			wordMap.merge(word, 1, (x, y) -> x + y);
    	}
    	
    	return wordMap;
    }
    
    public Map<String, Integer> wordReduce(List<List<String>> wordLists){
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
    }
    
    public void wordReduce(Map<String, Integer> wordMap){
    	
    	wordMap.forEach(
    		    (key, value) -> resultMap.merge(key, value, (x, y) -> x + y)
    		);
    }
    

    
    private void printResultMap(String filename) {
    	System.out.println("\n\n" + filename + ":");
    	
    	resultMap.forEach((key, value) -> {
    	    System.out.println(key + " : " + value);
    	});
    }
    
    
    
    public static BlockingQueue<String> getBlockingQueue() {
		return blockingQueue;
	}
    

	public List<Character> characterMap(String line) {
    	List<Character> characterMap = new ArrayList<>();
    	
    	for (Character c :  line.toCharArray() ) {
    		characterMap.add(c);
    	}
    	
    	return characterMap;
    }
    
    public Map<Character, Integer> characterReduce(List<List<Character>> characterLists){
    	Map<Character, Integer> characterMap = new HashMap<>();
    	
    	for (List<Character> characterList : characterLists) {
    		for(Character character : characterList) {
    			// Java 8 compact way
    			characterMap.put(character, characterMap.getOrDefault(character, 0) + 1);
    		}
    	}
    	
    	return characterMap;
    }





}
