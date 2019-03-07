import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



class MapReduce implements Runnable {

private static String[] filenameArr;

private static Map<String, Integer> resultMap = new HashMap<>();


private static final int CONSUMER_COUNT = Runtime.getRuntime().availableProcessors();
private static final int BLOCKING_QUEUE_CAPACITY = 50;

private final static BlockingQueue<String> blockingQueueMap = new ArrayBlockingQueue<String>(BLOCKING_QUEUE_CAPACITY);
private final static BlockingQueue<List<List<String>>> blockingQueueShuffle = new ArrayBlockingQueue<List<List<String>>>(BLOCKING_QUEUE_CAPACITY);
private final static BlockingQueue<List<String>> blockingQueueReduce = new ArrayBlockingQueue<List<String>>(BLOCKING_QUEUE_CAPACITY);
private final static BlockingQueue<List<Map<String, Integer>>> blockingQueueMerge = new ArrayBlockingQueue<List<Map<String, Integer>>>(BLOCKING_QUEUE_CAPACITY);
private final static List<List<String>> waitingQueue = new ArrayList<List<String>>();

private static List<Map<String, Integer>> mergeMapList = new ArrayList<>();
private static List<List<String>> reducedMapList = new ArrayList<>();
private static List<Map<String, Integer>> shuffleMapList = new ArrayList<>();
private static List<Map<String, Integer>> mapMapList = new ArrayList<>();


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
			System.out.println("Soy un thread");
            consume();
        } else {
        	
        	
        	for (String filename : filenameArr) {
        		InputReader ir = new InputReader();
        		producerIsDone = ir.readInputFile(filename);
        		System.out.println("Printo el final");
        		printResultMap(filename);
        		resultMap.clear();
        	}
            
        }
	}
	
	private void consume() {
        try {

            while (!producerIsDone || (producerIsDone && !blockingQueueMap.isEmpty() && !blockingQueueShuffle.isEmpty()
            							&& !blockingQueueReduce.isEmpty() && !blockingQueueMerge.isEmpty())) {


				if (blockingQueueMerge.size() > 5) {

            		merge(blockingQueueMerge.take());

            		
            	} else if (blockingQueueReduce.size() > 5 ) {

            		Map<String, Integer> reducedMap = reduce(blockingQueueReduce.take());
            		mergeMapList.add(reducedMap);
            		blockingQueueMerge.put(mergeMapList);

            	} else if (blockingQueueShuffle.size() > 5 ) {

            		List<List<String>> shuffledMap = shuffle(blockingQueueShuffle.take());

            		reducedMapList = shuffledMap;

            		for (int i=0; i< reducedMapList.size();i++){
            		blockingQueueReduce.put(reducedMapList.get(i));
            		}
            		
            	} else if (!blockingQueueMap.isEmpty()) {
					System.out.println(blockingQueueMap.take());
            		List<String> mappedList = map(blockingQueueMap.take());

					waitingQueue.add(mappedList);
					System.out.println(waitingQueue);//errores de concurrencia al leer.
					if(waitingQueue.size() > 5 || blockingQueueMap.isEmpty()){

						blockingQueueShuffle.put(waitingQueue);
						waitingQueue.clear();
					}


            	}else {
					System.out.println("No entro en ninguna condicion");
				}
            	
                /*String lineToProcess = blockingQueue.take();
                List<String> wordList = wordMap(lineToProcess);
                Map<String, Integer> wordMap = wordReduceSimple(wordList);
                wordReduce(wordMap);*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        //System.out.println(Thread.currentThread().getName() + " consumer is done");
        
    }
	
    
    public List<String> map(String line) {
    	List<String> wordMap = new ArrayList<>();
    	
    	for (String word : line.replaceAll("[.,]", "").toLowerCase().split(" ") ) {
    		wordMap.add(word);
    	}
    	
    	return wordMap;
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
    
    public List<List<String>> shuffle(List<List<String>> mappedList){
    	List<List<String>> shuffledList = new ArrayList<>();
    	Map<String, Integer> indexMap = new HashMap<>();
    	
    	for (List<String> wordList : mappedList) {
    		for(String word : wordList) {
    			if (indexMap.get(word) != null){
    				shuffledList.get(indexMap.get(word)).add(word);
    			}else {
    				List<String> auxWordList = new ArrayList<>();
    				auxWordList.add(word);
    				shuffledList.add(auxWordList);
    				indexMap.put(word, shuffledList.size()-1);
    			}
    		}
    	}
    	
    	return shuffledList;    	
    }
    
    public Map<String, Integer> reduce(List<String> shuffledWordsList){
    	Map<String, Integer> reducedMap = new HashMap<>();
    	String word = shuffledWordsList.get(0);
    	int numWords = shuffledWordsList.size();
    	
    	
    	reducedMap.put(word, numWords);
    	
    	return reducedMap;
    }
    
    public void merge(List<Map<String, Integer>> wordMapList){
    	for (Map<String,Integer> reducedMap : wordMapList) {
    		reducedMap.forEach(
    		    (key, value) -> resultMap.merge(key, value, (x, y) -> x + y)
    		);
    	}
    }
    

    
    private void printResultMap(String filename) {
    	System.out.println("\n\n" + filename + ":");
    	
    	resultMap.forEach((key, value) -> {
    	    System.out.println(key + " : " + value);
    	});
    }
    
    
    
    public static BlockingQueue<String> getBlockingQueueMap() {
		return blockingQueueMap;
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
