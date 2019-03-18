import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordReducer extends Reducer{
	
	private Map<String, Integer> reducedMap;
	private List<List<String>> shuffledWordsList; // We use lists since it's only words
	
	WordReducer(List<List<String>> listOfWordsList){
		shuffledWordsList = listOfWordsList;
	}
	
	@Override
	public void run() {
		reduce();
	}
	
    public void reduce(){
    	reducedMap = new HashMap<>();
    	
    	for (List<String> wordList : shuffledWordsList) {	    	
	    	String word = wordList.get(0);
	    	int numWords = wordList.size();
	    	reducedMap.put(word, numWords);
    	}
    	
    	addJob();
    }
    
    private void addJob(){
    	//System.out.println("adding to merge " + reducedMap);  
    	Job mergeJob = new WordMerger(reducedMap);
		addJobToQueue(mergeJob);
    }
}
