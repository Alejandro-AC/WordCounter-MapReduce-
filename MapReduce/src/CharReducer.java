import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CharReducer extends Reducer{
	
	private Map<Character, Integer> reducedMap;
	private List<List<Character>> shuffledCharList; // We use lists since it's only words
	
	CharReducer(List<List<Character>> listOfCharsList){
		shuffledCharList = listOfCharsList;
	}
	
	@Override
	public void run() {
		reduce();
	}
	
    public void reduce(){
    	reducedMap = new HashMap<>();
    	
    	for (List<Character> charList : shuffledCharList) {	    	
	    	char ch = charList.get(0);
	    	int numChars = charList.size();
	    	reducedMap.put(ch, numChars);
    	}
    	
    	addJob();
    }
    
    private void addJob(){ 
    	Job mergeJob = new CharMerger(reducedMap);
		addJobToQueue(mergeJob);
    }
}
