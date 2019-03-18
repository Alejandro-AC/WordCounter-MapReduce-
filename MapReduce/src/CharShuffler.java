import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CharShuffler extends Shuffler {
	
	private List<Character> charsToShuffle;
	private List<List<Character>> shuffledCharsList; // We use lists since it's only chars
	
	CharShuffler(List<Character> charsList){
		charsToShuffle = charsList;
	}
	
	@Override
	public void run() {
		shuffle();
	}
	
    public void shuffle(){
    	shuffledCharsList = new ArrayList<>();
    	Map<Character, Integer> indexMap = new HashMap<>();   	
    	
		for(Character ch : charsToShuffle) {
			if (indexMap.get(ch) != null){
				shuffledCharsList.get(indexMap.get(ch)).add(ch);
			}else {
				List<Character> auxCharList = new ArrayList<>();
				auxCharList.add(ch);
				shuffledCharsList.add(auxCharList);
				indexMap.put(ch, shuffledCharsList.size()-1);
			}
		}
		
		addJob();				
    }
    
    private void addJob(){  	
    	Job reduceJob = new CharReducer(shuffledCharsList);
		addJobToQueue(reduceJob);
    }    
    
}
