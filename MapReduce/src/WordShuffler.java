import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordShuffler extends Shuffler {
	
	private List<String> wordsToShuffle;
	private List<List<String>> shuffledWordsList; // We use lists since it's only words
	
	WordShuffler(List<String> wordsList){
		wordsToShuffle = wordsList;
	}

	
	@Override
	public void run() {
		shuffle();
	}
	
    public void shuffle(){
    	shuffledWordsList = new ArrayList<>();
    	Map<String, Integer> indexMap = new HashMap<>();   	
    	
		for(String word : wordsToShuffle) {
			if (indexMap.get(word) != null){
				shuffledWordsList.get(indexMap.get(word)).add(word);
			}else {
				List<String> auxWordList = new ArrayList<>();
				auxWordList.add(word);
				shuffledWordsList.add(auxWordList);
				indexMap.put(word, shuffledWordsList.size()-1);
			}
		}
		
		addJob();
				
    }
    
    private void addJob(){
    	//System.out.println("adding to reduce " + shuffledWordsList);    	
    	Job reduceJob = new WordReducer(shuffledWordsList);
		addJobToQueue(reduceJob);
    }
    
    
}
