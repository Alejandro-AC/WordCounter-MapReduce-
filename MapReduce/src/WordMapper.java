import java.util.ArrayList;
import java.util.List;

public class WordMapper extends Mapper{
	
	private String blockOfLinesToMap;
	private List<String> mappedLines; // We use a list since it's only words
	
	WordMapper(String linesList){
		blockOfLinesToMap = linesList;
	}
	
	@Override
	public void run() {
		map();
	}
		
    public void map() {
    	mappedLines = new ArrayList<>();      	
    	
		for (String word : blockOfLinesToMap.split("\\s")) {
			mappedLines.add(word);
		}    	
		
    	addJob();
    }
    
    private void addJob() {
		Job shuffleJob = new WordShuffler(mappedLines);
		addJobToQueue(shuffleJob);
    }
}
