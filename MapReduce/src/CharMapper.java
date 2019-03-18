import java.util.ArrayList;
import java.util.List;

public class CharMapper extends Mapper{
	
	private String blockOfLinesToMap;
	private List<Character> mappedLines; // We use a list since it's only characters
	
	CharMapper(String linesList){
		blockOfLinesToMap = linesList;
	}
	
	@Override
	public void run() {
		map();
	}
		
    public void map() {
    	mappedLines = new ArrayList<>();     	
    	
		for (Character ch : blockOfLinesToMap.toCharArray()) {
			mappedLines.add(ch);
		}    	
		
    	addJob();
    }
    
    private void addJob() {
    	//System.out.println("adding to shuffle " + mappedLines);
		Job shuffleJob = new CharShuffler(mappedLines);
		addJobToQueue(shuffleJob);
    }
}
