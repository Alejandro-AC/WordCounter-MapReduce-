import java.util.Map;

public class WordMerger extends Merger{
	
	private Map<String, Integer> mapToMerge;	
	
	WordMerger(Map<String, Integer> reducedMap){
		mapToMerge = reducedMap;
	}
	
	@Override
	public void run() {
		merge(MapReduce.getResultMap());
	}
	
    public void merge(Map<String, Integer> map){    	
    	
    	//System.out.println("Merging " + mapToMerge);  
    	
    	mapToMerge.forEach(
		    (key, value) -> map.merge(key, value, (x, y) -> x + y)
		);    	
    	
    }
    
    
    
}
