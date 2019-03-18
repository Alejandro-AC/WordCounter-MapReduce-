import java.util.Map;

public class CharMerger extends Merger{
	
	private Map<Character, Integer> mapToMerge;	
	
	CharMerger(Map<Character, Integer> reducedMap){
		mapToMerge = reducedMap;
	}
	
	@Override
	public void run() {
		merge(MapReduce.getCharResultMap());
	}
	
    public void merge(Map<Character, Integer> map){
    	
    	mapToMerge.forEach(
		    (key, value) -> map.merge(key, value, (x, y) -> x + y)
		);    	 
    }    
    
}
