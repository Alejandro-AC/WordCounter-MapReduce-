import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class CharSplitter extends Splitter {

	private int nLines = 10;
	private String blockOfLines = "";
 
    public void split(String filename) {
 
        Path filePath = Paths.get(filename);
        try
        {
            //Java 8: Stream class
            Stream<String> lines = Files.lines( filePath, StandardCharsets.UTF_8 );
            
            int linesRead = 0; 
            for( String line : (Iterable<String>) lines::iterator )
            {
            	if (linesRead == 0) {
            		blockOfLines += line;
            	}else {
            		blockOfLines += " " + line;
            	}
            	linesRead++;
            	if (linesRead == nLines) {
            		addJob();
            		linesRead = 0;
            	}            	
            }
        
            if (linesRead > 0) {
            	addJob();    		
            }            
            
            lines.close();
            
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    
    void addJob() {
		Job mapJob = new CharMapper(blockOfLines);
		if (MapReduce.isMinimizeMemoryUsage()) {
			addJobToInputQueue(mapJob);	
		}else {
			addJobToQueue(mapJob);	
		}
		
		blockOfLines = "";
    }    
 
}
