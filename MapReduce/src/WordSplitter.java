import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class WordSplitter extends Splitter {

	private int nLines = 10;
	private String blockOfLines = "";
	
	public void readAllFiles() {
		for (String filename : filenameArr) {
			split(filename);
		}
	}
 
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
    	//blockOfLines = blockOfLines.replaceAll("[^a-zA-Z0-9ΰαθιμνοςσωϊόηρ\\s'·]", "").toLowerCase();
    	blockOfLines = replace(blockOfLines);
		Job mapJob = new WordMapper(blockOfLines);
		if (MapReduce.isMinimizeMemoryUsage()) {
			addJobToInputQueue(mapJob);	
		}else {
			addJobToQueue(mapJob);	
		}
			
		blockOfLines = "";
    }
    
    private String replace(String s) {
    	String regex = "[.:;,]";
    	Pattern pattern = Pattern.compile(regex);
    	Matcher matcher = pattern.matcher(s);
    	String result = matcher.replaceAll("");
    	
    	return result;
    }
    
 
}
