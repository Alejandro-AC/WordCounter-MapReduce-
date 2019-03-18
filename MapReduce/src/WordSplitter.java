import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
 
public class WordSplitter extends Splitter {

	private int nLines = 5;
	private String blockOfLines = "";
	
	public void readAllFiles() {
		for (String filename : filenameArr) {
			split(filename);
		}
	}
 
    public boolean split(String filename) {
    	
    	boolean finished = false;
 
        Path filePath = Paths.get(filename);
        try
        {
            //Java 8: Stream class
            Stream<String> lines = Files.lines( filePath, StandardCharsets.UTF_8 );
            
            int linesRead = 0; 
            for( String line : (Iterable<String>) lines::iterator )
            {
            	if (blockOfLines.equals("")) {
            		blockOfLines += line.replaceAll("[.,]", "").toLowerCase();
            	}else {
            		blockOfLines += " " + line.replaceAll("[.,;:]", "").toLowerCase();
            	}
            	linesRead++;
            	if (nLines == linesRead) {
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
        
        finished = true; // signal consumer
        //System.out.println(Thread.currentThread().getName() + " producer is done");        
        
        
        return finished;
 
    }
    
    void addJob() {
		Job mapJob = new WordMapper(blockOfLines);
		//System.out.println("add  " + blockOfLines);
		addJobToQueue(mapJob);		
		blockOfLines = "";

    }
    
 
}
