import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
 
public class InputReader {
	
	private String[] filenameArr;
	
	InputReader(String[] filenames) {
		filenameArr = filenames;
	}
	
	InputReader() {
	}
	
	public void readAllFiles() {
		for (String filename : filenameArr) {
			readInputFile(filename);
		}
	}
 
    public boolean readInputFile(String filename) {
    	
    	boolean finished = false;
 
        Path filePath = Paths.get(filename);
        try
        {
            //Java 8: Stream class
            Stream<String> lines = Files.lines( filePath, StandardCharsets.UTF_8 );
            
            for( String line : (Iterable<String>) lines::iterator )
            {
            	MapReduce.getBlockingQueueMap().put(line); //blocked if reaches its capacity, until consumer consumes
            }
        
            lines.close();
            
        } catch (Exception e){
            e.printStackTrace();
        }
        
        finished = true; // signal consumer
        //System.out.println(Thread.currentThread().getName() + " producer is done");
        
        
        
        return finished;
 
    }
}
