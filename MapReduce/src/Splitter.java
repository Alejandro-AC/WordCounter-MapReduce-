
public abstract class Splitter extends Job{
	
	protected String[] filenameArr;
	
	Splitter(String[] filenames) {
		filenameArr = filenames;
	}
	
	Splitter() {
	}
	
	public void readAllFiles() { }
}
