import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class MapReduce {

    public List<String> getLinesFromFile(String path){

        String line;
        List<String> lineslist = new ArrayList<>();

        try {
            FileReader fileReader = new FileReader(path);
            BufferedReader br = new BufferedReader(fileReader);
            while(( line = br.readLine()) != null){
                if(!line.isEmpty()){

                    lineslist.add(line);
                }
            }
        }catch (FileNotFoundException e){

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }


        return lineslist;
    }
}
