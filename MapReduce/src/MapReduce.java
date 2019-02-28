import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class MapReduce {

    public List<String> getLiniesFromFile(String path){

        String linia;
        List<String> lineslist = new ArrayList<>();

        try {
            FileReader fileReader = new FileReader(path);
            BufferedReader br = new BufferedReader(fileReader);
            while(( linia = br.readLine()) != null){
                if(!linia.isEmpty()){

                    lineslist.add(linia);
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
