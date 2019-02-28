import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static List<String> lineslist = new ArrayList<>();
    public static void main(String[] args) {

        if(args.length == 0){
            System.out.println("File not especified");
        }else{
            for(int i=0;i<args.length;i++){

                File file = new File(args[i]);

                if(file.exists()){

                    //TODO all logic of MapReduce
                    MapReduce mapReduce = new MapReduce();
                    lineslist = mapReduce.getLinesFromFile(file.toString());
                    System.out.println(lineslist);

                }else{
                    System.out.println("File " + args[i] + " does not exist.");
                }
            }
        }


    }
}
