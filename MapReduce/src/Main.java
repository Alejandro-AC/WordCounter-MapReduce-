import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static List<String> llistatlinies = new ArrayList<>();
    public static void main(String[] args) {

        if(args.length == 0){
            System.out.println("No s'ha especificat cap arxiu");
        }else{
            for(int i=0;i<args.length;i++){

                File arxiu = new File(args[i]);

                if(arxiu.exists()){

                    //TODO tota la lògica del MapReduce
                    MapReduce mapReduce = new MapReduce();
                    llistatlinies = mapReduce.getLiniesFromFile(arxiu.toString());
                    System.out.println(llistatlinies);

                }else{
                    System.out.println("L'arxiu" + args[i] + " no existeix.");
                }
            }
        }


    }
}
