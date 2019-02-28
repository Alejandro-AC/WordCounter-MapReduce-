import java.io.File;

public class Main {

    public static void main(String[] args) {

        if(args.length == 0){
            System.out.println("No s'ha especificat cap arxiu");
        }else{
            for(int i=0;i<args.length;i++){

                File arxiu = new File(args[i]);

                if(arxiu.exists()){

                    //TODO tota la lògica del MapReduce

                }else{
                    System.out.println("L'arxiu" + args[i] + " no existeix.");
                }
            }
        }
        

    }
}
