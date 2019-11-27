package eps.scp;

import java.io.File;

public class Indexing {

    public static int num_threads=3;


    public static void main(String[] args)
    {
        InvertedIndex hash;
        int[] threadCharge = new int[num_threads];
        int start=0;
        int end=0;

        if (args.length <2 || args.length>4)
            System.err.println("Erro in Parameters. Usage: Indexing <TextFile> [<Key_Size>] [<Index_Directory>]");
        if (args.length < 2)
            hash = new InvertedIndex(args[0]);
        else
            hash = new InvertedIndex(args[0], Integer.parseInt(args[1]));

        threadCharge=balanceoCarga(args[0]);

        for(int i = 0; i < num_threads; i++){
            end+=threadCharge[i]-1;
            System.out.println("Thread " + i + "\n" + "Start " + start + "\n" + "End " + end );
            new Thread(new partsBuildIndex(start,end,hash,args)).start();
            start+=threadCharge[i];
            end++;
        }
    }

    public static int[] balanceoCarga(String file_name){

        File file = new File(file_name);
        int[] threadCharge = new int[num_threads];
        System.out.println(file.length());
        for(int i = 0;i < num_threads;i++){
            threadCharge[i]= (int) Math.floor((float)file.length()/num_threads);
        }

        for(int i = 0; i<(int)file.length()%num_threads; i++){
            threadCharge[i]++;
        }

        return threadCharge;

        //Bucle per comprovar que el balanceo és correcte
        /*for(int i = 0;i < num_threads;i++){
            System.out.print(threadCharge[i]+"\n");
        }*/
    }

    public static class partsBuildIndex implements Runnable{

        public int start;
        public int end;
        InvertedIndex hash;
        String[] args;

        public partsBuildIndex(int start, int end, InvertedIndex hash, String[] args){
            this.start=start;
            this.end=end;
            this.hash=hash;
            this.args=args;
        }

        public void run(){
            /*Print per comprovar que funcionen els fils i que els parametres start i stop són correctes
            System.out.print("Thread: "+Thread.currentThread().getId()+"; Start: "+this.start+"; End: "+this.end+"\n");*/
            this.hash.BuildIndex(start, end);
            if (this.args.length > 2)
                this.hash.SaveIndex(args[2]);
            else
                this.hash.PrintIndex();
        }
    }
}


