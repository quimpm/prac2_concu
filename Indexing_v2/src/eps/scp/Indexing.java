package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.File;

public class Indexing {

    public static int num_threads=4;


    public static void main(String[] args)
    {
        InvertedIndex final_hash;
        int[] threadCharge = new int[num_threads];
        InvertedIndex[] hashes = new InvertedIndex[num_threads];
        Thread[] threads_storage = new Thread[num_threads];
        int start=0;
        int end=0;

        if (args.length <2 || args.length>4)
            System.err.println("Erro in Parameters. Usage: Indexing <TextFile> [<Key_Size>] [<Index_Directory>]");
        if (args.length < 2) {
            //hash = new InvertedIndex(args[0]);
            for (int i = 0; i < num_threads; i++) hashes[i] = new InvertedIndex(args[0]);
        }else{
            //hash = new InvertedIndex(args[0], Integer.parseInt(args[1]));
            for(int i = 0; i < num_threads; i++) hashes[i] = new InvertedIndex(args[0], Integer.parseInt(args[1]));
        }


        threadCharge=balanceoCarga(args[0]);

        // Creación de hilos
        for(int i = 0; i < num_threads; i++){
            end+=threadCharge[i]-1;
            //System.out.println("Thread " + i + "\n" + "Start " + start + "\n" + "End " + end );
            threads_storage[i] =  new Thread(new partsBuildIndex(start,end,hashes[i],args));
            threads_storage[i].start();
            start+=threadCharge[i];
            end++;
        }

        // Join de hilos
        try{
            for(int i = 0; i < num_threads; i++){
                threads_storage[i].join();
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }

        // Juntar hashes parciales
        HashMultimap<String, Long> mult_hash = hashes[0].getHash();

        for(int i = 1; i < num_threads; i++){
            mult_hash.putAll(hashes[i].getHash());
        }
        hashes[0].setHash(mult_hash);

        /*HashMultimap<String, Long>[] hashMultimaps;
        HashMultimap<String, Long> acumHash = hashes[0].getHash();
        for(int i = 1; i < num_threads ; i++){
            acumHash.putAll(hashes[i].getHash());
        }

        final_hash.setHash(acumHash);*/


        if (args.length > 2) {
            hashes[0].SaveIndex(args[2]); //TODO: Debug
        }
        else
            hashes[0].PrintIndex();



    }

    public static int[] balanceoCarga(String file_name){

        File file = new File(file_name);
        int[] threadCharge = new int[num_threads];
        //System.out.println(file.length());
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

        }
    }
}


