package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.File;

public class Indexing {

    private static int num_threads=4; //TODO: Generalitzar

    // UNDER CONSTRUCTIOON vvvvvvvv
    private static InvertedIndex inv_index = new InvertedIndex();

    public Indexing(String[] args){
        main(args);
    }

    public InvertedIndex get_InvertedIndex(){
        return inv_index;
    }

    // UNDER CONSTRUCTIOON ^^^^^^
    public static void main(String[] args)
    {
        /* Inicialización de variables */
        int[] threadCharge;
        InvertedIndex[] inverted_hashes = new InvertedIndex[num_threads];
        Thread[] threads_storage = new Thread[num_threads];
        int start=0;
        int end=0;

        /* Control argumentos */
        if (args.length <2 || args.length>4)
            System.err.println("Erro in Parameters. Usage: Indexing <TextFile> [<Key_Size>] [<Index_Directory>]");
        if (args.length < 2) {
            //hash = new InvertedIndex(args[0]);
            for (int i = 0; i < num_threads; i++) inverted_hashes[i] = new InvertedIndex(args[0]);
        }else{
            //hash = new InvertedIndex(args[0], Integer.parseInt(args[1]));
            for(int i = 0; i < num_threads; i++) inverted_hashes[i] = new InvertedIndex(args[0], Integer.parseInt(args[1]));
        }



        /* Balanceo de carga y creación de hilos */
        threadCharge=balanceoCarga(args[0]);
        for(int i = 0; i < num_threads; i++){
            end+=threadCharge[i]-1;
            //System.out.println("Thread " + i + "\n" + "Start " + start + "\n" + "End " + end );
            threads_storage[i] =  new Thread(new partsBuildIndex(start,end,inverted_hashes[i],args));
            threads_storage[i].start();
            start+=threadCharge[i];
            end++;
        }

        /* Join de hilos */
        try{
            for(int i = 0; i < num_threads; i++){
                threads_storage[i].join();
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }

        /* Juntar hashes parciales */
        HashMultimap<String, Long> mult_hash = inverted_hashes[0].getHash();
        for(int i = 1; i < num_threads; i++) mult_hash.putAll(inverted_hashes[i].getHash());
        inverted_hashes[0].setHash(mult_hash);

        /* Guardar resultado */

        if (args.length > 2) {
            inverted_hashes[0].SaveIndex(args[2]);
        }
        else
            inverted_hashes[0].PrintIndex();

        /* Actualizar método para el testing */
        inv_index.setHash(inverted_hashes[0].getHash());

    }

    private static int[] balanceoCarga(String file_name){

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
        //Bucle per comprovar que el balanceo és correcte //TODO:Treure
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
            /*Print per comprovar que funcionen els fils i que els parametres start i stop són correctes //TODO:Treure
            System.out.print("Thread: "+Thread.currentThread().getId()+"; Start: "+this.start+"; End: "+this.end+"\n");*/
            this.hash.BuildIndex(start, end);

        }
    }
}


