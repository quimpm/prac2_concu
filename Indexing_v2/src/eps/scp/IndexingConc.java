package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.File;

public class IndexingConc {

    /* Argument methods*/
    private static int num_threads;

    private static InvertedIndexConc inv_index = new InvertedIndexConc();

    public IndexingConc(String[] args){
        main(args);
    }

    public InvertedIndexConc get_InvertedIndex(){
        return inv_index;
    }


    public static void main(String[] args)
    {
        /* Inicialización de variables */
        int[] threadCharge;
        InvertedIndexConc[] inverted_hashes;
        Thread[] threads_storage;
        int start=0;
        int end=0;

        //System.out.println("Num args: " + args.length);
        //for(String str:args) System.out.println("Arg: " + str);


        /* Control argumentos */
        String text_file;
        if (args.length <2 || args.length >4) {
            System.err.println("Error in Parameters. Usage: Indexing <TextFile> <Thread_number>[<Key_Size>] [<Index_Directory>]");
            throw new IllegalArgumentException();
            System.exit(0);
        }
        text_file = args[1];
        num_threads = Integer.parseInt( args[0] );
        inverted_hashes = new InvertedIndexConc[num_threads];
        threads_storage = new Thread[num_threads];
        if (args.length == 2){ /* Text file and thread number */
            //System.out.println("Num threads: " + num_threads );
            for (int i = 0; i < num_threads; i++) inverted_hashes[i] = new InvertedIndexConc(text_file);
        }else{ /* Text file, thread number and key size (and possibly index directory) */
            int key_size = Integer.parseInt(args[2]);
            for(int i = 0; i < num_threads; i++) inverted_hashes[i] = new InvertedIndexConc(text_file, key_size);
        }


        /* Balanceo de carga y creación de hilos */
        //threadCharge=balanceoCarga(args[0]); //TODO: Descomentat funciona
        threadCharge = balanceoCarga_v2(text_file);

        for(int i = 0; i < num_threads; i++){
            end+=threadCharge[i]-1;
            //System.out.println("Thread " + i + "\n" + "Start " + start + "\n" + "End " + end );
            threads_storage[i] =  new Thread(new partsBuildIndex(start,end,inverted_hashes[i]));
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

        /* Juntar hashes parciales */ //TODO: Descomentar
        HashMultimap<String, Long> mult_hash = inverted_hashes[0].getHash();
        for(int i = 1; i < num_threads; i++) mult_hash.putAll(inverted_hashes[i].getHash());
        inverted_hashes[0].setHash(mult_hash);

        /* Guardar resultado */

        /* UNDER CONSTRUCTION */
        /*if (args.length > 3) {
            for(int i = 1; i < num_threads; i++){
                inverted_hashes[i].SaveIndex(args[3]);
            }
        }
        else
            for(int i = 1; i < num_threads; i++){
                inverted_hashes[1].PrintIndex();
            }*/


        /* UNDER CONSTRUCTION */


        if (args.length > 3) { //TODO: Descomentar
            inverted_hashes[0].SaveIndex(args[3]);
        }
        else
            inverted_hashes[0].PrintIndex();

        /* Actualizar método para el testing */
        inv_index.setHash(inverted_hashes[0].getHash());
    }

    private static int[] balanceoCarga_v2(String file_name){

        File file = new File(file_name);
        int[] threadCharge = new int[num_threads];
        float real_end = file.length() - 10 + 1; //TODO: KeySize hardcodejat a 10, s'ha de generalitzar
        //System.out.println(file.length());
        for(int i = 0;i < num_threads;i++){
            threadCharge[i]= (int) Math.floor((float)real_end/num_threads);
        }

        for(int i = 0; i<(int)real_end%num_threads; i++){
            threadCharge[i]++;
        }
        return threadCharge;
        //Bucle per comprovar que el balanceo és correcte //TODO:Treure
        /*for(int i = 0;i < num_threads;i++){
            System.out.print(threadCharge[i]+"\n");A----------------
        }*/
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
            System.out.print(threadCharge[i]+"\n");A----------------
        }*/
    }

    public static class partsBuildIndex implements Runnable{

        public int start;
        public int end;
        InvertedIndexConc hash;

        public partsBuildIndex(int start, int end, InvertedIndexConc hash){
            this.start=start;
            this.end=end;
            this.hash=hash;
        }

        public void run(){
            /*Print per comprovar que funcionen els fils i que els parametres start i stop són correctes //TODO:Treure*/
            //System.out.print("Thread: "+Thread.currentThread().getId()+"; Start: "+this.start+"; End: "+this.end+"\n");
            this.hash.BuildIndex(start, end);
        }
    }
}


