package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexingConc {

    /* Argument methods*/
    private static int numThreads;

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
        AtomicBoolean debug = new AtomicBoolean(true);//TODO desatomitzar
        if(debug.get()) System.err.println("Inicialització");
        int[] threadCharge;
        InvertedIndexConc[] inverted_hashes;
        Thread[] threads_storage;
        int start=0;
        int end=0;
        if(debug.get()) System.err.println("Fi inicialització");

        //System.out.println("Num args: " + args.length);
        //for(String str:args) System.out.println("Arg: " + str);

        /* Control argumentos */
        if(debug.get()) System.err.println("Control arguments");
        if (args.length <2 || args.length >4) {
            System.err.println("Error in Parameters. Usage: Indexing <TextFile> <Thread_number>[<Key_Size>] [<Index_Directory>]");
            throw new IllegalArgumentException();
        }
        String text_file = args[1];
        numThreads = Integer.parseInt( args[0] );
        inverted_hashes = new InvertedIndexConc[numThreads];
        threads_storage = new Thread[numThreads];
        if (args.length == 2){ /* Text file and thread number */
            //System.out.println("Num threads: " + num_threads );
            for (int i = 0; i < numThreads; i++) inverted_hashes[i] = new InvertedIndexConc(text_file);
        }else{ /* Text file, thread number and key size (and possibly index directory) */
            int key_size = Integer.parseInt(args[2]);
            for(int i = 0; i < numThreads; i++) inverted_hashes[i] = new InvertedIndexConc(text_file, key_size);
        }
        if(debug.get()) System.err.println("Fi control arguments");



        /* Balanceo de carga y creación de hilos */
        if(debug.get()) System.err.println("Balanceig carrega");
        //threadCharge=balanceoCarga(args[0]); //TODO: Descomentat funciona
        threadCharge = balanceoCarga_v2(text_file);
        if(debug.get()) System.err.println("Fi balanceig carrega");

        if(debug.get()) System.err.println("Creació threads");
        /* Creación threads */
        for(int i = 0; i < numThreads; i++){
            end+=threadCharge[i]-1;
            //System.out.println("Thread " + i + "\n" + "Start " + start + "\n" + "End " + end );
            threads_storage[i] =  new Thread(new partsBuildIndex(start,end,inverted_hashes[i]));
            threads_storage[i].start();
            start+=threadCharge[i];
            end++;
        }
        if(debug.get()) System.err.println("Fi creació threads");



        /* Join de hilos */
        if(debug.get()) System.err.println("Juntar fils");
        try{
            for(int i = 0; i < numThreads; i++){
                threads_storage[i].join();
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        if(debug.get()) System.err.println("Fi juntar fils");



        /* Juntar hashes parciales */ //TODO: Descomentar
        if(debug.get()) System.err.println("Juntar hashos parcials");
        HashMultimap<String, Long> mult_hash = inverted_hashes[0].getHash();
        for(int i = 1; i < numThreads; i++){
            if(debug.get()) System.err.println("Fusionant hash " + i);
            mult_hash.putAll(inverted_hashes[i].getHash());
            if(debug.get()) System.err.println("Fi fusionant hash " + i);
        }
        inverted_hashes[0].setHash(mult_hash);
        if(debug.get()) System.err.println("Fi juntar hashos parcials");



        /* Guardar resultado */
        if(debug.get()) System.err.println("Guardar resultats");
        /* UNDER CONSTRUCTION */
        int numberOfFiles, remainingFiles;
        long remainingKeys=0, keysByFile=0;
        String key="";
        Charset utf8 = StandardCharsets.UTF_8;
        Set<String> keySet = inverted_hashes[0].getHash().keySet();

        // Calculamos el número de ficheros a crear en función del núemro de claves que hay en el hash.
        // Número máximio de ficheros para salvar el índice invertido.
        int DIndexMaxNumberOfFiles = 1000;
        if (keySet.size()> DIndexMaxNumberOfFiles)
            numberOfFiles = DIndexMaxNumberOfFiles;
        else{
            numberOfFiles = keySet.size();
            //System.out.print(keySet);//TODO: Debug
            //System.out.print(keySet.size());
        }
        Iterator keyIterator = keySet.iterator();
        remainingKeys =  keySet.size();
        remainingFiles = numberOfFiles;
        



        if (args.length > 3) {
            for(int i = 1; i < numThreads; i++){
                inverted_hashes[0].SaveIndexConc(args[3], numThreads);
            }
        }
        else
            for(int i = 1; i < numThreads; i++){
                inverted_hashes[0].PrintIndex();
            }


        /* UNDER CONSTRUCTION */


        /*if (args.length > 3) { //TODO: Descomentar
            inverted_hashes[0].SaveIndex(args[3]);
        }
        else
            inverted_hashes[0].PrintIndex();*/

        /* Actualizar método para el testing */
        if(debug.get()) System.err.println("Fi guardar resultats");

        inv_index.setHash(inverted_hashes[0].getHash());
    }

    private static int[] balanceoCarga_v2(String file_name){

        File file = new File(file_name);
        int[] threadCharge = new int[numThreads];
        float real_end = file.length() - 10 + 1; //TODO: KeySize hardcodejat a 10, s'ha de generalitzar
        //System.out.println(file.length());
        for(int i = 0; i < numThreads; i++){
            threadCharge[i]= (int) Math.floor((float)real_end/ numThreads);
        }

        for(int i = 0; i<(int)real_end% numThreads; i++){
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
        int[] threadCharge = new int[numThreads];
        //System.out.println(file.length());
        for(int i = 0; i < numThreads; i++){
            threadCharge[i]= (int) Math.floor((float)file.length()/ numThreads);
        }

        for(int i = 0; i<(int)file.length()% numThreads; i++){
            threadCharge[i]++;
        }
        return threadCharge;
        //Bucle per comprovar que el balanceo és correcte //TODO:Treure
        /*for(int i = 0;i < num_threads;i++){
            System.out.print(threadCharge[i]+"\n");A----------------
        }*/
    }

    public static class partsSaveIndex implements Runnable{

        public partsSaveIndex(){

        }
        public void run(){

        }
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


