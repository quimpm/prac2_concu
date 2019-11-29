package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.File;

import static java.lang.System.exit;

/**
 * Created by Nando on 8/10/19.
 */
public class Query
{

    public static void main(String[] args)
    {
        //TODO: Generalitzar, descomentar per agafar dels arguments
        int num_threads=3;

        String queryString=null, indexDirectory=null, fileName=null;
        int start=0,end=0,index;
        File folder;
        int[] threadsCharge;
        Thread[] threads_storage = new Thread[num_threads];
        InvertedIndexConc[] inverted_hashes = new InvertedIndexConc[num_threads];
        File[] threadListOfFiles;


        if (args.length <2 || args.length>4)
            System.err.println("Erro in Parameters. Usage: Query <String> <IndexDirectory> <filename> [<Key_Size>]");
        if (args.length > 0)
            queryString = args[0];
        if (args.length > 1)
            indexDirectory = args[1];
        if (args.length > 2)
            fileName = args[2];
        if (args.length > 3)
            for (int i = 0; i < num_threads; i++) inverted_hashes[i] = new InvertedIndexConc(Integer.parseInt(args[3]));
        else
            for (int i = 0; i < num_threads; i++) inverted_hashes[i] = new InvertedIndexConc();
        /* Agafar nombre de threads dels arguments
        if(args.length > 4)
            num_threads=Integer.parseInt(args[4]);*/

        //Agafem la llista de fitxers continguts dincs de la carpeta folder
        folder= new File(indexDirectory);
        File[] listOfFiles = folder.listFiles();

        //Fem el balanceo de carga per cada thread
        threadsCharge=balanceoCarga(listOfFiles.length, num_threads);

        //Creació fils
        for(int i = 0; i < num_threads; i++){
            index=0;
            end += threadsCharge[i] - 1;

            //Creem i omplim la llista de referències a fitxers els quals cada thread haurà de llegir.
            threadListOfFiles = new File[threadsCharge[i]];
            for(int j=start;j<=end;j++){
                threadListOfFiles[index]=listOfFiles[j];
                index++;
            }

            System.out.println("Thread " + i + "; Carga:" + threadsCharge[i] + "; Start " + start + "; End " + end);

            //Creem thread
            threads_storage[i] =  new Thread(new partsLoadIndex(threadListOfFiles, inverted_hashes[i]));
            threads_storage[i].start();

            start += threadsCharge[i];
            end++;
        }

        /* Join de fils */
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
        inverted_hashes[0].SetFileName(fileName);

        //inverted_hashes[0].PrintIndex();
        inverted_hashes[0].Query(queryString);
    }

    public static int[] balanceoCarga(int num_files, int num_threads){

        int[] threadCharge = new int[num_threads];
        //System.out.println(file.length());
        for(int i = 0;i < num_threads;i++){
            threadCharge[i]= (int) Math.floor(num_files/num_threads);
        }

        for(int i = 0; i<num_files%num_threads; i++){
            threadCharge[i]++;
        }
        return threadCharge;
    }

    public static class partsLoadIndex implements Runnable{


        public int start, end;
        public InvertedIndexConc hash;
        File[] listOfFiles;

        public partsLoadIndex(File[] listOfFiles, InvertedIndexConc hash){
            this.listOfFiles=listOfFiles;
            this.hash=hash;
        }

        public void run(){
            hash.LoadIndex(listOfFiles);
        }

    }

}

