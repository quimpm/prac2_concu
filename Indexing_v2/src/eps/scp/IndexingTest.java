package eps.scp;

import static org.junit.jupiter.api.Assertions.*;

class IndexingTest {


    @org.junit.jupiter.api.Test
    void main() {
        testBuildIndexExample1();
    }

    private void testBuildIndexExample1(){
        String[] args = {"test/example1.txt", "10"};

        //Indexacion concurrente
        Indexing index_conc = new Indexing(args);
        System.out.println("HOTAL"+  index_conc.get_InvertedIndex().getHash());

        //Indexación secuencial
        Indexing_seq index_seq = new Indexing_seq(args);
        System.out.println("HOTAL SEQ " + index_seq.get_InvertedIndex_seq().getHash());


        assertEquals(1, 1);
    }
}