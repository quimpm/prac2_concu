package eps.scp;

import static org.junit.jupiter.api.Assertions.*;

class IndexingTest {


    @org.junit.jupiter.api.Test
    void main() {
        testBuildIndexExample1();
    }

    public void testBuildIndexExample1(){
        String[] args = {"test/example1.txt", "10"};

        //Indexacion concurrente
        Indexing index_conc = new Indexing();
        index_conc.main(args);

        //Indexación secuencial
        Indexing_seq index_seq = new Indexing_seq();
        //System.out.print();


        assertEquals(1, 1);
    }
}