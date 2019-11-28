package eps.scp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IndexingTest {

/*
    @org.junit.jupiter.api.Test
    void main() {
        testBuildIndexExample1();
        testBuildIndexExample2();
        //testBuildIndexExample3();
        testBuildIndexExample4();

    }*/

    @Test
    public void testBuildIndexExample1(){
        String[] args = {"test/example1.txt", "10", "Output/example1"};

        //Indexacion concurrente
        IndexingConc index_conc = new IndexingConc(args);
        //System.out.println("HOTAL"+  index_conc.get_InvertedIndex().getHash());

        //Indexación secuencial
        IndexingSeq index_seq = new IndexingSeq(args);
        //System.out.println("HOTAL SEQ " + index_seq.get_InvertedIndex_seq().getHash());

        assertEquals(index_seq.get_InvertedIndex_seq().getHash(), index_conc.get_InvertedIndex().getHash());
    }

    @Test
    public void testBuildIndexExample2(){
        String[] args = {"test/example2.txt", "10", "Output/example2"};
        IndexingConc index_conc = new IndexingConc(args);
        IndexingSeq index_seq = new IndexingSeq(args);
        System.out.println("HOTAC"+  index_conc.get_InvertedIndex().getHash());
        System.out.println("HOTAS" + index_seq.get_InvertedIndex_seq().getHash());

        assertEquals(index_conc.get_InvertedIndex().getHash(), index_seq.get_InvertedIndex_seq().getHash());
    }

    @Test
    public void testBuildIndexExample3(){
        String[] args = {"test/example3.txt", "10", "Output/example3"};
        IndexingConc index_conc = new IndexingConc(args);
        IndexingSeq index_seq = new IndexingSeq(args);
        System.out.println("HOTAC"+  index_conc.get_InvertedIndex().getHash());
        System.out.println("HOTAS " + index_seq.get_InvertedIndex_seq().getHash());

        assertEquals(index_conc.get_InvertedIndex().getHash(), index_seq.get_InvertedIndex_seq().getHash());
    }

    @Test
    public void testBuildIndexExample4(){
        String[] args = {"test/example4.txt", "10", "Output/example4"};
        IndexingConc index_conc = new IndexingConc(args);
        IndexingSeq index_seq = new IndexingSeq(args);
        System.out.println("HOTAC"+  index_conc.get_InvertedIndex().getHash());
        System.out.println("HOTAS " + index_seq.get_InvertedIndex_seq().getHash());

        assertEquals(index_conc.get_InvertedIndex().getHash(), index_seq.get_InvertedIndex_seq().getHash());
    }

    /*@Test
    public void testBuildIndexQuijote(){
        String[] args = {"test/pg2000.txt", "10", "Output/quijote"};
        IndexingConc index_conc = new IndexingConc(args);
        Indexing_seq index_seq = new Indexing_seq(args);

        assertEquals(index_conc.get_InvertedIndex().getHash(), index_seq.get_InvertedIndex_seq().getHash());
    }*/
}