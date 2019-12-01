package eps.scp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

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
        String[] argsConc = {"test/example1.txt", "4", "10", "Output/example1"};
        String[] argsSeq = {"test/example1.txt", "10", "Output/example1"};

        //Indexacion concurrente
        IndexingConc index_conc = new IndexingConc(argsConc);

        //Indexación secuencial
        IndexingSeq index_seq = new IndexingSeq(argsSeq);

        assertEquals(index_seq.get_InvertedIndex_seq().getHash(), index_conc.get_InvertedIndex().getHash());
    }

    @Test
    public void testBuildIndexExample2(){
        String[] argsConc = {"test/example2.txt", "4", "10", "Output/example2"};
        String[] argsSeq = {"test/example2.txt", "10", "Output/example2"};
        IndexingConc indexConc = new IndexingConc(argsConc);
        IndexingSeq indexSeq = new IndexingSeq(argsSeq);

        assertEquals(indexConc.get_InvertedIndex().getHash(), indexSeq.get_InvertedIndex_seq().getHash());
    }

    @Test
    public void testBuildIndexExample3(){
        String[] argsConc = {"test/example3.txt", "4", "10", "Output/example3"};
        String[] argsSeq = {"test/example3.txt", "10", "Output/example3"};
        IndexingConc indexConc = new IndexingConc(argsConc);
        IndexingSeq indexSeq = new IndexingSeq(argsSeq);

        assertEquals(indexConc.get_InvertedIndex().getHash(), indexSeq.get_InvertedIndex_seq().getHash());
    }

    @Test
    public void testBuildIndexExample4(){
        String[] argsConc = {"test/example4.txt", "4", "10", "Output/example4"};
        String[] argsSeq = {"test/example4.txt", "10", "Output/example4"};
        IndexingConc indexConc = new IndexingConc(argsConc);
        IndexingSeq indexSeq = new IndexingSeq(argsSeq);

        assertEquals(indexConc.get_InvertedIndex().getHash(), indexSeq.get_InvertedIndex_seq().getHash());
    }

    @Test
    public void testArgumentsNoArguments(){
        String[] args_conc = {};
        try {
            new IndexingConc(args_conc);
            fail("Exception not thrown");
        }catch (Exception e){
            //No statements needed
        }
    }

    @Test
    public void testArgumentsOneFiveArgument(){
        String[] args_conc_1 = {"test/example3.txt"};
        String[] args_conc_2 = {"test/example3.txt", "4", "10", "Output/example3", "This should not happen"};
        try {
            new IndexingConc(args_conc_1);
            fail("Exception not thrown");
        }catch (Exception e){/*No statements needed*/}
        try {
            new IndexingConc(args_conc_2);
            fail("Exception not thrown");
        }catch (Exception e){/*No statements needed*/}
}

    @Test
    public void testArgumentsTwoThreeFourArguments(){
        String[] argsConc1 = {"test/example3.txt", "4"};
        String[] argsConc2 = {"test/example3.txt", "4", "10"};
        String[] argsConc3 = {"test/example3.txt", "4", "10", "Output/example3"};

        try {
            new IndexingConc(argsConc1);
        }catch (Exception e){
            System.out.print(e.getMessage());
            fail("Exception thrown and should not");}
        try{
            new IndexingConc(argsConc2);
        }catch (Exception e){fail("Exception thrown and should not");}
        try{
            new IndexingConc(argsConc3);
        }catch (Exception e){fail("Exception thrown and should not");}
    }
}