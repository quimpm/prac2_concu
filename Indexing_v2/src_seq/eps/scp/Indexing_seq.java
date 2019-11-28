package eps.scp;

public class Indexing_seq {
    // UNDER CONSTRUCTIOON vvvvvvvv
    private static InvertedIndex_seq inv_index = new InvertedIndex_seq();

    public Indexing_seq(String[] args){
        main(args);
    }

    public InvertedIndex_seq get_InvertedIndex_seq(){
        return inv_index;
    }

    // UNDER CONSTRUCTIOON ^^^^^^

    public static void main(String[] args)
    {
        InvertedIndex_seq hash;

        if (args.length <1 || args.length>4)
            System.err.println("Erro in Parameters. Usage: Indexing <TextFile> [<Key_Size>] [<Index_Directory>]");
        if (args.length < 2)
            hash = new InvertedIndex_seq(args[0]);
        else
            hash = new InvertedIndex_seq(args[0], Integer.parseInt(args[1]));

        hash.BuildIndex();

        if (args.length > 2)
            hash.SaveIndex(args[2]);
        else
            hash.PrintIndex();

        inv_index.setHash(hash.getHash());
    }

}
