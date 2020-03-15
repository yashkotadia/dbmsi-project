package diskmgr;
public class pcounter {
    public static int rcounter;
    public static int wcounter;
    public static void initialize() {
        rcounter =0;wcounter =0;
    }
    public static void readIncrement() {
        rcounter++;
    }
    public static void writeIncrement(){
        wcounter++;
    }
    public static void printReads(){
        System.out.println("The number of reads is:" +rcounter);
    }
    public static void printWrites(){
        System.out.println("The number of writes is:" +wcounter);
    }
}