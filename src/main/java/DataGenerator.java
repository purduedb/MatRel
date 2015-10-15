import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Random;

/**
 * Created by yongyangyu on 10/14/15.
 * This class generates data given certain parameters.
 * The generating procedure should be performed before the other operations.
 * And this data generator is a sequential procedure.
 */
public class DataGenerator {
    private final long nrows;
    private final long ncols;
    private final double sparsity;
    private final boolean skewType;  // true: col skew; false: row skew
    private final double skew;

    public DataGenerator(long nrows, long ncols, double sparsity, boolean skewType, double skew) {
        this.nrows = nrows;
        this.ncols = ncols;
        this.sparsity = sparsity;
        this.skew = skew;
        this.skewType = skewType;
    }

    public void generate(String filename) throws FileNotFoundException{
        PrintWriter writer = new PrintWriter(filename);
        long nnz = (long)(nrows * ncols * sparsity);
        long remains = nnz;
        System.out.println("Generating data ...");
        if (skewType) {
            List<Long> colCount = new LinkedList<>();
            long cnt = 0L;
            while (remains != 0) {
                cnt ++;
                if (cnt >= ncols) {
                    System.err.println("Col_Fill_in: there are still " + remains + " entries left to be filled in!");
                    System.exit(1);
                }
                long curr = Math.max(Math.min((long)(remains * skew), nrows), 1);
                colCount.add(curr);
                remains -= curr;
            }
            Set<Long> rows = new HashSet<>();
            Random rand = new Random();
            for (int j = 0; j < colCount.size(); j ++) {
                rows.clear();
                if (colCount.get(j) == nrows) {
                    for (long i = 0; i < nrows; i ++) {
                        rows.add(i);
                    }
                }
                else {
                    while (rows.size() < colCount.get(j)) {
                        rows.add(Math.abs(rand.nextLong()) % ncols);
                    }
                }
                for (long i: rows) {
                    writer.println(i + " " + j);
                }
            }
            writer.close();
        }
        else {
            List<Long> rowCount = new LinkedList<>();
            long cnt = 0L;
            while (remains != 0) {
                cnt ++;
                if (cnt >= nrows) {
                    System.err.println("Row_Fill_in: there are still " + remains + " entries left to be filled in!");
                    System.exit(1);
                }
                long curr = Math.max(Math.min((long)(remains * skew), ncols), 1);
                rowCount.add(curr);
                remains -= curr;
            }
            Set<Long> cols = new HashSet<>();
            Random rand = new Random();
            for (int i = 0; i < rowCount.size(); i ++) {
                cols.clear();
                if (rowCount.get(i) == ncols) {
                    for (long j = 0; j < ncols; j ++) {
                        cols.add(j);
                    }
                }
                else {
                    while (cols.size() < rowCount.get(i)) {
                        cols.add(Math.abs(rand.nextLong()) % nrows);
                    }
                }
                for (long j: cols) {
                    writer.println(i + " " + j);
                }
            }
            writer.close();
        }
        System.out.println("Generating data done!");
    }
    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Usage: DateGenerator nrows[long] ncols[long] sparsity[double] skewType[0/1] skew[double] filename");
            System.out.println("Note: for skewType, 0 means row skew; 1 means column skew");
            System.exit(1);
        }
        long nrows = Long.parseLong(args[0]);
        long ncols = Long.parseLong(args[1]);
        double sparsity = Double.parseDouble(args[2]);
        boolean skewType = Integer.parseInt(args[3]) == 0 ? false : true;
        double skew = Double.parseDouble(args[4]);
        String filename = args[5];
        DataGenerator dg = new DataGenerator(nrows, ncols, sparsity, skewType, skew);
        try {
            dg.generate(filename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
