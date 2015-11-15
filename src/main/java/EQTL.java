import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yongyangyu on 11/15/15.
 */
public class EQTL {
    private final int ell;
    private int[][] geno;
    private int[][] mrna;
    private int[][][] I;
    private int[][] N;
    private double[][] S;

    public EQTL(String geno_name, String mrna_name, int l) {
        this.ell = l;
        try {
            // read geno matrix
            FileInputStream fstream = new FileInputStream(geno_name);
            List<int[]> input = new ArrayList<>();
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("Sample")) continue;
                String[] elems = line.split("\t");
                int[] x = new int[elems.length-1];
                for (int i = 0; i < x.length; i ++) {
                    x[i] = Integer.parseInt(elems[i+1]);
                }
                input.add(x);
            }
            geno = new int[input.size()][input.get(0).length];
            for (int i = 0; i < input.size(); i ++) {
                geno[i] = input.get(i);
            }
            br.close();
            // read mrna matrix and convert it into discrete version
            fstream = new FileInputStream(mrna_name);
            input = new ArrayList<>();
            br = new BufferedReader(new InputStreamReader(fstream));
            while ((line = br.readLine()) != null) {
                if (line.contains("Sample")) continue;
                input.add(RankData.rank(line));
            }
            mrna = new int[input.size()][input.get(0).length];
            for (int i = 0; i < input.size(); i ++) {
                mrna[i] = input.get(i);
            }
            br.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void generateIAndN() {
        I = new int[ell][geno.length][geno[0].length];
        for (int i = 0; i < I.length; i ++) {
            for (int m = 0; m < geno.length; m ++) {
                for (int k = 0; k < geno[0].length; k ++) {
                    if (geno[m][k] == i)
                        I[i][m][k] = 1;
                }
            }
        }
        N = new int[I.length][geno.length];
        for (int i = 0; i < I.length; i ++) {
            for (int m = 0; m < geno.length; m ++) {
                for (int k = 0; k < geno[0].length; k ++) {
                    N[i][m] += I[i][m][k];
                }
            }
        }
    }

    public void computeS() {
        double[][][] Si = new double[I.length][mrna.length][geno.length];
        for (int i = 0; i < Si.length; i ++) {
            for (int n = 0; n < mrna.length; n ++) {
                for (int m = 0; m < geno.length; m ++) {
                    for (int k = 0; k < geno[0].length; k ++) {
                        Si[i][n][m] += mrna[n][k] * I[i][m][k];
                    }
                }
            }
        }
        S = new double[mrna.length][geno.length];
        int K = geno[0].length;
        for (int n = 0; n < S.length; n ++) {
            for (int m = 0; m < S[0].length; m ++) {
                double tmp = 0.0;
                for (int i = 0; i < ell; i ++) {
                    tmp += Si[i][n][m] * Si[i][n][m] / N[i][m];
                }
                S[n][m] = 12.0 / K / (K+1) * tmp - 3 * (K+1);
            }
        }
    }

    private void printMatrix(int[][] matrix) {
        System.out.println("[");
        for (int[] row: matrix) {
            System.out.println("\t" + Arrays.toString(row));
        }
        System.out.println("]");
    }

    private void printMatrix(double[][] matrix) {
        System.out.println("[");
        for (double[] row: matrix) {
            System.out.println("\t" + Arrays.toString(row));
        }
        System.out.println("]");
    }

    public static void main(String[] args) {
        String geno_name = "/Users/yongyangyu/Desktop/krux-master/test/geno.tab.tmp";
        String mrna_name = "/Users/yongyangyu/Desktop/krux-master/test/mrna.tab.tmp";
        EQTL eqtl = new EQTL(geno_name, mrna_name, 2);
        eqtl.generateIAndN();
        eqtl.computeS();
        eqtl.printMatrix(eqtl.S);
    }
}
