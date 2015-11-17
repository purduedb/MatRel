package helper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by yongyangyu on 11/15/15.
 */
public class EQTL {
    private final int ell;
    private boolean nanInMrna;
    private int[][] geno;
    private int[][] rankedMrna; // keep the original rank, put missing entry at the highest rank
    private Map<Integer, List<Integer>> nanPos;
    private int[][] mrna; // convert all the missing entry to rank value of 0
    private int[][] Z;
    private int[][][] I;
    private int[][] N;
    private int[][][] Ni;
    private double[][] S;
    private int[][] K;
    private Map<Integer, List<Integer>> missingCount;

    public EQTL(String geno_name, String mrna_name, int l) {
        this.ell = l;
        nanInMrna = false;
        missingCount = new HashMap<>();
        nanPos = new HashMap<>();
        int row = 0;
        try {
            // read geno matrix
            FileInputStream fstream = new FileInputStream(geno_name);
            List<int[]> input = new ArrayList<>();
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String line;
            row = 0;
            while ((line = br.readLine()) != null) {
                if (line.contains("Sample")) continue;
                String[] elems = line.split("\t");
                int[] x = new int[elems.length-1];
                for (int i = 0; i < x.length; i ++) {
                    // handling if any entry is missing for geno matrix
                    if (Double.isNaN(Double.parseDouble(elems[i+1]))) {
                        x[i] = -1;
                        if (missingCount.containsKey(row)) {
                            missingCount.get(row).add(i);
                        }
                        else {
                            List<Integer> list = new ArrayList<>();
                            list.add(i);
                            missingCount.put(row, list);
                        }
                    }
                    else {
                        x[i] = Integer.parseInt(elems[i + 1]);
                    }
                }
                input.add(x);
                row ++;
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
            row = 0;
            while ((line = br.readLine()) != null) {
                if (line.contains("Sample")) continue;
                if (!nanInMrna && line.contains("NaN")) nanInMrna = true;
                input.add(RankData.rank(line, row++, nanPos));
            }
            rankedMrna = new int[input.size()][input.get(0).length];
            for (int i = 0; i < input.size(); i ++) {
                rankedMrna[i] = input.get(i);
            }
            mrna = new int[input.size()][input.get(0).length];
            for (int i = 0; i < input.size(); i ++) {
                System.arraycopy(rankedMrna[i], 0, mrna[i], 0, mrna[i].length);
                if (!nanPos.isEmpty() && nanPos.containsKey((long)i)) {
                    for (int j: nanPos.get((long)i)) {
                        mrna[i][j] = 0;
                    }
                }
            }
            if (nanInMrna) {
                Z = new int[mrna.length][mrna[0].length];
                for (int n = 0; n < Z.length; n ++) {
                    for (int k = 0; k < Z[0].length; k ++) {
                        Z[n][k] = (mrna[n][k] == 0) ? 0 : 1;
                    }
                }
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
        if (!nanInMrna) {
            N = new int[I.length][geno.length];
            for (int i = 0; i < I.length; i++) {
                for (int m = 0; m < geno.length; m++) {
                    for (int k = 0; k < geno[0].length; k++) {
                        N[i][m] += I[i][m][k];
                    }
                }
            }
        }
        else { // compute Ni
            Ni = new int[I.length][Z[0].length][I[0][0].length];
            for (int i = 0; i < Ni.length; i ++) {
                for (int n = 0; n < Ni[0].length; n ++) {
                    for (int m = 0; m < Ni[0][0].length; m ++) {
                        for (int k = 0; k < geno[0].length; k ++) {
                            Ni[i][n][m] += Z[n][k] * I[i][m][k];
                        }
                    }
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
        if (!nanInMrna) {
            int KK = geno[0].length;
            for (int n = 0; n < S.length; n++) {
                for (int m = 0; m < S[0].length; m++) {
                    double tmp = 0.0;
                    for (int i = 0; i < ell; i++) {
                        tmp += Si[i][n][m] * Si[i][n][m] / N[i][m];
                    }
                    S[n][m] = 12.0 / KK / (KK + 1) * tmp - 3 * (KK + 1);
                }
            }
        }
        else {
            K = new int[mrna.length][geno.length];
            for (int n = 0; n < K.length; n ++) {
                for (int m = 0; m < K[0].length; m ++) {
                    for (int i = 0;i < Ni.length; i ++) {
                        K[n][m] += Ni[i][n][m];
                    }
                }
            }
            for (int n = 0; n < S.length; n ++) {
                for (int m = 0; m < S[0].length; m ++) {
                    double tmp = 0.0;
                    for (int i = 0; i < ell; i ++) {
                        tmp += Si[i][n][m] * Si[i][n][m] / Ni[i][n][m];
                    }
                    int KK = K[n][m];
                    S[n][m] = 12.0 / KK / (KK + 1) * tmp - 3 * (KK + 1);
                }
            }
        }
        if (!missingCount.isEmpty()) {
            computeMissingInS();
        }
    }

    private void computeMissingInS() {
        int total = geno[0].length;
        int[] order = new int[total];
        for (int i = 0; i < order.length; i ++) {
            order[i] = i+1;
        }
        for (int col: missingCount.keySet()) {
            int KK = total - missingCount.get(col).size();
            // for each row in S, we need to update every entry
            for (int j = 0; j < rankedMrna.length; j ++) {
                int[] mrnaOrder = rankedMrna[j];
                int[][] genoRank = new int[ell][geno[0].length];
                // set values in genoRank
                for (int i = 0; i < genoRank.length; i ++) {
                    for (int k = 0; k < mrnaOrder.length; k++) {
                        genoRank[i][mrnaOrder[k]-1] = I[i][col][k];
                    }
                }
                int[] inner = new int[ell];
                for (int i = 0; i < inner.length; i ++) {
                    for (int m = 0; m < order.length; m ++) {
                        inner[i] += order[m] * genoRank[i][m];
                    }
                }
                //System.out.println(Arrays.toString(inner));
                int[] cumSum = new int[total];
                int[] currRow = geno[col];
                for (int i = 0; i < currRow.length; i ++) {
                    cumSum[mrnaOrder[i]-1] = currRow[i] < 0 ? 1 : 0;
                }
                for (int i = 1; i < cumSum.length; i ++) {
                    cumSum[i] += cumSum[i-1];
                }
                System.out.println(Arrays.toString(cumSum));
                for (int i = 0; i < inner.length; i ++) {
                    for (int m = 0; m < cumSum.length; m ++) {
                        inner[i] -= cumSum[m] * genoRank[i][m];
                    }
                }

                for (int i = 0; i < inner.length; i ++) {
                    inner[i] = inner[i] * inner[i];
                }
                double tmp = 0.0;
                for (int i = 0; i < ell; i ++) {
                    tmp += inner[i] * 1.0 / N[i][col];
                }
                
                S[j][col] = 12.0 / KK / (KK + 1) * tmp - 3 * (KK + 1);
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
        eqtl.printMatrix(eqtl.mrna);
        eqtl.printMatrix(eqtl.S);
    }
}
