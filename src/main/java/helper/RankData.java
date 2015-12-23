package helper;

import java.util.*;

/**
 * Created by yongyangyu on 11/13/15.
 */
public class RankData {
    // According to the document of scipy.stats.mstats.rankdata,
    // if some values are tied, their rank is averaged. We follow the same logic here.
    public static double[] rankWithNoMissing(String line) {
        if (line.contains("Sample")) return new double[]{0.0};
        String[] strs = line.split("\t");
        List<Integer> cols = new ArrayList<>();
        double[] data = new double[strs.length-1];
        int nanCnt = 0;
        for (int i = 0; i < data.length; i ++) {
            data[i] = Double.parseDouble(strs[i+1]);
            if (Double.isNaN(data[i])) {
                nanCnt ++;
                cols.add(i);
            }
        }
        double[] cp = new double[data.length];
        System.arraycopy(data, 0, cp, 0, data.length);
        Arrays.sort(cp);
        double [] res = new double[data.length];
        for (int i = 0; i < res.length; i ++) {
            if (Double.isNaN(data[i])) {
                res[i] = data.length - nanCnt + 1;
                nanCnt --;
            }
            else {
                double total = 0.0;
                int curr = binSearch(cp, data[i]);
                total += curr + 1;
                curr ++;
                int cnt = 1;
                while (curr < data.length && Math.abs(data[i] - cp[curr]) < 1e-6) {
                    total += curr + 1;
                    cnt ++;
                    curr ++;
                }
                res[i] = total / cnt;
            }
        }
        return res;
    }

    public static int[] rank(String line, Map<Integer, List<Integer>> pos) {
        if (line.contains("Sample")) return new int[0];
        String[] strs = line.split("\t");
        List<Integer> cols = new ArrayList<>();
        double[] data = new double[strs.length-1];
        int nanCnt = 0;
        int row = Integer.parseInt(strs[0]);
        for (int i = 0; i < data.length; i ++) {
            data[i] = Double.parseDouble(strs[i+1]);
            if (Double.isNaN(data[i])) {
                nanCnt ++;
                cols.add(i);
            }
        }
        double[] cp = new double[data.length];
        System.arraycopy(data, 0, cp, 0, data.length);
        Arrays.sort(cp);
        int [] res = new int[data.length];
        for (int i = 0; i < res.length; i ++) {
            if (Double.isNaN(data[i])) {
                res[i] = data.length - nanCnt + 1;
                nanCnt --;
            }
            else {
                res[i] = binSearch(cp, data[i]) + 1;
            }
        }
        if (!cols.isEmpty()) pos.put(row, cols);
        return res;
    }

    // find the first appearance of the target
    private static int binSearch(double[] data, double target) {
        int lo = -1, hi = data.length;
        //while (Double.isNaN(data[hi])) hi --;
        while (lo + 1 != hi) {
            int mid = lo + (hi-lo) / 2;
            if (data[mid] < target) lo = mid;
            else hi = mid;
        }
        if (hi >= data.length || data[hi] != target) {
            return -1;
        }
        return hi;
    }

    public static void main(String[] args) {
        String line = "3\t0.042002\t0.533541\t0.328873\t0.409464\t0.271178\t0.1061\t0.611949\t1.099074\t0.588023\t1.030736\t0.491325\t0.820836\t0.105903\t0.590658\t0.173028\t0.488444\t0.275684\t0.706454\t0.748914\t0.451721\t0.647671\t1.463691\t0.087273\t0.379609\t-0.040123\t-0.293504\t-0.058982\t1.007062\t0.75134\t1.543364\t0.325842\t0.97896\t1.020396\t1.729996\t0.538893\t-0.105836\t1.070826\t1.427554\t0.735343\t0.207176\t0.259057\t1.691009\t1.063179\t0.711104\t0.588382\t0.84449\t0.131111\t0.171285\t1.401957\t0.66138\t0.76747\t0.129931\t-0.149375\t0.282124\t0.028877\t0.945827\t1.545562\t0.225979\t0.02374\t0.36895\t0.693427\t0.444448\t0.149887\t0.856575\t0.289791\t0.833108\t-0.052045\t0.252389\t0.272205\t0.228128\t1.173777\t-0.05586\t0.415659\t0.008765\t0.139978\t0.167905\t1.583163\t1.503249\t1.188236\t0.495295\t0.502472\t1.528728\t-0.197742\t0.108489\t0.346492\t0.338715\t0.963509\t0.02637\t1.112872\t-0.173453\t0.479336\t-0.12702\t0.538893\t0.737758\t0.81987\t0.644548\t1.379447\t-0.107274\t0.780493\t-0.218765";
        double[] res = rankWithNoMissing(line);
        System.out.println(Arrays.toString(res));
    }
}
