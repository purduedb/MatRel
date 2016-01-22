package helper;

import java.util.*;

/**
 * Created by yongyangyu on 11/13/15.
 */
public class RankData {
    // According to the document of scipy.stats.mstats.rankdata,
    // if some values are tied, their rank is averaged. We follow the same logic here.
    public static double[] rankWithNoMissing(String line) {
        if (line.contains("Sample") || line.contains("#") || line.contains("HG") || line.contains("NA"))
            return new double[]{0.0};
        String[] strs = line.split("\t");
        List<Integer> cols = new ArrayList<>();
        double[] data = new double[strs.length];
        int nanCnt = 0;
        for (int i = 0; i < data.length; i ++) {
            data[i] = Double.parseDouble(strs[i]);
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
                while (curr < data.length && Math.abs(data[i] - cp[curr]) < 1e-8) {
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
        if (line.contains("Sample") || line.contains("#") || line.contains("HG") || line.contains("NA"))
            return new int[0];
        String[] strs = line.split("\t");
        List<Integer> cols = new ArrayList<>();
        double[] data = new double[strs.length];
        int nanCnt = 0;
        int row = Integer.parseInt(strs[0]);
        for (int i = 0; i < data.length; i ++) {
            data[i] = Double.parseDouble(strs[i]);
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
        // floating point number equal
        if (hi >= data.length || Math.abs(data[hi] - target) > 1e-8) {
            return -1;
        }
        return hi;
    }

    public static void main(String[] args) {
        String line = "0.220815\t-0.003084\t0.120763\t0.130911\t-0.133571\t0.173637\t-0.075663\t-0.276158\t-0.331503\t0.415624";
        double[] res = rankWithNoMissing(line);
        System.out.println(Arrays.toString(res));
    }
}
