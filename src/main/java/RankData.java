import org.apache.commons.collections.map.HashedMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yongyangyu on 11/13/15.
 */
public class RankData {
    public static int[] rank(String line, long row, Map<Long, List<Integer>> pos) {
        if (line.contains("Sample")) return new int[0];
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

    private static int binSearch(double[] data, double target) {
        int lo = 0, hi = data.length-1;
        while (Double.isNaN(data[hi])) hi --;
        while (lo <= hi) {
            int mid = lo + (hi-lo) / 2;
            if (data[mid] == target) return mid;
            else if (data[mid] > target) hi = mid - 1;
            else lo = mid + 1;
        }
        System.err.println("NaN found during binSearch!");
        return -1;
    }

    public static void main(String[] args) {
        String line = "10\tNaN\t-0.003084\t0.120763\t0.130911\t-0.133571\tNaN\t-0.075663\t-0.276158\tNaN\t0.415624";
        long row = 0;
        Map<Long, List<Integer>> pos = new HashedMap();
        int[] res = rank(line, row++, pos);
        System.out.println(Arrays.toString(res));
    }
}
