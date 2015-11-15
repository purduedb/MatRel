import java.util.Arrays;

/**
 * Created by yongyangyu on 11/13/15.
 */
public class RankData {
    public static int[] rank(String line) {
        if (line.contains("Sample")) return new int[0];
        String[] strs = line.split("\t");
        double[] data = new double[strs.length-1];
        //int nanCnt = 0;
        for (int i = 0; i < data.length; i ++) {
            data[i] = Double.parseDouble(strs[i+1]);
            //if (Double.isNaN(data[i])) nanCnt ++;
        }
        double[] cp = new double[data.length];
        System.arraycopy(data, 0, cp, 0, data.length);
        Arrays.sort(cp);
        int [] res = new int[data.length];
        for (int i = 0; i < res.length; i ++) {
            if (Double.isNaN(data[i])) {
                res[i] = 0;//data.length - nanCnt + 1;
                //nanCnt --;
            }
            else {
                res[i] = binSearch(cp, data[i]) + 1;
            }
        }
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
        int[] res = rank(line);
        System.out.println(Arrays.toString(res));
    }
}
