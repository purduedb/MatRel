import java.util.Arrays;

/**
 * Created by yongyangyu on 11/13/15.
 */
public class RankData {
    public static int[] rank(String line) {
        if (line.contains("Sample")) return new int[0];
        String[] strs = line.split("\t");
        double[] data = new double[strs.length-1];
        for (int i = 0; i < data.length; i ++) {
            data[i] = Double.parseDouble(strs[i+1]);
        }
        double[] cp = new double[data.length];
        System.arraycopy(data, 0, cp, 0, data.length);
        Arrays.sort(cp);
        int [] res = new int[data.length];
        for (int i = 0; i < res.length; i ++) {
            res[i] = binSearch(cp, data[i]) + 1;
        }
        return res;
    }

    private static int binSearch(double[] data, double target) {
        int lo = 0, hi = data.length-1;
        while (lo <= hi) {
            int mid = lo + (hi-lo) / 2;
            if (data[mid] == target) return mid;
            else if (data[mid] > target) hi = mid - 1;
            else lo = mid + 1;
        }
        return -1;
    }

    public static void main(String[] args) {
        String line = "10\t0.19952\t0.240309\t-0.020509\t0.058591\t0.176037\t0.395668\t-1.177776\t0.069836\t-0.063251\t-0.098166";
        int[] res = rank(line);
        System.out.println(Arrays.toString(res));
    }
}
