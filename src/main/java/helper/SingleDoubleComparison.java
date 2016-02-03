package helper;

import java.util.ArrayList;
import java.util.Random;
import java.util.List;

/**
 * Created by yongyangyu on 2/3/16.
 * This class serves as a performance comparison between single and double precision floating
 * point numbers. Here, we utilize vector inner product as an illustration.
 */
public class SingleDoubleComparison {
    public static int size = 1000000;
    public static void comparison() {
        List<List<Float>> single1 = new ArrayList<>();
        List<List<Float>> single2 = new ArrayList<>();
        double res1 = 0.0;

        Random rand = new Random();
        for (int n = 0; n < 100; n ++) {
            List<Float> x1 = new ArrayList<>();
            List<Float> x2 = new ArrayList<>();
            for (int i = 0; i < size; i++) { // generating single precision number 0 ~ 100
                x1.add(rand.nextFloat() * 100.0f);
                x2.add(rand.nextFloat() * 100.0f);
            }
            single1.add(x1);
            single2.add(x2);
        }
        long t1 = System.currentTimeMillis();
        for (int n = 0; n < 100; n ++) {
            List<Float> x1 = single1.get(n);
            List<Float> x2 = single2.get(n);
            for (int i = 0; i < size; i ++) {
                res1 += x1.get(i) * x2.get(i);
            }
        }
        System.out.println(res1);
        long t2 = System.currentTimeMillis();
        System.out.println((t2-t1)/1000.0 + " sec for " + size + " inner product for single precision.");

        List<List<Double>> doub1 = new ArrayList<>();
        List<List<Double>> doub2 = new ArrayList<>();
        double res2 = 0.0;
        for (int n = 0; n < 100; n ++) {
            List<Double> x1 = new ArrayList<>();
            List<Double> x2 = new ArrayList<>();
            for (int i = 0; i < size; i ++) {
                x1.add(rand.nextDouble() * 100.0f);
                x2.add(rand.nextDouble() * 100.0f);
            }
            doub1.add(x1);
            doub2.add(x2);
        }
        t1 = System.currentTimeMillis();
        for (int n = 0; n < 100; n ++) {
            List<Double> x1 = doub1.get(n);
            List<Double> x2 = doub2.get(n);
            for (int i = 0; i < size; i ++) {
                res2 += x1.get(i) * x2.get(i);
            }
        }
        t2 = System.currentTimeMillis();
        System.out.println(res2);
        System.out.println((t2-t1)/1000.0 + " sec for " + size + " inner product for double precision.");
    }

    public static void main (String[] args) {
        comparison();
    }
}
