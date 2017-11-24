package com.ipd.jmq.test.performance.safe.check;

import java.io.IOException;

/**
 * Created by lining on 17-6-8.
 */
public class ProduceTest {

    public static void main(String[] args) {
        ProduceDataCheck produceDataCheck = new ProduceDataCheck();
        produceDataCheck.setLineSize(100);
        produceDataCheck.setSourceFolder("consume");
        try {
            produceDataCheck.start();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
