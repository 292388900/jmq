package com.ipd.jmq.client.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by llw on 15-5-19.
 */
public class ExecutorTest {

    public static void main(String[] args) {


        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        System.out.println(System.currentTimeMillis() / 1000);

        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                System.out.println(System.currentTimeMillis() / 1000);
            }
        }, 2000, 5 * 1000, TimeUnit.MILLISECONDS);

    }
}
