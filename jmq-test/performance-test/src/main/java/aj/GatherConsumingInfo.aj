package aj;


import com.ipd.jmq.test.performance.producer.ProducerStat;

public aspect GatherConsumingInfo {
    /**
     * 切入点：SampleService继承树中所有 public 且以 add 开头的方法。SampleServiceImpl#add(int,int)方法满足这个条件。
     */
    public pointcut serviceSendMethods(): execution(public void com.ipd.jmq.test.performance.Producer.send(..));

    void around(): serviceSendMethods() {
        long start = System.nanoTime();
        proceed();
        long duration = System.nanoTime() - start;
        ProducerStat.num.incrementAndGet();
        ProducerStat.time.addAndGet(duration);
        ProducerStat.tpStatBuffer.success(1, 1, (int) (duration / 1000000));
    }
}