package com.ipd.jmq.replication;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by guoliang on 2016.9.12.
 */
public class TestReplicationService {
    private static final int NODE_COUNT = 5;
    private static final int START_PORT = 18000;
    private static Logger logger = LoggerFactory.getLogger(TestReplicationService.class);

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void teardown(){
    }

    @Test
    public void testReplication() throws Exception {
    }

    public static void main(String[] args) {
        TestReplicationService _this = new TestReplicationService();
        try {
            _this.setUp();
            _this.testReplication();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
