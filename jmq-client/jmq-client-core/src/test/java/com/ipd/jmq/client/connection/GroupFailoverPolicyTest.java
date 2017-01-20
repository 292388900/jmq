package com.ipd.jmq.client.connection;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.cluster.Permission;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hexiaofeng on 14-6-6.
 */
public class GroupFailoverPolicyTest {

    @Test
    public void testSort(){
        BrokerGroup group=new BrokerGroup();
        group.addBroker(new Broker("127.0.0.2",50000,"mq1_s"));
        group.addBroker(new Broker("127.0.0.3",50000,"mq1_b"));
        group.addBroker(new Broker("127.0.0.1",50000,"mq1_m"));

        GroupFailoverPolicy policy=new GroupFailoverPolicy(group, Permission.WRITE);
        Assert.assertEquals(policy.getAddresses().size(),2);
        Assert.assertEquals(policy.getAddresses().get(0),"127_0_0_1_50000");
        Assert.assertEquals(policy.getAddresses().get(1),"127_0_0_2_50000");
        policy=new GroupFailoverPolicy(group, Permission.READ);
        Assert.assertEquals(policy.getAddresses().size(),3);
        Assert.assertEquals(policy.getAddresses().get(0),"127_0_0_1_50000");
        Assert.assertEquals(policy.getAddresses().get(1),"127_0_0_2_50000");
        Assert.assertEquals(policy.getAddresses().get(2),"127_0_0_3_50000");

    }

}
