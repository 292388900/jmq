package com.ipd.jmq.common.network.v3.command;

/**
 * 客户端性能应答
 */
public class ClientProfileAck extends JMQPayload {
    private int interval;
    private int machineMetricsInterval;
    /**
     * opener for tp&metrics
     * (byte) ((tp & 0x1) | ((metrics << 1) & 0x2))
     */
    private byte opener;

    public ClientProfileAck interval(int interval) {
        setInterval(interval);
        return this;
    }

    public ClientProfileAck machineMetricsInterval(int machineMetricsInterval) {
        setMachineMetricsInterval(machineMetricsInterval);
        return this;
    }

    public ClientProfileAck opener(byte opener) {
        setOpener(opener);
        return this;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public int getMachineMetricsInterval() {
        return machineMetricsInterval;
    }

    public void setMachineMetricsInterval(int machineMetricsInterval) {
        this.machineMetricsInterval = machineMetricsInterval;
    }

    public byte getOpener() {
        return opener;
    }

    public void setOpener(byte opener) {
        this.opener = opener;
    }

    public int predictionSize() {
        return 4 + 4 + 1 + 1;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientProfileAck{");
        sb.append("interval=").append(interval);
        sb.append(", machineMetricsInterval=").append(machineMetricsInterval);
        sb.append(", opener=").append(opener);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ClientProfileAck that = (ClientProfileAck) o;

        if (interval != that.interval) {
            return false;
        }
        if (machineMetricsInterval != that.machineMetricsInterval) {
            return false;
        }
        if (opener != that.opener) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + interval;
        result = 31 * result + machineMetricsInterval;
        result = 31 * result + (int) opener;
        return result;
    }

    @Override
    public int type() {
        return CmdTypes.CLIENT_PROFILE_ACK;
    }
}
