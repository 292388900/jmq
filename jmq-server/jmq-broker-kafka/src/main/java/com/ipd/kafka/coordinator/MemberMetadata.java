package com.ipd.kafka.coordinator;

import java.util.*;

/**
 * Created by zhangkepeng on 17-2-13.
 */
public class MemberMetadata {

    private byte[] assignment = null;
    private GroupCoordinator.JoinCallback awaitingJoinCallback = null;
    private GroupCoordinator.SyncCallback awaitingSyncCallback = null;
    private long latestHeartbeat = -1;
    private boolean isLeaving = false;

    private String memberId;
    private String groupId;
    private String clientId;
    private String clientHost;
    private int sessionTimeoutMs;
    private Map<String, byte[]> supportedProtocols;

    public MemberMetadata(String memberId, String groupId, String clientId, String clientHost, int sessionTimeoutMs, Map<String, byte[]> supportedProtocols) {
        this.memberId = memberId;
        this.groupId = groupId;
        this.clientId = clientId;
        this.clientHost = clientHost;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.supportedProtocols = supportedProtocols;
    }

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public String getMemberId() {
        return memberId;
    }

    public String getGroupId() {
        return groupId;
    }

    public boolean isLeaving() {
        return isLeaving;
    }

    public void setLeaving(boolean isLeaving) {
        this.isLeaving = isLeaving;
    }

    public long getLatestHeartbeat() {
        return latestHeartbeat;
    }

    public Map<String, byte[]> getSupportedProtocols() {
        return supportedProtocols;
    }

    public void setSupportedProtocols(Map<String, byte[]> supportedProtocols) {
        this.supportedProtocols = supportedProtocols;
    }

    public void setLatestHeartbeat(long latestHeartbeat) {
        this.latestHeartbeat = latestHeartbeat;
    }

    public byte[] getAssignment() {
        return assignment;
    }

    public void setAssignment(byte[] assignment) {
        this.assignment = assignment;
    }

    public GroupCoordinator.JoinCallback getAwaitingJoinCallback() {
        return awaitingJoinCallback;
    }

    public void setAwaitingJoinCallback(GroupCoordinator.JoinCallback awaitingJoinCallback) {
        this.awaitingJoinCallback = awaitingJoinCallback;
    }

    public void setAwaitingSyncCallback(GroupCoordinator.SyncCallback awaitingSyncCallback) {
        this.awaitingSyncCallback = awaitingSyncCallback;
    }

    public GroupCoordinator.SyncCallback getAwaitingSyncCallback() {
        return awaitingSyncCallback;
    }

    /**
     * Check if the provided protocol metadata matches the currently stored metadata.
     */
    protected boolean matches(Map<String, byte[]> protocols) {
        if (protocols != null && supportedProtocols != null) {
            if (protocols.size() != this.supportedProtocols.size()) {
                return false;
            }
            Iterator<Map.Entry<String, byte[]>> protocolsIter = protocols.entrySet().iterator();
            while(protocolsIter.hasNext()){
                Map.Entry<String, byte[]> protocolsEntry = (Map.Entry<String, byte[]>) protocolsIter.next();
                byte[] protocolsValue = protocolsEntry.getValue() == null ? null : protocolsEntry.getValue();
                byte[] supportedProtocolsvalue = supportedProtocols.get(protocolsEntry.getKey()) == null ? null : supportedProtocols.get(protocolsEntry.getKey());

                if (!Arrays.equals(protocolsValue, supportedProtocolsvalue)) {
                    return false;
                }
            }
        }
        return true;
    }

    protected Set<String> protocols() {
        if (supportedProtocols != null && !supportedProtocols.isEmpty()) {
            return supportedProtocols.keySet();
        } else {
            return null;
        }
    }

    /**
     * Get metadata corresponding to the provided protocol.
     */
    protected byte[] metadata(String protocol) {
        if (supportedProtocols != null && !supportedProtocols.isEmpty()) {
            Set<String> protocols = supportedProtocols.keySet();
            for (String supportProtocol : protocols) {
                if (supportProtocol.equals(protocol)) {
                    return supportedProtocols.get(supportProtocol);
                }
            }
        }
        throw new IllegalArgumentException("Member does not support protocol");
    }

    protected MemberSummary summary(String protocol) {
        return new MemberSummary(memberId, clientId, clientHost, metadata(protocol), assignment);
    }

    protected MemberSummary summaryNoMetadata() {
        return new MemberSummary(memberId, clientId, clientHost, null, null);
    }

    /**
     * Vote for one of the potential group protocols. This takes into account the protocol preference as
     * indicated by the order of supported protocols and returns the first one also contained in the set
     */
    protected String vote(Set<String> candidates) {
        if (candidates != null && !candidates.isEmpty()) {
            if (supportedProtocols != null && !supportedProtocols.isEmpty()) {
                Set<String> protocols = supportedProtocols.keySet();
                for (String protocol : protocols) {
                    if (candidates.contains(protocol)) {
                        return protocol;
                    }
                }
            }
        }
        throw new IllegalArgumentException("Member does not support any of the candidate protocols");
    }

    @Override
    public String toString() {
        return String.format("[%s,%s,%s,%s,%d]", memberId, groupId, clientId, clientHost, sessionTimeoutMs);
    }

    public static class MemberSummary {

        private String memberId;
        private String clientId;
        private String clientHost;
        private byte[] metadata;
        private byte[] assignment;

        public MemberSummary(String memberId,
                             String clientId,
                             String clientHost,
                             byte[] metadata,
                             byte[] assignment) {
            this.memberId = memberId;
            this.clientId = clientId;
            this.clientHost = clientHost;
            this.metadata = metadata;
            this.assignment = assignment;
        }
    }
}
