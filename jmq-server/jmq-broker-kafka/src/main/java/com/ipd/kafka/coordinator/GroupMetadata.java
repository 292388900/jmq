package com.ipd.kafka.coordinator;

import com.ipd.jmq.common.network.kafka.model.GroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by zhangkepeng on 17-2-13.
 */
public class GroupMetadata {

    private Map<String, MemberMetadata> members = new HashMap<String, MemberMetadata>();
    private GroupState state = GroupState.STABLE;
    private int generationId = 0;
    private String leaderId = null;
    private String protocol = null;

    private String groupId;
    private String protocolType;

    private static Set<GroupState> DeadPreviousStates = new HashSet<GroupState>();
    private static Set<GroupState> AwaitingSyncPreviousStates = new HashSet<GroupState>();
    private static Set<GroupState> StablePreviousStates = new HashSet<GroupState>();
    private static Set<GroupState> PreparingRebalancePreviousStates = new HashSet<GroupState>();
    private static Map<GroupState, Set<GroupState>> ValidPreviousStates = new HashMap<GroupState, Set<GroupState>>();

    static {
        DeadPreviousStates.add(GroupState.STABLE);
        DeadPreviousStates.add(GroupState.PREPARINGREBALANCE);
        DeadPreviousStates.add(GroupState.AWAITINGSYNC);
        AwaitingSyncPreviousStates.add(GroupState.PREPARINGREBALANCE);
        StablePreviousStates.add(GroupState.AWAITINGSYNC);
        PreparingRebalancePreviousStates.add(GroupState.STABLE);
        PreparingRebalancePreviousStates.add(GroupState.AWAITINGSYNC);

        ValidPreviousStates.put(GroupState.DEAD, DeadPreviousStates);
        ValidPreviousStates.put(GroupState.AWAITINGSYNC, AwaitingSyncPreviousStates);
        ValidPreviousStates.put(GroupState.STABLE, StablePreviousStates);
        ValidPreviousStates.put(GroupState.PREPARINGREBALANCE, PreparingRebalancePreviousStates);
    }

    public GroupMetadata(String groupId, String protocolType) {
        this.groupId = groupId;
        this.protocolType = protocolType;
    }

    protected GroupState getState() {
        return state;
    }

    protected String getGroupId() {
        return groupId;
    }

    protected String getProtocolType() {
        return protocolType;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public Map<String, MemberMetadata> getMembers() {
        return members;
    }

    public int getGenerationId() {
        return generationId;
    }

    public String getProtocol() {
        return protocol;
    }

    protected boolean is(GroupState groupState)  {
        return state == groupState;
    }
    protected boolean not(GroupState groupState) {
        return state != groupState;
    }
    protected boolean has(String memberId) {
        return members.containsKey(memberId);
    }
    protected MemberMetadata get(String memberId) {
        return members.get(memberId);
    }

    protected void add(String memberId, MemberMetadata member) {

        if (!supportsProtocols(member.protocols())) {
            throw new RuntimeException("no support protocol");
        }

        if (leaderId == null) {
            leaderId = memberId;
        }
        members.put(memberId, member);
        //System.out.println(System.nanoTime() + " | " + memberId + " was added by " + Thread.currentThread().getName());
    }

    protected void remove(String memberId) {
        members.remove(memberId);
        //System.out.println(System.nanoTime() + " | " + memberId + " was removed by " + Thread.currentThread().getName());
        if (memberId.equals(leaderId)) {
            if (members.isEmpty()) {
                leaderId = null;
            } else {
                Set<String> memberIds = members.keySet();
                leaderId = memberIds.iterator().next();
            }
        }
    }

    protected GroupState currentState() {
        return state;
    }

    protected boolean isEmpty() {
        return members.isEmpty();
    }

    protected Set<MemberMetadata> notYetRejoinedMembers() {
        Collection<MemberMetadata> memberMetadataCollection = members.values();
        //System.out.println("size of memberMetadataCollection : " + memberMetadataCollection.size());
        Set<MemberMetadata> notYetRejoinedMembers = new HashSet<MemberMetadata>();
        if (!memberMetadataCollection.isEmpty()) {
            for (MemberMetadata memberMetadata : memberMetadataCollection) {
                //System.out.println("notYetRejoinedMembers : " + memberMetadata.getMemberId() + " | " + memberMetadata.getAwaitingJoinCallback());
                if (memberMetadata.getAwaitingJoinCallback() == null) {
                    notYetRejoinedMembers.add(memberMetadata);
                }
            }
        }
        return notYetRejoinedMembers;
    }

    protected Set<String> allMembers() {
        return members.keySet();
    }

    protected Set<MemberMetadata> allMemberMetadata() {
        Collection<MemberMetadata> memberMetadataCollection = members.values();
        Set<MemberMetadata> memberMetadatas = new HashSet<MemberMetadata>(memberMetadataCollection);
        return memberMetadatas;
    }

    protected int rebalanceTimeout() {
        Collection<MemberMetadata> memberMetadatas = members.values();
        int max = 0;
        if (memberMetadatas != null && !memberMetadatas.isEmpty()) {
            for (MemberMetadata memberMetadata : memberMetadatas) {
                if (max < memberMetadata.getSessionTimeoutMs()) {
                    max = memberMetadata.getSessionTimeoutMs();
                }
            }
        }
        return max;
    }

    protected String generateMemberIdSuffix() {
        return UUID.randomUUID().toString();
    }

    protected boolean canRebalance() {
        return state == GroupState.STABLE || state == GroupState.AWAITINGSYNC;
    }

    protected void transitionTo(GroupState groupState) {
        assertValidTransition(groupState);
        state = groupState;
    }

    protected String selectProtocol() {
        if (members.isEmpty()) {
            throw new IllegalStateException("Cannot select protocol for empty group");
        }

        // select the protocol for this group which is supported by all members
        Set<String> candidates = candidateProtocols();
        Map<String, Integer> mapValues = new HashMap<String, Integer>();

        // let each member vote for one of the protocols and choose the one with the most votes
        Set<MemberMetadata> allMemberMetadata = allMemberMetadata();
        for (MemberMetadata memberMetadata : allMemberMetadata) {
            String protocol = memberMetadata.vote(candidates);
            Integer value = mapValues.get(protocol);
            if (value == null) {
                mapValues.put(protocol, 1);
            } else {
                value++;
                mapValues.put(protocol, value);
            }
        }
        String selectProtocol = null;
        int max = 0;
        Iterator<Map.Entry<String, Integer>> iter = mapValues.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Integer> entry = iter.next();
            String protocol = entry.getKey();
            int size = entry.getValue();
            if (max < size) {
                selectProtocol = protocol;
            }
        }
        return selectProtocol;
    }

    private Set<String> candidateProtocols() {
        // get the set of protocols that are commonly supported by all members
        Set<MemberMetadata> allMemberMetadata = allMemberMetadata();
        Set<String> commonProtocols = new HashSet<String>();
        List<Set<String>> allMemberProtocol = new ArrayList<Set<String>>();
        if (allMemberMetadata != null) {
            for (MemberMetadata memberMetadata : allMemberMetadata) {
                Set<String> protocols = memberMetadata.protocols();
                allMemberProtocol.add(protocols);
            }
        }
        for (int i = 0; i < allMemberProtocol.size(); i++) {
            if (i == 0) {
                commonProtocols.addAll(allMemberProtocol.get(i));
            } else {
                commonProtocols.retainAll(allMemberProtocol.get(i));
            }
        }

        return commonProtocols;
    }

    protected boolean supportsProtocols(Set<String> memberProtocols) {
        Set<String> result = new HashSet<String>();
        result.addAll(memberProtocols);
        result.retainAll(candidateProtocols());
        return isEmpty() || !result.isEmpty();
    }

    protected void initNextGeneration() {
        Set<MemberMetadata> memberMetadataSet = notYetRejoinedMembers();
        if (memberMetadataSet != null && !memberMetadataSet.isEmpty()) {
            throw new RuntimeException("not yet reJoined members is not empty");
        }
        generationId += 1;
        protocol = selectProtocol();
        //System.out.println("protocol now are : " + protocol + " | generationId : " + generationId);
        transitionTo(GroupState.AWAITINGSYNC);
    }

    protected Map<String, byte[]> currentMemberMetadata() {
        if (is(GroupState.DEAD) || is(GroupState.PREPARINGREBALANCE)) {
            throw new IllegalStateException("Cannot obtain member metadata for group in state " + state.toString());
        }
        Map<String, byte[]> memeberMeta = new HashMap<String, byte[]>();
        Iterator<Map.Entry<String, MemberMetadata>> iterator = members.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String,MemberMetadata> entry = iterator.next();
            String memberId = entry.getKey();
            MemberMetadata memberMetadata = entry.getValue();
            byte[] metadata = memberMetadata.metadata(protocol);
            memeberMeta.put(memberId, metadata);
        }
        return memeberMeta;
    }

    private void assertValidTransition(GroupState targetState) {
        if (!GroupMetadata.ValidPreviousStates.get(targetState).contains(state)) {
            throw new IllegalStateException(String.format("Group %s should be in the %s states before moving to %s state. Instead it is in %s state",
                    groupId, GroupMetadata.ValidPreviousStates.get(targetState), targetState, state));
        }
    }

    @Override
    public String toString() {
        return String.format("[%s,%s,%s,%s]", groupId, protocolType, currentState().toString(), members);
    }
}
