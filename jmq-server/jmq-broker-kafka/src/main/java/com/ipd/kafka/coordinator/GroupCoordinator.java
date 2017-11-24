package com.ipd.kafka.coordinator;

import com.google.common.collect.HashMultimap;
import com.ipd.jmq.common.network.kafka.command.JoinGroupRequest;
import com.ipd.jmq.common.network.kafka.command.OffsetRequest;
import com.ipd.jmq.common.network.kafka.exception.ErrorCode;
import com.ipd.jmq.common.network.kafka.model.GroupState;
import com.ipd.jmq.common.network.kafka.model.OffsetAndMetadata;
import com.ipd.jmq.common.network.kafka.model.OffsetMetadataAndError;
import com.ipd.jmq.common.network.kafka.model.PartitionOffsetsResponse;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.kafka.DelayedOperation;
import com.ipd.jmq.common.network.kafka.model.DelayedOperationKey;
import com.ipd.kafka.DelayedOperationManager;
import com.ipd.kafka.cluster.KafkaClusterEvent;
import com.ipd.kafka.cluster.KafkaClusterManager;
import com.ipd.kafka.cluster.TopicsBrokerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by zhangkepeng on 17-2-13.
 * Modified by luoruiheng
 */
public class GroupCoordinator extends Service {
    private final static Logger logger = LoggerFactory.getLogger(GroupCoordinator.class);

    private BrokerConfig brokerConfig;
    private KafkaClusterManager clusterManager;
    private GroupMetadataManager groupManager;

    private static String NoProtocol = "";
    private static String NoLeader = "";

    // 集群更新监听器
    private EventListener<KafkaClusterEvent> kafkaClusterEventListener;

    private DelayedOperationManager<DelayedHeartbeat> heartbeatPurgatory = null;
    private DelayedOperationManager<DelayedJoin> joinPurgatory = null;

    public GroupCoordinator(BrokerConfig brokerConfig, KafkaClusterManager clusterManager,
                            DispatchService dispatchService) {
        Preconditions.checkArgument(brokerConfig != null, "BrokerConfig can't be null");
        Preconditions.checkArgument(clusterManager != null, "KafkaClusterManager can't be null");
        Preconditions.checkArgument(dispatchService != null, "DispatchService can't be null");
        this.brokerConfig = brokerConfig;
        this.clusterManager = clusterManager;
        this.groupManager = new GroupMetadataManager(brokerConfig, clusterManager, dispatchService);
        this.kafkaClusterEventListener = new KafkaClusterListener();
        heartbeatPurgatory = new DelayedOperationManager<DelayedHeartbeat>("Heartbeat");
        joinPurgatory = new DelayedOperationManager<DelayedJoin>("Rebalance");
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        groupManager.start();
        heartbeatPurgatory.start();
        joinPurgatory.start();
        clusterManager.addListener(kafkaClusterEventListener);
    }

    @Override
    protected void doStop() {
        super.doStop();
        groupManager.stop();
        heartbeatPurgatory.shutdown();
        joinPurgatory.shutdown();
        clusterManager.removeListener(kafkaClusterEventListener);
    }

    public void handleJoinGroup(String groupId, String memberId, String clientId, String clientHost, int sessionTimeoutMs,
                                String protocolType, Map<String, byte[]> protocols, JoinCallback joinCallback) {
        if (!isStarted()) {
            joinCallback.sendResponseCallback(joinError(memberId, ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE));
        } else if (!validGroupId(groupId)) {
            joinCallback.sendResponseCallback(joinError(memberId, ErrorCode.INVALID_GROUP_ID));
        } else if (sessionTimeoutMs < brokerConfig.getMinSessionTimeoutMs() ||
                sessionTimeoutMs > brokerConfig.getMaxSessionTimeoutMs()) {
            joinCallback.sendResponseCallback(joinError(memberId, ErrorCode.INVALID_SESSION_TIMEOUT));
        } else {
            // only try to create the group if the group is not unknown AND
            // the member id is UNKNOWN, if member is specified but group does not
            // exist we should reject the request
            GroupMetadata group = groupManager.getGroup(groupId);
            if (group == null) {
                if (!memberId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID)) {
                    joinCallback.sendResponseCallback(joinError(memberId, ErrorCode.UNKNOWN_MEMBER_ID));
                } else {
                    group = groupManager.addGroup(new GroupMetadata(groupId, protocolType));
                    doJoinGroup(group, memberId, clientId, clientHost, sessionTimeoutMs, protocolType, protocols, joinCallback);
                }
            } else {
                doJoinGroup(group, memberId, clientId, clientHost, sessionTimeoutMs, protocolType, protocols, joinCallback);
            }
        }
    }

    private void doJoinGroup(GroupMetadata group, String memberId, String clientId, String clientHost,
                             int sessionTimeoutMs, String protocolType, Map<String, byte[]> protocols,
                             JoinCallback responseCallback) {
        synchronized (group) {
            if (!group.getProtocolType().equals(protocolType) || !group.supportsProtocols(protocols.keySet())) {
                // if the new member does not support the group protocol, reject it
                responseCallback.sendResponseCallback(joinError(memberId, ErrorCode.INCONSISTENT_GROUP_PROTOCOL));
            } else if (!memberId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID) && !group.has(memberId)) {
                // if the member trying to register with a un-recognized id, send the response to let
                // it reset its member id and retry
                responseCallback.sendResponseCallback(joinError(memberId, ErrorCode.UNKNOWN_MEMBER_ID));
            } else {
                //logger.info("doJoinGroup, group state : " + group.getState());
                switch (group.getState()) {
                    case DEAD:
                        // if the group is marked as dead, it means some other thread has just removed the group
                        // from the coordinator metadata; this is likely that the group has migrated to some other
                        // coordinator OR the group is in a transient unstable phase. Let the member retry
                        // joining without the specified member id,
                        responseCallback.sendResponseCallback(joinError(memberId, ErrorCode.UNKNOWN_MEMBER_ID));
                        break;
                    case PREPARINGREBALANCE:
                        if (memberId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID)) {
                            //logger.info("doJoinGroup - PREPARINGREBALANCE");
                            addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback);
                        } else {
                            MemberMetadata member = group.get(memberId);
                            updateMemberAndRebalance(group, member, protocols, responseCallback);
                        }
                        break;
                    case AWAITINGSYNC:
                        if (memberId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID)) {
                            //logger.info("doJoinGroup - AWAITINGSYNC");
                            addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback);
                        } else {
                            MemberMetadata member = group.get(memberId);
                            if (member.matches(protocols)) {
                                // member is joining with the same metadata (which could be because it failed to
                                // receive the initial JoinGroup response), so just return current group information
                                // for the current generation.
                                Map<String, byte[]> members = null;
                                if (memberId.equals(group.getLeaderId())) {
                                    members = group.currentMemberMetadata();
                                } else {
                                    members = new HashMap<String, byte[]>();
                                }
                                JoinGroupResult joinGroupResult = new JoinGroupResult(members, memberId, group.getGenerationId(),
                                                                        group.getProtocol(), group.getLeaderId(), ErrorCode.NO_ERROR);
                                //logger.info("calling callback in doJoinGroup - AWAITINGSYNC");
                                responseCallback.sendResponseCallback(joinGroupResult);
                            } else {
                                // member has changed metadata, so force a rebalance
                                updateMemberAndRebalance(group, member, protocols, responseCallback);
                            }
                        }
                        break;
                    case STABLE:
                        if (memberId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID)) {
                            //logger.info("doJoinGroup - STABLE");
                            // if the member id is unknown, register the member to the group
                            addMemberAndRebalance(sessionTimeoutMs, clientId, clientHost, protocols, group, responseCallback);
                        } else {
                            MemberMetadata member = group.get(memberId);
                            if (memberId.equals(group.getLeaderId()) || !member.matches(protocols)) {
                                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                                // The latter allows the leader to trigger rebalances for changes affecting assignment
                                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                                updateMemberAndRebalance(group, member, protocols, responseCallback);
                            } else {
                                // for followers with no actual change to their metadata, just return group information
                                // for the current generation which will allow them to issue SyncGroup
                                JoinGroupResult joinGroupResult = new JoinGroupResult(new HashMap<String, byte[]>(), memberId, group.getGenerationId(),
                                                                    group.getProtocol(), group.getLeaderId(), ErrorCode.NO_ERROR);
                                //logger.info("calling callback in doJoinGroup - STABLE");
                                responseCallback.sendResponseCallback(joinGroupResult);
                            }
                        }
                        break;
                    default:
                        break;
                }

                if (group.is(GroupState.PREPARINGREBALANCE)) {
                    joinPurgatory.checkAndComplete(new DelayedOperationKey(group.getGroupId()));
                }
            }

        }
    }

    public void handleSyncGroup(String groupId, int generation, String memberId, Map<String, byte[]> groupAssignment, SyncCallback callback) {
        if (!isStarted()) {
            callback.sendResponseCallback(null, ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE);
        } else {
            GroupMetadata group = groupManager.getGroup(groupId);
            if (group == null) {
                callback.sendResponseCallback(null, ErrorCode.UNKNOWN_MEMBER_ID);
            } else {
                doSyncGroup(group, generation, memberId, groupAssignment, callback);
            }
        }
    }

    private void doSyncGroup(final GroupMetadata group, final int generationId, final String memberId, final Map<String, byte[]> groupAssignment, final
    SyncCallback callback) {
        synchronized (group) {
            if (!group.has(memberId)) {
                callback.sendResponseCallback(null, ErrorCode.UNKNOWN_MEMBER_ID);
            } else if (generationId != group.getGenerationId()) {
                callback.sendResponseCallback(null, ErrorCode.ILLEGAL_GENERATION);
            } else {
                switch (group.currentState()) {
                    case DEAD:
                        callback.sendResponseCallback(null, ErrorCode.UNKNOWN_MEMBER_ID);
                        break;
                    case PREPARINGREBALANCE:
                        callback.sendResponseCallback(null, ErrorCode.REBALANCE_IN_PROGRESS);
                        break;
                    case AWAITINGSYNC:
                        group.get(memberId).setAwaitingSyncCallback(callback);
                        completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId));
                        // if this is the leader, then we can attempt to persist state and transition to stable
                        if (memberId.equals(group.getLeaderId())) {
                            //logger.info("doSyncGroup, AWAITINGSYNC, isLeader");
                            // fill any missing members with an empty assignment
                            Set<String> allMembers = group.allMembers();
                            Set<String> groupAssignments = groupAssignment.keySet();
                            Set<String> missing = new HashSet<String>();
                            missing.addAll(allMembers);
                            missing.removeAll(groupAssignments);
                            if (!missing.isEmpty()) {
                                for (String member : missing) {
                                    groupAssignment.put(member, null);
                                }
                            }

                            synchronized (group) {
                                // another member may have joined the group while we were awaiting this callback,
                                // so we must ensure we are still in the AwaitingSync state and the same generation
                                // when it gets invoked. if we have transitioned to another state, then do nothing
                                if (group.is(GroupState.AWAITINGSYNC) && generationId == group.getGenerationId()) {
                                    setAndPropagateAssignment(group, groupAssignment);
                                    group.transitionTo(GroupState.STABLE);
                                }
                            }
                        }
                        break;
                    case STABLE:
                        // if the group is stable, we just return the current assignment
                        MemberMetadata member = group.get(memberId);
                        callback.sendResponseCallback(member.getAssignment(), ErrorCode.NO_ERROR);
                        completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId));
                        break;
                    default:
                        break;
                }
            }
        }
    }

    public void handleLeaveGroup(String groupId, String memberId, LeaveCallback callback) {
        if (!isStarted()) {
            callback.sendResponseCallback(ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE);
        } else {
            GroupMetadata group = groupManager.getGroup(groupId);
            if (group == null) {
                // if the group is marked as dead, it means some other thread has just removed the group
                // from the coordinator metadata; this is likely that the group has migrated to some other
                // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
                // joining without specified consumer id,
                callback.sendResponseCallback(ErrorCode.UNKNOWN_MEMBER_ID);
            } else {
                synchronized (group) {
                    if (group.is(GroupState.DEAD)) {
                        callback.sendResponseCallback(ErrorCode.UNKNOWN_MEMBER_ID);
                    } else if (!group.has(memberId)) {
                        callback.sendResponseCallback(ErrorCode.UNKNOWN_MEMBER_ID);
                    } else {
                        MemberMetadata member = group.get(memberId);
                        removeHeartbeatForLeavingMember(group, member);
                        onMemberFailure(group, member);
                        callback.sendResponseCallback(ErrorCode.NO_ERROR);
                    }
                }
            }
        }
    }

    public void handleHeartbeat(String groupId, String memberId, int generationId, HeartbeatCallback callback) {
        if (!isStarted()) {
            callback.sendResponseCallback(ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE);
        } else {
            GroupMetadata group = groupManager.getGroup(groupId);
            if (group == null) {
                callback.sendResponseCallback(ErrorCode.UNKNOWN_MEMBER_ID);
            } else {
                synchronized (group) {
                    if (group.is(GroupState.DEAD)) {
                        // if the group is marked as dead, it means some other thread has just removed the group
                        // from the coordinator metadata; this is likely that the group has migrated to some other
                        // coordinator OR the group is in a transient unstable phase. Let the member retry
                        // joining without the specified member id,
                        callback.sendResponseCallback(ErrorCode.UNKNOWN_MEMBER_ID);
                    } else if (!group.is(GroupState.STABLE)) {
                        callback.sendResponseCallback(ErrorCode.REBALANCE_IN_PROGRESS);
                    } else if (generationId != group.getGenerationId()) {
                        callback.sendResponseCallback(ErrorCode.ILLEGAL_GENERATION);
                    } else {
                        MemberMetadata member = group.get(memberId);
                        completeAndScheduleNextHeartbeatExpiration(group, member);
                        callback.sendResponseCallback(ErrorCode.NO_ERROR);
                    }
                }
            }
        }
    }

    public void handleCommitOffsets(String groupId, String memberId, int generationId, Map<String, Map<Integer, OffsetAndMetadata>> offsetMetadata,
                                    CommitCallback callback) {
        GroupMetadataManager.OffsetCache offsetCache = null;
        Map<String, Map<Integer, Short>> commitStatus = null;
        if (!isStarted()) {
            commitStatus = topicAndPartitionError(offsetMetadata, ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE);
        } else {
            GroupMetadata group = groupManager.getGroup(groupId);
            if (group == null) {
                if (generationId < 0) {
                    // the group is not relying on Kafka for partition management, so allow the commit
                    offsetCache = groupManager.prepareCacheOffset(groupId, memberId, generationId, offsetMetadata, callback);
                } else {
                    // the group has failed over to this coordinator (which will be handled in KAFKA-2017),
                    // or this is a request coming from an older generation. either way, reject the commit
                    commitStatus = topicAndPartitionError(offsetMetadata, ErrorCode.ILLEGAL_GENERATION);
                }
            } else {
                synchronized (group) {
                    if (group.is(GroupState.DEAD)) {
                        commitStatus = topicAndPartitionError(offsetMetadata, ErrorCode.UNKNOWN_MEMBER_ID);
                    } else if (group.is(GroupState.AWAITINGSYNC)) {
                        commitStatus = topicAndPartitionError(offsetMetadata, ErrorCode.REBALANCE_IN_PROGRESS);
                    } else if (!group.has(memberId)) {
                        commitStatus = topicAndPartitionError(offsetMetadata, ErrorCode.UNKNOWN_MEMBER_ID);
                    } else if (generationId != group.getGenerationId()) {
                        commitStatus = topicAndPartitionError(offsetMetadata, ErrorCode.ILLEGAL_GENERATION);
                    } else {
                        offsetCache = groupManager.prepareCacheOffset(groupId, memberId, generationId,
                                offsetMetadata, callback);
                    }
                }
            }
        }
        if (offsetCache == null) {
            callback.sendResponseCallback(commitStatus);
        } else {
            groupManager.cacheOffset(offsetCache);
        }
    }

    public Map<String, Map<Integer, OffsetMetadataAndError>> handleFetchOffsets(String groupId, HashMultimap<String, Integer> topicAndPartitions) {
        Map<String, Map<Integer, OffsetMetadataAndError>> offsetMetadataInfo = new HashMap<String, Map<Integer, OffsetMetadataAndError>>();
        if (topicAndPartitions == null || topicAndPartitions.isEmpty()) {
            return offsetMetadataInfo;
        }
        if (!isStarted()) {
            Set<String> topics = topicAndPartitions.keySet();
            for (String topic : topics) {
                Set<Integer> partitions = topicAndPartitions.get(topic);
                Map<Integer, OffsetMetadataAndError> partitionOffsetMetadataMap = new HashMap<Integer, OffsetMetadataAndError>();
                for (int partition : partitions) {
                    partitionOffsetMetadataMap.put(partition, OffsetMetadataAndError.GroupCoordinatorNotAvailable);
                }
                offsetMetadataInfo.put(topic, partitionOffsetMetadataMap);
            }
        } else {
            // return offsets blindly regardless the current group state since the group may be using
            // Kafka commit storage without automatic group management
            return groupManager.getOffsets(groupId, topicAndPartitions);
        }
        return offsetMetadataInfo;
    }

    public void handleQueryOffsets(Map<String, Map<Integer, OffsetRequest.PartitionOffsetRequestInfo>> offsetRequestInfo, QueryCallback callback) {
        GroupMetadataManager.OffsetQueryEvent offsetQueryEvent = new GroupMetadataManager.OffsetQueryEvent(offsetRequestInfo, callback);
        boolean addBool = groupManager.add(offsetQueryEvent);
        if (!addBool) {
            logger.error("offset query can't be added to queue");
        }
    }

    private Map<String, Map<Integer, Short>> topicAndPartitionError(Map<String, Map<Integer, OffsetAndMetadata>> offsetMetadata, short errorCode) {
        Map<String, Map<Integer, Short>> commitStatus = new HashMap<String, Map<Integer, Short>>();
        Set<String> topics = offsetMetadata.keySet();
        if (topics == null || topics.isEmpty()) {
            return commitStatus;
        }
        for (String topic : topics) {
            Map<Integer, Short> partitionsCode = new HashMap<Integer, Short>();
            Map<Integer, OffsetAndMetadata> partitionsOffsetInfo = offsetMetadata.get(topic);
            Set<Integer> partitions = partitionsOffsetInfo.keySet();
            for (Integer partition : partitions) {
                partitionsCode.put(partition, ErrorCode.ILLEGAL_GENERATION);
            }
            commitStatus.put(topic, partitionsCode);
        }
        return commitStatus;
    }

    private MemberMetadata addMemberAndRebalance(int sessionTimeoutMs, String clientId, String clientHost, Map<String, byte[]> protocols,
                                       GroupMetadata group, JoinCallback callback) {
        // use the client-id with a random id suffix as the member-id
        String memberId = clientId + "-" + group.generateMemberIdSuffix();
        MemberMetadata member = new MemberMetadata(memberId, group.getGroupId(), clientId, clientHost, sessionTimeoutMs, protocols);
        //logger.info("addMemberAndRebalance set callback : " + callback);
        member.setAwaitingJoinCallback(callback);
        group.add(member.getMemberId(), member);
        /*logger.info("members: " + group.allMembers());
        for (MemberMetadata metadata : group.allMemberMetadata()) {
            logger.info(metadata.getMemberId() + " | " + metadata.getAwaitingJoinCallback());
        }*/
        maybePrepareRebalance(group);
        return member;
    }

    private void updateMemberAndRebalance(GroupMetadata group, MemberMetadata member, Map<String, byte[]> protocols,
                                          JoinCallback callback) {
        member.setSupportedProtocols(protocols);
        //logger.info("updateMemberAndRebalance set callback : " + callback);
        member.setAwaitingJoinCallback(callback);
        maybePrepareRebalance(group);
    }

    private void maybePrepareRebalance(GroupMetadata group) {
        synchronized (group) {
            if (group.canRebalance()) {
                prepareRebalance(group);
            }
        }
    }

    private void prepareRebalance(GroupMetadata group) {
        //logger.info("prepareRebalance, group state : " + group.getState());
        // if any members are awaiting sync, cancel their request and have them rejoin
        if (group.is(GroupState.AWAITINGSYNC)) {
            resetAndPropagateAssignmentError(group, ErrorCode.REBALANCE_IN_PROGRESS);
        }

        group.transitionTo(GroupState.PREPARINGREBALANCE);
        //logger.info(String.format("Preparing to restabilize group %s with old generation %s", group.getGroupId(), group.getGenerationId()));
        int rebalanceTimeout = group.rebalanceTimeout();
        DelayedJoin delayedJoin = new DelayedJoin(this, group, rebalanceTimeout);
        DelayedOperationKey groupKey = new DelayedOperationKey(group.getGroupId());
        Set<Object> delayedOperationKeys = new HashSet<Object>();
        delayedOperationKeys.add(groupKey);
        joinPurgatory.tryCompleteElseWatch(delayedJoin, delayedOperationKeys);
    }

    private void setAndPropagateAssignment(GroupMetadata group, Map<String, byte[]> assignment) {
        Preconditions.checkState(group.is(GroupState.AWAITINGSYNC));
        Set<MemberMetadata> allMemberMetadata = group.allMemberMetadata();
        if (allMemberMetadata != null) {
            for (MemberMetadata member : allMemberMetadata) {
                member.setAssignment(assignment.get(member.getMemberId()));
            }
        }
        propagateAssignment(group, ErrorCode.NO_ERROR);
    }

    private void resetAndPropagateAssignmentError(GroupMetadata group, short errorCode) {
        Preconditions.checkState(group.is(GroupState.AWAITINGSYNC));
        Set<MemberMetadata> memberMetadatas = group.allMemberMetadata();
        if (memberMetadatas != null) {
            for (MemberMetadata member : memberMetadatas) {
                member.setAssignment(null);
            }
        }
        propagateAssignment(group, errorCode);
    }

    private void propagateAssignment(GroupMetadata group, short errorCode) {
        Set<MemberMetadata> memberMetadatas = group.allMemberMetadata();
        if (memberMetadatas != null) {
            for (MemberMetadata member : memberMetadatas) {
                if (member.getAwaitingSyncCallback() != null) {
                    member.getAwaitingSyncCallback().sendResponseCallback(member.getAssignment(), errorCode);
                    member.setAwaitingSyncCallback(null);
                    // reset the session timeout for members after propagating the member's assignment.
                    // This is because if any member's session expired while we were still awaiting either
                    // the leader sync group or the storage callback, its expiration will be ignored and no
                    // future heartbeat expectations will not be scheduled.
                    completeAndScheduleNextHeartbeatExpiration(group, member);
                }
            }
        }
    }

    private void onMemberFailure(GroupMetadata group, MemberMetadata member) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Member %s in group %s has failed", member.getMemberId(), group.getGroupId()));
        }
        group.remove(member.getMemberId());
        //logger.info("onMemberFailure removed " + member.getMemberId());
        switch (group.currentState()) {
            case DEAD:
                break;
            case STABLE:
            case AWAITINGSYNC:
                maybePrepareRebalance(group);
                break;
            case PREPARINGREBALANCE:
                DelayedOperationKey groupKey = new DelayedOperationKey(group.getGroupId());
                joinPurgatory.checkAndComplete(groupKey);
                break;
            default:
                break;
        }
    }

    protected boolean tryCompleteJoin(GroupMetadata group, DelayedOperation delayedOperation) {
        synchronized (group) {
            //logger.info("called notYetRejoinedMembers() at tryCompleteJoin");
            if (group.notYetRejoinedMembers().isEmpty()) {
                //logger.info("forceComplete in tryCompleteJoin");
                return delayedOperation.forceComplete();
            } else {
                return false;
            }
        }
    }

    protected void onExpireJoin() {

    }
    protected void onCompleteJoin(GroupMetadata group) {
        synchronized (group) {
            Set<MemberMetadata> failedMembers = group.notYetRejoinedMembers();
            if (group.isEmpty() || !failedMembers.isEmpty()) {
                for (MemberMetadata memberMetadata : failedMembers) {
                    //logger.info("onCompleteJoin to remove member");
                    group.remove(memberMetadata.getMemberId());
                }

                if (group.isEmpty()) {
                    group.transitionTo(GroupState.DEAD);
                    groupManager.removeGroup(group);
                    logger.info(String.format("Group %s generation %s is dead and removed", group.getGroupId(), group.getGenerationId()));
                }
            }

            if (!group.is(GroupState.DEAD)) {
                group.initNextGeneration();
                logger.info(String.format("Stabilized group %s generation %s", group.getGroupId(), group.getGenerationId()));

                for (MemberMetadata memberMetadata : group.allMemberMetadata()) {
                    //logger.info("onCompleteJoin : " + memberMetadata.getMemberId() + " | " + memberMetadata.getAwaitingJoinCallback());
                    Preconditions.checkState(memberMetadata.getAwaitingJoinCallback() != null);
                    Map<String, byte[]> members = null;
                    if (memberMetadata.getMemberId().equals(group.getLeaderId())) {
                        members = group.currentMemberMetadata();
                    } else {
                        members = new HashMap<String, byte[]>();
                    }
                    JoinGroupResult joinGroupResult = new JoinGroupResult(members, memberMetadata.getMemberId(), group.getGenerationId(), group.getProtocol(),
                                                        group.getLeaderId(), ErrorCode.NO_ERROR);
                    //logger.info("calling callback in onCompleteJoin");
                    memberMetadata.getAwaitingJoinCallback().sendResponseCallback(joinGroupResult);
                    memberMetadata.setAwaitingJoinCallback(null);
                    completeAndScheduleNextHeartbeatExpiration(group, memberMetadata);
                }
            }
        }
    }

    protected boolean tryCompleteHeartbeat(GroupMetadata group, MemberMetadata member, long heartbeatDeadline, DelayedOperation delayedOperation) {
        synchronized(group) {
            if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving()) {
                //logger.info("forceComplete in tryCompleteHeartbeat");
                return delayedOperation.forceComplete();
            } else {
                return false;
            }
        }
    }

    protected void onExpireHeartbeat(GroupMetadata group, MemberMetadata member, long heartbeatDeadline) {
        //logger.info("onExpireHeartbeat");
        synchronized(group) {
            if (!shouldKeepMemberAlive(member, heartbeatDeadline)) {
                onMemberFailure(group, member);
            }
        }
    }

    protected void onCompleteHeartbeat() {
        if (logger.isDebugEnabled()) {
            logger.debug("******************complete heartbeat******************");
        }
    }

    private boolean shouldKeepMemberAlive(MemberMetadata member, long heartbeatDeadline) {
        return member.getAwaitingJoinCallback() != null ||
                member.getAwaitingSyncCallback() != null ||
                member.getLatestHeartbeat() + member.getSessionTimeoutMs() > heartbeatDeadline;
    }

    /**
     * Complete existing DelayedHeartbeats for the given member and schedule the next one
     */
    private void completeAndScheduleNextHeartbeatExpiration(GroupMetadata group, MemberMetadata member) {
        // complete current heartbeat expectation
        member.setLatestHeartbeat(SystemClock.now());
        DelayedOperationKey memberKey = new DelayedOperationKey(member.getGroupId(), member.getMemberId());
        heartbeatPurgatory.checkAndComplete(memberKey);

        // reschedule the next heartbeat expiration deadline
        long newHeartbeatDeadline = member.getLatestHeartbeat() + member.getSessionTimeoutMs();
        DelayedHeartbeat delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.getSessionTimeoutMs());
        Set<Object> delayedOperationKeys = new HashSet<Object>();
        delayedOperationKeys.add(memberKey);
        heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, delayedOperationKeys);
    }

    private void removeHeartbeatForLeavingMember(GroupMetadata group, MemberMetadata member) {
        member.setLeaving(true);
        DelayedOperationKey memberKey = new DelayedOperationKey(member.getGroupId(), member.getMemberId());
        heartbeatPurgatory.checkAndComplete(memberKey);
    }

    private boolean validGroupId(String groupId) {
        return groupId != null && !groupId.isEmpty();
    }

    private JoinGroupResult joinError(String memberId, short errorCode) {
        JoinGroupResult joinGroupResult = new JoinGroupResult(new HashMap<String, byte[]>(), memberId, 0,
                                            GroupCoordinator.NoProtocol, GroupCoordinator.NoLeader, errorCode);
        return joinGroupResult;
    }

    public static class JoinGroupResult {

        private Map<String, byte[]> members;
        private String memberId;
        private int generationId;
        private String subProtocol;
        private String leaderId;
        private short errorCode;

        public JoinGroupResult(Map<String, byte[]> members, String memberId, int generationId,
                               String subProtocol, String leaderId, short errorCode) {
            this.members = members;
            this.memberId = memberId;
            this.generationId = generationId;
            this.subProtocol = subProtocol;
            this.leaderId = leaderId;
            this.errorCode = errorCode;
        }

        public Map<String, byte[]> getMembers() {
            return members;
        }

        public String getMemberId() {
            return memberId;
        }

        public int getGenerationId() {
            return generationId;
        }

        public String getSubProtocol() {
            return subProtocol;
        }

        public String getLeaderId() {
            return leaderId;
        }

        public short getErrorCode() {
            return errorCode;
        }
    }

    protected class KafkaClusterListener implements EventListener<KafkaClusterEvent> {

        @Override
        public void onEvent(KafkaClusterEvent kafkaClusterEvent) {
            if (kafkaClusterEvent instanceof TopicsBrokerEvent) {
                TopicsBrokerEvent topicsBrokerEvent = (TopicsBrokerEvent)kafkaClusterEvent;
                String topic = topicsBrokerEvent.getTopic();
                List<String> groupIds = clusterManager.getConsumerApp(topic);
                if (groupIds != null) {
                    for (String groupId : groupIds) {
                        GroupMetadata group = groupManager.getGroup(groupId);
                        if (group == null) {
                            continue;
                        }
                        Set<MemberMetadata> memberMetadatas = group.allMemberMetadata();
                        if (memberMetadatas == null || memberMetadatas.isEmpty()) {
                            continue;
                        }
                        for (MemberMetadata member : memberMetadatas) {
                            onMemberFailure(group, member);
                        }
                    }
                }
            }
        }
    }

    public static interface JoinCallback {
        void sendResponseCallback(JoinGroupResult joinGroupResult);
    }

    public static interface SyncCallback {
        void sendResponseCallback(byte[] assignment, short errorCode);
    }

    public static interface HeartbeatCallback {
        void sendResponseCallback(short errorCode);
    }

    public static interface LeaveCallback {
        void sendResponseCallback(short errorCode);
    }

    public static interface CommitCallback {
        void sendResponseCallback(Map<String, Map<Integer, Short>> commitStatus);
    }

    public static interface QueryCallback {
        void sendResponseCallback(Map<String, Map<Integer, PartitionOffsetsResponse>> topicPartitionOffsets);
    }
}
