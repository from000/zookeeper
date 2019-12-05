/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;

import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 */
public class SyncedLearnerTracker {

    protected ArrayList<QuorumVerifierAcksetPair> qvAcksetPairs = 
                new ArrayList<QuorumVerifierAcksetPair>();

    public void addQuorumVerifier(QuorumVerifier qv) {
        qvAcksetPairs.add(new QuorumVerifierAcksetPair(qv,
                new HashSet<Long>(qv.getVotingMembers().size())));
    }

    public boolean addAck(Long sid) {
        boolean change = false;
        for (QuorumVerifierAcksetPair qvAckset : qvAcksetPairs) {
            if (qvAckset.getQuorumVerifier().getVotingMembers().containsKey(sid)) {
                qvAckset.getAckset().add(sid);
                change = true;
            }
        }
        return change;
    }

    /**
     * 检验是否通过本轮选举或事务投票，这里需要注意的是，只要SyncedLearnerTracker缓存的投票验证器QuorumVerifier中有一个没有通过投票，那么会认为本轮投票失败
     * @return
     */
    public boolean hasAllQuorums() {
        for (QuorumVerifierAcksetPair qvAckset : qvAcksetPairs) {
            if (!qvAckset.getQuorumVerifier().containsQuorum(qvAckset.getAckset()))
                return false;
        }
        return true;
    }
        
    public String ackSetsToString(){
        StringBuilder sb = new StringBuilder();
            
        for (QuorumVerifierAcksetPair qvAckset : qvAcksetPairs) {
            sb.append(qvAckset.getAckset().toString()).append(",");
        }
            
        return sb.substring(0, sb.length()-1);
    }

    /**
     * peer被确认对象
     */
    public static class QuorumVerifierAcksetPair {
        private final QuorumVerifier qv; // peer校验
        private final HashSet<Long> ackset; // 确认serverId列表

        public QuorumVerifierAcksetPair(QuorumVerifier qv, HashSet<Long> ackset) {                
            this.qv = qv;
            this.ackset = ackset;
        }

        public QuorumVerifier getQuorumVerifier() {
            return this.qv;
        }

        public HashSet<Long> getAckset() {
            return this.ackset;
        }
    }
}
