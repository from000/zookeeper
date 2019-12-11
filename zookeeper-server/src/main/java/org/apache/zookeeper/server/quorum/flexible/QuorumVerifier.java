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

package org.apache.zookeeper.server.quorum.flexible;

import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

import java.util.Map;
import java.util.Set;

/**
 * All quorum validators have to implement a method called
 * containsQuorum, which verifies if a HashSet of server 
 * identifiers constitutes a quorum.
 *
 * 集群验证器，维护zk集群中各成员列表和权重等关系
 *
 */

public interface QuorumVerifier {
    /**
     * 服务权重
     * @param id serverId
     * @return
     */
    long getWeight(long id);

    /**
     * 是否通过投票
     * @param set
     * @return
     */
    boolean containsQuorum(Set<Long> set);
    long getVersion();
    void setVersion(long ver);

    /**
     * zk集群的所有服务成员
     * @return
     */
    Map<Long, QuorumServer> getAllMembers();

    /**
     * 投票者成员集合，key:serverId。投票者不一定都是follower,如果选举成功，就会变成leader
     * @return
     */
    Map<Long, QuorumServer> getVotingMembers();

    /**
     * observer成员集合，key:serverId
     * @return
     */
    Map<Long, QuorumServer> getObservingMembers();
    boolean equals(Object o);
    String toString();
}
