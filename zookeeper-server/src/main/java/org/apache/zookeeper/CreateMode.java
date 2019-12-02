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
package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 *  CreateMode value determines how the znode is created on ZooKeeper.
 *
 *  zookeeper的节点类型：
 *      PERSISTENT： 永久节点，不会随客户端会话失效而消失
 *      PERSISTENT_SEQUENTIAL： 有序的永久节点
 *      EPHEMERAL： 临时节点
 *      EPHEMERAL_SEQUENTIAL： 临时有序节点
 *      CONTAINER： 如果节点中最后一个子Znode被删除，将会触发删除该Znode
 *      PERSISTENT_WITH_TTL： 持久顺序定时节点，客户端断开连接后不会自动删除Znode，如果该Znode没有子Znode且在给定TTL时间内无修改，该Znode将会被删除；TTL单位是毫秒，必须大于0且小于或等于 EphemeralType.MAX_TTL
 *      PERSISTENT_SEQUENTIAL_WITH_TTL： 持久顺序定时节点
 */
@InterfaceAudience.Public
public enum CreateMode {
    
    /**
     * The znode will not be automatically deleted upon client's disconnect.
     */
    PERSISTENT (0, false, false, false, false),
    /**
    * The znode will not be automatically deleted upon client's disconnect,
    * and its name will be appended with a monotonically increasing number.
    */
    PERSISTENT_SEQUENTIAL (2, false, true, false, false),
    /**
     * The znode will be deleted upon the client's disconnect.
     */
    EPHEMERAL (1, true, false, false, false),
    /**
     * The znode will be deleted upon the client's disconnect, and its name
     * will be appended with a monotonically increasing number.
     */
    EPHEMERAL_SEQUENTIAL (3, true, true, false, false),
    /**
     * The znode will be a container node. Container
     * nodes are special purpose nodes useful for recipes such as leader, lock,
     * etc. When the last child of a container is deleted, the container becomes
     * a candidate to be deleted by the server at some point in the future.
     * Given this property, you should be prepared to get
     * {@link org.apache.zookeeper.KeeperException.NoNodeException}
     * when creating children inside of this container node.
     */
    CONTAINER (4, false, false, true, false),
    /**
     * The znode will not be automatically deleted upon client's disconnect.
     * However if the znode has not been modified within the given TTL, it
     * will be deleted once it has no children.
     */
    PERSISTENT_WITH_TTL(5, false, false, false, true),
    /**
     * The znode will not be automatically deleted upon client's disconnect,
     * and its name will be appended with a monotonically increasing number.
     * However if the znode has not been modified within the given TTL, it
     * will be deleted once it has no children.
     */
    PERSISTENT_SEQUENTIAL_WITH_TTL(6, false, true, false, true);

    private static final Logger LOG = LoggerFactory.getLogger(CreateMode.class);

    private boolean ephemeral;
    private boolean sequential;
    private final boolean isContainer;
    private int flag;
    private boolean isTTL;

    CreateMode(int flag, boolean ephemeral, boolean sequential,
               boolean isContainer, boolean isTTL) {
        this.flag = flag;
        this.ephemeral = ephemeral;
        this.sequential = sequential;
        this.isContainer = isContainer;
        this.isTTL = isTTL;
    }

    public boolean isEphemeral() { 
        return ephemeral;
    }

    public boolean isSequential() { 
        return sequential;
    }

    public boolean isContainer() {
        return isContainer;
    }

    public boolean isTTL() {
        return isTTL;
    }

    public int toFlag() {
        return flag;
    }

    /**
     * Map an integer value to a CreateMode value
     */
    static public CreateMode fromFlag(int flag) throws KeeperException {
        switch(flag) {
        case 0: return CreateMode.PERSISTENT;

        case 1: return CreateMode.EPHEMERAL;

        case 2: return CreateMode.PERSISTENT_SEQUENTIAL;

        case 3: return CreateMode.EPHEMERAL_SEQUENTIAL ;

        case 4: return CreateMode.CONTAINER;

        case 5: return CreateMode.PERSISTENT_WITH_TTL;

        case 6: return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

        default:
            String errMsg = "Received an invalid flag value: " + flag
                    + " to convert to a CreateMode";
            LOG.error(errMsg);
            throw new KeeperException.BadArgumentsException(errMsg);
        }
    }

    /**
     * Map an integer value to a CreateMode value
     */
    static public CreateMode fromFlag(int flag, CreateMode defaultMode) {
        switch(flag) {
            case 0:
                return CreateMode.PERSISTENT;

            case 1:
                return CreateMode.EPHEMERAL;

            case 2:
                return CreateMode.PERSISTENT_SEQUENTIAL;

            case 3:
                return CreateMode.EPHEMERAL_SEQUENTIAL;

            case 4:
                return CreateMode.CONTAINER;

            case 5:
                return CreateMode.PERSISTENT_WITH_TTL;

            case 6:
                return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

            default:
                return defaultMode;
        }
    }
}
