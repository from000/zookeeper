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

package org.apache.zookeeper.server.quorum.auth;

import java.io.IOException;
import java.net.Socket;

/**
 * Interface for quorum learner authentication mechanisms.
 *
 * 通过客户端和服务端的角度区分org.apache.zookeeper.server.quorum.auth.QuorumAuthServer
 * @see QuorumAuthServer
 */
public interface QuorumAuthLearner {

    /**
     * Performs an authentication step for the given socket connection.
     *
     *
     * 作为客户端，认证socket对应的服务端通讯
     *
     * @param sock
     *            socket connection to other quorum peer server
     * @param hostname
     *            host name of other quorum peer server
     * @throws IOException
     *             if there is an authentication failure
     */
    public void authenticate(Socket sock, String hostname) throws IOException;
}
