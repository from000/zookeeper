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

package org.apache.zookeeper.server.util;
/*
ZXID是一个长度64位的数字，其中低32位是按照数字递增，即每次客户端发起一个proposal，低32位的数字简单加1。
                            高32位是epoch用来标识Leader关系是否改变，每次一个Leader被选出来(epoch加1)，它都会有一个新的epoch。
 */
public class ZxidUtils {
    /**
     * 根据zxid获取选举次数Epoch
     * @param zxid
     * @return
     */
	static public long getEpochFromZxid(long zxid) {
		return zxid >> 32L;
	}
	static public long getCounterFromZxid(long zxid) {
		return zxid & 0xffffffffL;
	}
	static public long makeZxid(long epoch, long counter) {
		return (epoch << 32L) | (counter & 0xffffffffL);
	}
	static public String zxidToString(long zxid) {
		return Long.toHexString(zxid);
	}
}
