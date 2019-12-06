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

package org.apache.zookeeper.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a class that implements prefix matching for 
 * components of a filesystem path. the trie
 * looks like a tree with edges mapping to 
 * the component of a path.
 * example /ab/bc/cf would map to a trie
 *           /
 *        ab/
 *        (ab)
 *      bc/
 *       / 
 *      (bc)
 *   cf/
 *   (cf)
 *
 *
 *
 *   字典树完成配额目录的增删查, 在zk中的目录结构为/zookeeper/quota/xxx(可以有多级目录)/zookeeper_limits
 *   字典树参考： https://www.cnblogs.com/TheRoadToTheGold/p/6290732.html
 *
 *
 *   @see org.apache.zookeeper.Quotas
 */    
public class PathTrie {
    /**
     * the logger for this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(PathTrie.class);
    
    /**
     * the root node of PathTrie
     */
    private final TrieNode rootNode ;
    
    static class TrieNode {
        boolean property = false;//表示当前节点是否有配额
        final HashMap<String, TrieNode> children;
        TrieNode parent = null;
        /**
         * create a trienode with parent
         * as parameter
         * @param parent the parent of this trienode
         */
        private TrieNode(TrieNode parent) {
            children = new HashMap<String, TrieNode>();
            this.parent = parent;
        }
        
        /**
         * get the parent of this node
         * @return the parent node
         */
        TrieNode getParent() {
            return this.parent;
        }
        
        /**
         * set the parent of this node
         * @param parent the parent to set to
         */
        void setParent(TrieNode parent) {
            this.parent = parent;
        }
        
        /**
         * a property that is set 
         * for a node - making it 
         * special.
         */
        void setProperty(boolean prop) {
            this.property = prop;
        }
        
        /** the property of this
         * node 
         * @return the property for this
         * node
         */
        boolean getProperty() {
            return this.property;
        }
        /**
         * add a child to the existing node
         * @param childName the string name of the child
         * @param node the node that is the child
         */
        void addChild(String childName, TrieNode node) {
            synchronized(children) {
                if (children.containsKey(childName)) {
                    return;
                }
                children.put(childName, node);
            }
        }
     
        /**
         * delete child from this node
         * @param childName the string name of the child to 
         * be deleted
         */
        void deleteChild(String childName) {
            synchronized(children) {
                if (!children.containsKey(childName)) {
                    return;
                }
                TrieNode childNode = children.get(childName);
                // this is the only child node.
                if (childNode.getChildren().length == 1) { 
                    childNode.setParent(null);
                    children.remove(childName);
                }
                else {
                    // their are more child nodes
                    // so just reset property.
                    childNode.setProperty(false);
                }
            }
        }
        
        /**
         * return the child of a node mapping
         * to the input childname
         * @param childName the name of the child
         * @return the child of a node
         */
        TrieNode getChild(String childName) {
            synchronized(children) {
               if (!children.containsKey(childName)) {
                   return null;
               }
               else {
                   return children.get(childName);
               }
            }
        }

        /**
         * get the list of children of this 
         * trienode.
         * @param node to get its children
         * @return the string list of its children
         */
        String[] getChildren() {
           synchronized(children) {
               return children.keySet().toArray(new String[0]);
           }
        }
        
        /**
         * get the string representation
         * for this node
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Children of trienode: ");
            synchronized(children) {
                for (String str: children.keySet()) {
                    sb.append(" " + str);
                }
            }
            return sb.toString();
        }
    }
    
    /**
     * construct a new PathTrie with
     * a root node of /
     */
    public PathTrie() {
        this.rootNode = new TrieNode(null);
    }
    
    /**
     * add a path to the path trie 
     * @param path
     */
    public void addPath(String path) {
        if (path == null) {
            return;
        }
        String[] pathComponents = path.split("/");
        TrieNode parent = rootNode;
        String part = null;
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }

        for (int i=1; i<pathComponents.length; i++) {
            // 如果path是 aaa/bbb/ccc，相当于依次处理aaa, bbb, ccc
            part = pathComponents[i];
            if (parent.getChild(part) == null) {
                // 添加子路径
                parent.addChild(part, new TrieNode(parent));
            }
            parent = parent.getChild(part);
        }
        // 表示当前节点的父节点(aaa/bbb)，已经设置了配额
        parent.setProperty(true);
    }
    
    /**
     * delete a path from the trie
     * 删除字典树路径
     *
     * @param path the path to be deleted
     */
    public void deletePath(String path) {
        if (path == null) {
            return;
        }
        String[] pathComponents = path.split("/");
        TrieNode parent = rootNode;
        String part = null;
        if (pathComponents.length <= 1) { 
            throw new IllegalArgumentException("Invalid path " + path);
        }
        for (int i=1; i<pathComponents.length; i++) {
            part = pathComponents[i];
            // 没有节点存在，直接返回
            if (parent.getChild(part) == null) {
                //the path does not exist 
                return;
            }
            parent = parent.getChild(part);
            LOG.info("{}",parent);
        }

        // 递归删除某条字典树路径
        TrieNode realParent  = parent.getParent();
        realParent.deleteChild(part);
    }
    
    /**
     * return the largest prefix for the input path.
     *
     * 查找路径的最大前缀
     *
     * @param path the input path
     * @return the largest prefix for the input path.
     */
    public String findMaxPrefix(String path) {
        if (path == null) {
            return null;
        }
        if ("/".equals(path)) {
            return path;
        }
        String[] pathComponents = path.split("/");

        // 从根目录查找
        TrieNode parent = rootNode;
        List<String> components = new ArrayList<String>();
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        int i = 1;
        String part = null;
        StringBuilder sb = new StringBuilder();
        int lastindex = -1;
        while((i < pathComponents.length)) {
            // 如果存在子路径，即路径存在重合
            if (parent.getChild(pathComponents[i]) != null) {
                part = pathComponents[i];
                parent = parent.getChild(part);
                components.add(part);
                if (parent.getProperty()) {
                    lastindex = i-1;
                }
            }
            else {
                break;
            }
            i++;
        }
        for (int j=0; j< (lastindex+1); j++) {
            sb.append("/" + components.get(j));
        }
        return sb.toString();
    }

    /**
     * clear all nodes
     */
    public void clear() {
        for(String child : rootNode.getChildren()) {
            rootNode.deleteChild(child);
        }
    }
}
