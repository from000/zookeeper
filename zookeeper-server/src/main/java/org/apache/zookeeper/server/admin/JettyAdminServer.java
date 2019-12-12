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

package org.apache.zookeeper.server.admin;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

/**
 * This class encapsulates a Jetty server for running Commands.
 *
 * Given the default settings, start a ZooKeeper server and visit
 * http://<hostname>:8080/commands for links to all registered commands. Visiting
 * http://<hostname>:8080/commands/<commandname> will execute the associated
 * Command and return the result in the body of the response. Any keyword
 * arguments to the command are specified with URL parameters (e.g.,
 * http://localhost:8080/commands/set_trace_mask?traceMask=306).
 *
 * @see Commands
 * @see CommandOutputter
 */
public class JettyAdminServer implements AdminServer {
    static final Logger LOG = LoggerFactory.getLogger(JettyAdminServer.class);

    public static final int DEFAULT_PORT = 8080;
    public static final int DEFAULT_IDLE_TIMEOUT = 30000;
    public static final String DEFAULT_COMMAND_URL = "/commands";
    private static final String DEFAULT_ADDRESS = "0.0.0.0";

    private final Server server; // jetty server对象
    private final String address; // jetty server监听的地址
    private final int port; // 服务端口
    private final int idleTimeout; // 超时时间
    private final String commandUrl; // 命令的通用地址
    private ZooKeeperServer zkServer;

    public JettyAdminServer() throws AdminServerException {
        this(System.getProperty("zookeeper.admin.serverAddress", DEFAULT_ADDRESS),
             Integer.getInteger("zookeeper.admin.serverPort", DEFAULT_PORT),
             Integer.getInteger("zookeeper.admin.idleTimeout", DEFAULT_IDLE_TIMEOUT),
             System.getProperty("zookeeper.admin.commandURL", DEFAULT_COMMAND_URL));
    }

    /**
     * jetty 服务配置
     * @param address
     * @param port
     * @param timeout
     * @param commandUrl
     */
    public JettyAdminServer(String address, int port, int timeout, String commandUrl) {
        this.port = port;
        this.idleTimeout = timeout;
        this.commandUrl = commandUrl;
        this.address = address;

        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setHost(address);
        connector.setPort(port);
        connector.setIdleTimeout(idleTimeout);
        server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/*");
        server.setHandler(context);

        // jetty admin的处理逻辑是通过CommandServlet实现的，请求地址(/commands/*)
        context.addServlet(new ServletHolder(new CommandServlet()), commandUrl + "/*");
    }

    /**
     * Start the embedded Jetty server.
     *
     * 启动jetty服务，用于直接访问zk admin服务
     */
    @Override
    public void start() throws AdminServerException {
        try {
            server.start();
        } catch (Exception e) {
            // Server.start() only throws Exception, so let's at least wrap it
            // in an identifiable subclass
            throw new AdminServerException(String.format(
                    "Problem starting AdminServer on address %s,"
                            + " port %d and command URL %s", address, port,
                    commandUrl), e);
        }
        LOG.info(String.format("Started AdminServer on address %s, port %d"
                + " and command URL %s", address, port, commandUrl));
    }

    /**
     * Stop the embedded Jetty server.
     *
     * This is not very important except for tests where multiple
     * JettyAdminServers are started and may try to bind to the same ports if
     * previous servers aren't shut down.
     */
    @Override
    public void shutdown() throws AdminServerException {
        try {
            server.stop();
        } catch (Exception e) {
            throw new AdminServerException(String.format(
                    "Problem stopping AdminServer on address %s,"
                            + " port %d and command URL %s", address, port, commandUrl),
                    e);
        }
    }

    /**
     * Set the ZooKeeperServer that will be used to run Commands.
     *
     * It is not necessary to set the ZK server before calling
     * AdminServer.start(), and the ZK server can be set to null when, e.g.,
     * that server is being shut down. If the ZK server is not set or set to
     * null, the AdminServer will still be able to issue Commands, but they will
     * return an error until a ZK server is set.
     */
    @Override
    public void setZooKeeperServer(ZooKeeperServer zkServer) {
        this.zkServer = zkServer;
    }

    /**
     * 处理jetty admin的请求
     */
    private class CommandServlet extends HttpServlet {
        private static final long serialVersionUID = 1L;

        /**
         * 处理get请求
         * @param request
         * @param response
         * @throws ServletException
         * @throws IOException
         */
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            // Capture the command name from the URL

            // 请求命令就是通过指定路径实现的，比如commands/config，cmd=config
            String cmd = request.getPathInfo();
            // 获取所有的请求路径
            if (cmd == null || cmd.equals("/")) {
                // No command specified, print links to all commands instead
                for (String link : commandLinks()) {
                    response.getWriter().println(link);
                    response.getWriter().println("<br/>");
                }
                return;
            }
            // Strip leading "/"
            cmd = cmd.substring(1);

            // Extract keyword arguments to command from request parameters

            // 请求参数对象
            @SuppressWarnings("unchecked")
            Map<String, String[]> parameterMap = request.getParameterMap();
            Map<String, String> kwargs = new HashMap<String, String>();
            for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
                kwargs.put(entry.getKey(), entry.getValue()[0]);
            }

            // 请求参数作为运行命令的参数
            // Run the command
            CommandResponse cmdResponse = Commands.runCommand(cmd, zkServer, kwargs);

            // Format and print the output of the command

            // json格式输出
            CommandOutputter outputter = new JsonOutputter();
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(outputter.getContentType());

            // 响应指定格式的输出结果
            outputter.output(cmdResponse, response.getWriter());
        }
    }

    /**
     * Returns a list of URLs to each registered Command.
     * 返回所有的命令请求路径集合
     *
     */
    private List<String> commandLinks() {
        List<String> links = new ArrayList<String>();
        List<String> commands = new ArrayList<String>(Commands.getPrimaryNames());
        Collections.sort(commands);
        for (String command : commands) {
            String url = commandUrl + "/" + command;
            links.add(String.format("<a href=\"%s\">%s</a>", url, command));
        }
        return links;
    }
}
