/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.yarn.appMaster.http;

import org.apache.drill.yarn.appMaster.Dispatcher;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.servlet.ServletContainer;

import com.typesafe.config.Config;

/**
 * Wrapper around the Jetty web server.
 * <p>
 * Adapted from Drill's drill.exec.WebServer class. Would be good to create
 * a common base class later, but the goal for the initial project is to
 * avoid Drill code changes.
 */

public class WebServer implements AutoCloseable
{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WebServer.class);
  private final Server embeddedJetty;
  private Dispatcher dispatcher;

  public WebServer( Dispatcher dispatcher ) {
    this.dispatcher = dispatcher;
    Config config = DrillOnYarnConfig.config();
    if (config.getBoolean(DrillOnYarnConfig.HTTP_ENABLED)) {
      embeddedJetty = new Server();
    } else {
      embeddedJetty = null;
    }
  }

  /**
   * Start the web server including setup.
   * @throws Exception
   */
  public void start() throws Exception {
    if (embeddedJetty == null) {
      return;
    }

    Config config = DrillOnYarnConfig.config();
    final ServerConnector serverConnector;
    // No support for SSL yet.
//    if (config.getBoolean(ExecConstants.HTTP_ENABLE_SSL)) {
//      serverConnector = createHttpsConnector();
//    } else {
      serverConnector = createHttpConnector( config );
//    }
    embeddedJetty.addConnector(serverConnector);

    // Add resources
    final ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setShowMessageInTitle(true);

    final ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
    servletContextHandler.setErrorHandler(errorHandler);
    servletContextHandler.setContextPath("/");
    embeddedJetty.setHandler(servletContextHandler);

    final ServletHolder servletHolder = new ServletHolder(new ServletContainer(new PageTree( dispatcher )));
    servletHolder.setInitOrder(1);
    servletContextHandler.addServlet(servletHolder, "/*");

//    embeddedJetty.setHandler( new WebTest.HelloServlet( ) );
//    ServletHandler handler = new ServletHandler();
//    embeddedJetty.setHandler(handler);
//    handler.addServletWithMapping(WebTest.HelloServlet.class, "/*");
//    servletContextHandler.addServlet(
//        new ServletHolder(new MetricsServlet(metrics)), "/status/metrics");
//    servletContextHandler.addServlet(new ServletHolder(new ThreadDumpServlet()), "/status/threads");
//
//    if (config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED)) {
//      servletContextHandler.setSecurityHandler(createSecurityHandler());
//      servletContextHandler.setSessionHandler(createSessionHandler(servletContextHandler.getSecurityHandler()));
//    }

    // Access to static resources (JS pages, images, etc.)
    // The static resources themselves come from Drill exec sub-project.

    final ServletHolder staticHolder = new ServletHolder("static", DefaultServlet.class);
    staticHolder.setInitParameter("resourceBase", Resource.newClassPathResource("/rest/static").toString());
    staticHolder.setInitParameter("dirAllowed", "false");
    staticHolder.setInitParameter("pathInfoOnly", "true");
    servletContextHandler.addServlet(staticHolder, "/static/*");

    final ServletHolder amStaticHolder = new ServletHolder("am-static", DefaultServlet.class);
    amStaticHolder.setInitParameter("resourceBase", Resource.newClassPathResource("/drill-am/static").toString());
    amStaticHolder.setInitParameter("dirAllowed", "false");
    amStaticHolder.setInitParameter("pathInfoOnly", "true");
    servletContextHandler.addServlet(amStaticHolder, "/drill-am/static/*");

    embeddedJetty.start();
  }

  /**
   * Create HTTP connector.
   * @return Initialized {@link ServerConnector} instance for HTTP connections.
   * @throws Exception
   */
  private ServerConnector createHttpConnector( Config config ) throws Exception {
    logger.info("Setting up HTTP connector for web server");
    final HttpConfiguration httpConfig = new HttpConfiguration();
    final ServerConnector httpConnector = new ServerConnector(embeddedJetty, new HttpConnectionFactory(httpConfig));
    httpConnector.setPort(config.getInt(DrillOnYarnConfig.HTTP_PORT));

    return httpConnector;
  }

  @Override
  public void close() throws Exception {
    if (embeddedJetty != null) {
      embeddedJetty.stop();
    }
  }
}
