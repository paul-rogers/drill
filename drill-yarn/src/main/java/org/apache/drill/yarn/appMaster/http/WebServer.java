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

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.yarn.appMaster.Dispatcher;
import org.apache.drill.yarn.appMaster.DrillApplicationMaster;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.servlet.ServletContainer;
import org.joda.time.DateTime;

import com.google.common.base.Strings;
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
  private static final Log LOG = LogFactory.getLog(WebServer.class);
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
    if (config.getBoolean(DrillOnYarnConfig.HTTP_ENABLE_SSL)) {
      serverConnector = createHttpsConnector( config );
    } else {
      serverConnector = createHttpConnector( config );
    }
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
    LOG.info("Setting up HTTP connector for web server");
    final HttpConfiguration httpConfig = new HttpConfiguration();
    final ServerConnector httpConnector = new ServerConnector(embeddedJetty, new HttpConnectionFactory(httpConfig));
    httpConnector.setPort(config.getInt(DrillOnYarnConfig.HTTP_PORT));

    return httpConnector;
  }

  /**
   * Create an HTTPS connector for given jetty server instance. If the admin has specified keystore/truststore settings
   * they will be used else a self-signed certificate is generated and used.
   * <p>
   * This is a shameless copy of {@link org.apache.drill.exec.server.rest.Webserver#createHttpsConnector( )}.
   * The two should be merged at some point. The primary issue is that the Drill version is tightly coupled
   * to Drillbit configuration.
   *
   * @return Initialized {@link ServerConnector} for HTTPS connections.
   * @throws Exception
   */

  private ServerConnector createHttpsConnector( Config config ) throws Exception {
    LOG.info("Setting up HTTPS connector for web server");

    final SslContextFactory sslContextFactory = new SslContextFactory();

//    if (config.hasPath(ExecConstants.HTTP_KEYSTORE_PATH) &&
//        !Strings.isNullOrEmpty(config.getString(ExecConstants.HTTP_KEYSTORE_PATH))) {
//      LOG.info("Using configured SSL settings for web server");
//      sslContextFactory.setKeyStorePath(config.getString(ExecConstants.HTTP_KEYSTORE_PATH));
//      sslContextFactory.setKeyStorePassword(config.getString(ExecConstants.HTTP_KEYSTORE_PASSWORD));
//
//      // TrustStore and TrustStore password are optional
//      if (config.hasPath(ExecConstants.HTTP_TRUSTSTORE_PATH)) {
//        sslContextFactory.setTrustStorePath(config.getString(ExecConstants.HTTP_TRUSTSTORE_PATH));
//        if (config.hasPath(ExecConstants.HTTP_TRUSTSTORE_PASSWORD)) {
//          sslContextFactory.setTrustStorePassword(config.getString(ExecConstants.HTTP_TRUSTSTORE_PASSWORD));
//        }
//      }
//    } else {
      LOG.info("Using generated self-signed SSL settings for web server");
      final SecureRandom random = new SecureRandom();

      // Generate a private-public key pair
      final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(1024, random);
      final KeyPair keyPair = keyPairGenerator.generateKeyPair();

      final DateTime now = DateTime.now();

      // Create builder for certificate attributes
      final X500NameBuilder nameBuilder =
          new X500NameBuilder(BCStyle.INSTANCE)
              .addRDN(BCStyle.OU, "Apache Drill (auth-generated)")
              .addRDN(BCStyle.O, "Apache Software Foundation (auto-generated)")
              .addRDN(BCStyle.CN, "Drill AM");

      final Date notBefore = now.minusMinutes(1).toDate();
      final Date notAfter = now.plusYears(5).toDate();
      final BigInteger serialNumber = new BigInteger(128, random);

      // Create a certificate valid for 5years from now.
      final X509v3CertificateBuilder certificateBuilder = new JcaX509v3CertificateBuilder(
          nameBuilder.build(), // attributes
          serialNumber,
          notBefore,
          notAfter,
          nameBuilder.build(),
          keyPair.getPublic());

      // Sign the certificate using the private key
      final ContentSigner contentSigner =
          new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(keyPair.getPrivate());
      final X509Certificate certificate =
          new JcaX509CertificateConverter().getCertificate(certificateBuilder.build(contentSigner));

      // Check the validity
      certificate.checkValidity(now.toDate());

      // Make sure the certificate is self-signed.
      certificate.verify(certificate.getPublicKey());

      // Generate a random password for keystore protection
      final String keyStorePasswd = RandomStringUtils.random(20);
      final KeyStore keyStore = KeyStore.getInstance("JKS");
      keyStore.load(null, null);
      keyStore.setKeyEntry("DrillAutoGeneratedCert", keyPair.getPrivate(),
          keyStorePasswd.toCharArray(), new java.security.cert.Certificate[]{certificate});

      sslContextFactory.setKeyStore(keyStore);
      sslContextFactory.setKeyStorePassword(keyStorePasswd);
//    }

    final HttpConfiguration httpsConfig = new HttpConfiguration();
    httpsConfig.addCustomizer(new SecureRequestCustomizer());

    // SSL Connector
    final ServerConnector sslConnector = new ServerConnector(embeddedJetty,
        new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
        new HttpConnectionFactory(httpsConfig));
    sslConnector.setPort(config.getInt(DrillOnYarnConfig.HTTP_PORT));

    return sslConnector;
  }

  @Override
  public void close() throws Exception {
    if (embeddedJetty != null) {
      embeddedJetty.stop();
    }
  }
}
