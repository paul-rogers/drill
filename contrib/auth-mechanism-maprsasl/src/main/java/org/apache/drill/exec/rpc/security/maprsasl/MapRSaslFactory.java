/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.security.maprsasl;

import com.mapr.security.callback.MaprSaslCallbackHandler;
import com.mapr.security.maprsasl.MaprSaslProvider;
import org.apache.drill.exec.rpc.security.AuthenticatorFactory;
import org.apache.drill.exec.rpc.security.FastSaslServerFactory;
import org.apache.drill.exec.rpc.security.FastSaslClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.Map;

public class MapRSaslFactory implements AuthenticatorFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRSaslFactory.class);

  public static final String SIMPLE_NAME = "MAPRSASL";

  private static final String MECHANISM_NAME = "MAPR-SECURITY";

  private static final String SASL_DEFAULT_REALM = "default";

  static {
    Security.addProvider(new MaprSaslProvider());
  }

  @Override
  public String getSimpleName() {
    return SIMPLE_NAME;
  }

  @Override
  public UserGroupInformation createAndLoginUser(final Map<String, ?> properties) throws IOException {
    final Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "CUSTOM");
    conf.set("hadoop.login", "maprsasl");
    UserGroupInformation.setConfiguration(conf);
    try {
      return UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof LoginException) {
        throw new SaslException("Failed to login.", cause);
      }
      throw new SaslException("Unexpected failure trying to login.", cause);
    }
  }

  @Override
  public SaslServer createSaslServer(final UserGroupInformation ugi, final Map<String, ?> properties)
      throws SaslException {
    try {
      final String primaryName = ugi.getShortUserName();

      final SaslServer saslServer = ugi.doAs(new PrivilegedExceptionAction<SaslServer>() {
        @Override
        public SaslServer run() throws Exception {
          final AccessControlContext context = AccessController.getContext();
          final Subject subject = Subject.getSubject(context); // login user subject
          return FastSaslServerFactory.getInstance()
              .createSaslServer(MECHANISM_NAME, null, SASL_DEFAULT_REALM, properties,
                  new MaprSaslCallbackHandler(subject, primaryName));
        }
      });
      logger.trace("MapRSasl SaslServer created.");
      return saslServer;
    } catch (final UndeclaredThrowableException e) {
      throw new SaslException("Unexpected failure trying to authenticate using MapRSasl", e.getCause());
    } catch (final IOException | InterruptedException e) {
      if (e instanceof SaslException) {
        throw (SaslException) e;
      } else {
        throw new SaslException("Unexpected failure trying to authenticate using MapRSasl", e);
      }
    }
  }

  @Override
  public SaslClient createSaslClient(final UserGroupInformation ugi, final Map<String, ?> properties)
      throws SaslException {
    try {
      final SaslClient saslClient = ugi.doAs(new PrivilegedExceptionAction<SaslClient>() {
        @Override
        public SaslClient run() throws Exception {
          return FastSaslClientFactory.getInstance().createSaslClient(new String[]{MECHANISM_NAME},
              null /** authorization ID */, null, null, properties, new CallbackHandler() {
                @Override
                public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                  throw new UnsupportedCallbackException(callbacks[0]);
                }
              });
        }
      });
      logger.trace("MapRSasl SaslClient created.");
      return saslClient;
    } catch (final UndeclaredThrowableException e) {
      throw new SaslException("Unexpected failure trying to authenticate using MapRSasl", e.getCause());
    } catch (final IOException | InterruptedException e) {
      if (e instanceof SaslException) {
        throw (SaslException) e;
      } else {
        throw new SaslException("Unexpected failure trying to authenticate using MapRSasl", e);
      }
    }
  }

  @Override
  public void close() throws Exception {
    // no-op
  }
}
