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

import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.security.PermitAll;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.exec.server.rest.ViewableWithPermissions;
import org.apache.drill.yarn.appMaster.Dispatcher;
import org.apache.drill.yarn.appMaster.DrillApplicationMaster;
import org.apache.drill.yarn.appMaster.http.AbstractTasksModel.TaskModel;
import org.apache.drill.yarn.appMaster.http.ControllerModel.PoolModel;
import org.apache.drill.yarn.core.DoYUtil;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.core.NameValuePair;
import org.apache.drill.yarn.zk.ZKClusterCoordinatorDriver;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.glassfish.jersey.server.mvc.Viewable;
import org.glassfish.jersey.server.mvc.freemarker.FreemarkerMvcFeature;

import com.typesafe.config.Config;

public class PageTree extends ResourceConfig
{
  private static final Log LOG = LogFactory.getLog(PageTree.class);

  @Path("/")
  @PermitAll
  public static class RootPage
  {
    @GET
    public Viewable getRoot( ) {
      ControllerModel model = new ControllerModel( );
      dispatcher.getController().visit( model );
      return new Viewable( "/drill-am/index.ftl", toModel( model ) );
    }
  }

  @Path("/")
  @PermitAll
  public class LogInLogOutPages {
    public static final String REDIRECT_QUERY_PARM = "redirect";
    public static final String LOGIN_RESOURCE = "login";

    @GET
    @Path("/login")
    @Produces(MediaType.TEXT_HTML)
    public Viewable getLoginPage(@Context HttpServletRequest request, @Context HttpServletResponse response,
        @Context SecurityContext sc, @Context UriInfo uriInfo, @QueryParam(REDIRECT_QUERY_PARM) String redirect)
        throws Exception {
      if (AuthDynamicFeature.isUserLoggedIn(sc)) {
        // if the user is already login, forward the request to homepage.
        request.getRequestDispatcher("/").forward(request, response);
        return null;
      }

      if (!StringUtils.isEmpty(redirect)) {
        // If the URL has redirect in it, set the redirect URI in session, so that after the login is successful, request
        // is forwarded to the redirect page.
        final HttpSession session = request.getSession(true);
        final URI destURI = UriBuilder.fromUri(URLDecoder.decode(redirect, "UTF-8")).build();
        session.setAttribute(FormAuthenticator.__J_URI, destURI.toString());
      }

      return new Viewable("/drill-am/login.ftl", null);
    }

    // Request type is POST because POST request which contains the login credentials are invalid and the request is
    // dispatched here directly.
    @POST
    @Path("/login")
    @Produces(MediaType.TEXT_HTML)
    public Viewable getLoginPageAfterValidationError() {
      return new Viewable("/drill-am/login.ftl", "Invalid user name or password.");
    }

    @GET
    @Path("/logout")
    public void logout(@Context HttpServletRequest req, @Context HttpServletResponse resp) throws Exception {
      final HttpSession session = req.getSession();
      if (session != null) {
        session.invalidate();
      }

      req.getRequestDispatcher("/").forward(req, resp);
    }
  }

  @Path("/redirect")
  @PermitAll
  public static class RedirectPage
  {
    @GET
    public Viewable getRoot( ) {
      Map<String,String> map = new HashMap<>( );
      String baseUrl = DoYUtil.unwrapAmUrl( dispatcher.getTrackingUrl() );
      map.put( "amLink", baseUrl );
      map.put( "clusterName", clusterName );
      return new Viewable( "/drill-am/redirect.ftl", map );
    }
  }

  @Path("/config")
  @PermitAll
  public static class ConfigPage
  {
    @GET
    public Viewable getRoot( ) {
      return new Viewable( "/drill-am/config.ftl", toModel( DrillOnYarnConfig.instance().getPairs() ) );
    }
  }

  @Path("/drillbits")
  @PermitAll
  public static class DrillbitsPage
  {
    @GET
    public Viewable getRoot( ) {
      AbstractTasksModel.TasksModel model = new AbstractTasksModel.TasksModel( );
      dispatcher.getController().visitTasks( model );
      model.sortTasks( );
      return new Viewable( "/drill-am/tasks.ftl", toModel( model.results ) );
    }
  }

  @Path("/cancel/")
  @PermitAll
  public static class CancelDrillbitPage
  {
    @QueryParam("id")
    int id;

    @GET
    public Viewable getPage( ) {
      ConfirmShrink confirm = new ConfirmShrink( ConfirmShrink.Mode.CANCEL );
      confirm.id = id;
      return new Viewable( "/drill-am/shrink-warning.ftl", toModel( confirm ) );
    }

    @POST
    public Viewable postPage( ) {
      Acknowledge ack;
      if ( dispatcher.getController().cancelTask( id ) ) {
        ack = new Acknowledge( Acknowledge.Mode.CANCELLED );
      } else {
        ack = new Acknowledge( Acknowledge.Mode.INVALID_TASK );
      }
      ack.value = id;
      return new Viewable( "/drill-am/confirm.ftl", toModel( ack ) );
    }
  }

  @Path("/history")
  @PermitAll
  public static class HistoryPage
  {
    @GET
    public Viewable getRoot( ) {
      AbstractTasksModel.HistoryModel model = new AbstractTasksModel.HistoryModel( );
      dispatcher.getController().visit( model );
      return new Viewable( "/drill-am/history.ftl", toModel( model.results ) );
    }
  }

  @Path("/manage")
  @PermitAll
  public static class ManagePage
  {
    @GET
    public Viewable getRoot( ) {
      ControllerModel model = new ControllerModel( );
      dispatcher.getController().visit( model );
      return new Viewable( "/drill-am/manage.ftl", toModel( model ) );
    }
  }

  /**
   * Pass information to the acknowledgement page.
   */

  public static class Acknowledge
  {
    public enum Mode {
      STOPPED, INVALID_RESIZE, INVALID_ACTION, NULL_RESIZE, RESIZED, CANCELLED, INVALID_TASK };

    Mode mode;
    Object value;

    public Acknowledge( Mode mode ) {
      this.mode = mode;
    }

    public String getType( ) { return mode.toString(); }
    public Object getValue( ) { return value; }
  }

  /**
   * Pass information to the confirmation page.
   */

  public static class ConfirmShrink
  {
    public enum Mode { SHRINK, STOP, CANCEL };

    Mode mode;
    int value;
    int id;

    public ConfirmShrink( Mode mode ) {
      this.mode = mode;
    }

    public boolean isStop( ) { return mode == Mode.STOP; }
    public boolean isCancel( ) { return mode == Mode.CANCEL; }
    public boolean isShrink( ) { return mode == Mode.SHRINK; }
    public int getCount( ) { return value; }
    public int getId( ) { return id; }
  }

  @Path("/resize")
  @PermitAll
  public static class ResizePage
  {
    @FormParam("n")
    int n;
    @FormParam( "type" )
    String type;

    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Viewable resize( ) {
      int curSize = dispatcher.getController().getTargetCount( );
      if ( n <= 0 ) {
        Acknowledge confirm = new Acknowledge( Acknowledge.Mode.INVALID_RESIZE );
        confirm.value = n;
        return new Viewable( "/drill-am/confirm.ftl", toModel( confirm ) );
      }
      if ( type == null ) {
        type = "null";
      }
      int newSize;
      boolean confirmed = false;
      if ( type.equalsIgnoreCase( "resize" ) ) {
        newSize = n;
      }
      else if ( type.equalsIgnoreCase( "grow" ) ) {
        newSize = curSize + n;
      }
      else if ( type.equalsIgnoreCase( "shrink" ) ) {
        newSize = curSize - n;
      }
      else if ( type.equalsIgnoreCase( "force-shrink" ) ) {
        newSize = curSize - n;
        confirmed = true;
      }
      else {
        Acknowledge confirm = new Acknowledge( Acknowledge.Mode.INVALID_ACTION );
        confirm.value = type;
        return new Viewable( "/drill-am/confirm.ftl", toModel( confirm ) );
      }

      if ( curSize == newSize ) {
        Acknowledge confirm = new Acknowledge( Acknowledge.Mode.NULL_RESIZE );
        confirm.value = newSize;
        return new Viewable( "/drill-am/confirm.ftl", toModel( confirm ) );
      }
      else if ( confirmed || curSize < newSize ) {
        dispatcher.getController().resizeTo( newSize );
        Acknowledge confirm = new Acknowledge( Acknowledge.Mode.RESIZED );
        confirm.value = newSize;
        return new Viewable( "/drill-am/confirm.ftl", toModel( confirm ) );
      }
      else {
        ConfirmShrink confirm = new ConfirmShrink( ConfirmShrink.Mode.SHRINK );
        confirm.value = curSize - newSize;
        return new Viewable( "/drill-am/shrink-warning.ftl", toModel( confirm ) );
      }
    }
  }

  @Path("/stop/")
  @PermitAll
  public static class StopPage
  {
    @GET
    public Viewable requestStop( ) {
      ConfirmShrink confirm = new ConfirmShrink( ConfirmShrink.Mode.STOP );
      return new Viewable( "/drill-am/shrink-warning.ftl", toModel( confirm ) );
    }

    @POST
    public Viewable doStop( ) {
      dispatcher.getController().shutDown();
      Acknowledge confirm = new Acknowledge( Acknowledge.Mode.STOPPED );
      return new Viewable( "/drill-am/confirm.ftl", toModel( confirm ) );
    }
  }

  @Path("/rest/config")
  @PermitAll
  public static class ConfigResource
  {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String,Object> getConfig( ) {
      Map<String,Object> map = new HashMap<>( );
      for ( NameValuePair pair : DrillOnYarnConfig.instance().getPairs() ) {
        map.put( pair.getName(), pair.getValue( ) );
      }
      return map;
    }
  }

  /**
   * Returns cluster status as a tree of JSON objects. Done as explicitly-defined
   * maps to specify the key names (which must not change to avoid breaking
   * compatibility) and to handle type conversions.
   */

  @Path("/rest/status")
  @PermitAll
  public static class StatusResource
  {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String,Object> getStatus( ) {
      ControllerModel model = new ControllerModel( );
      dispatcher.getController().visit( model );

      Map<String,Object> root = new HashMap<>( );
      root.put( "state", model.state.toString() );

      Map<String,Object> summary = new HashMap<>( );
      summary.put( "drillMemoryMb", model.totalDrillMemory );
      summary.put( "drillVcores", model.totalDrillVcores );
      summary.put( "yarnMemoryMb", model.yarnMemory );
      summary.put( "yarnVcores", model.yarnVcores );
      summary.put( "liveBitCount", model.liveCount );
      summary.put( "totalBitCount", model.taskCount );
      summary.put( "targetBitCount", model.targetCount );
      root.put( "summary", summary );

      List<Map<String,Object>> pools = new ArrayList<>( );
      for ( PoolModel pool : model.pools ) {
        Map<String,Object> poolObj = new HashMap<>( );
        poolObj.put( "name", pool.name );
        poolObj.put( "type", pool.type );
        poolObj.put( "liveBitCount", pool.liveCount );
        poolObj.put( "targetBitCount", pool.targetCount );
        poolObj.put( "totalBitCount", pool.taskCount );
        poolObj.put( "totalMemoryMb", pool.memory );
        poolObj.put( "totalVcores", pool.vcores );
        pools.add( poolObj );
      }
      root.put( "pools", pools );

      AbstractTasksModel.TasksModel tasksModel = new AbstractTasksModel.TasksModel( );
      dispatcher.getController().visitTasks( tasksModel );
      List<Map<String,Object>> bits = new ArrayList<>( );
      for ( TaskModel task : tasksModel.results ) {
        Map<String,Object> bitObj = new HashMap<>( );
        bitObj.put( "containerId", task.container.getId().toString() );
        bitObj.put( "host", task.getHost() );
        bitObj.put( "id", task.id );
        bitObj.put( "live", task.isLive() );
        bitObj.put( "memoryMb", task.memoryMb );
        bitObj.put( "vcores", task.vcores );
        bitObj.put( "pool", task.poolName );
        bitObj.put( "state", task.state );
        bitObj.put( "trackingState", task.trackingState );
        bitObj.put( "endpoint", ZKClusterCoordinatorDriver.asString( task.endpoint ) );
        bitObj.put( "link", task.getLink() );
        bitObj.put( "startTime", task.getStartTime() );
        bits.add( bitObj );
      }
      root.put( "drillbits", bits );

      return root;
    }
  }

  /**
   * Stop the cluster. Uses a key to validate the request. The value of the key is
   * set in the Drill-on-YARN configuration file. The purpose is simply to prevent
   * accidental cluster shutdown when experimenting with the REST API; this is
   * not meant to be a security mechanism.
   *
   * @param key
   * @return
   */

  @Path("/rest/stop")
  @PermitAll
  public static class StopResource
  {
    @DefaultValue( "" )
    @QueryParam( "key" )
    String key;

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public String postStop(
           )
    {
      String masterKey = DrillOnYarnConfig.config( ).getString( DrillOnYarnConfig.HTTP_REST_KEY );
      if ( ! DoYUtil.isBlank( masterKey ) && ! masterKey.equals( key ) ) {
        return "Invalid Key";
      }
      dispatcher.getController().shutDown();
      return "OK";
    }
  }

  private static String clusterName;
  private static Dispatcher dispatcher;

  public PageTree( Dispatcher dispatcher ) {
    PageTree.dispatcher = dispatcher;
    Config config = DrillOnYarnConfig.config( );
    clusterName = config.getString( DrillOnYarnConfig.APP_NAME );

    // Markup engine
    register(FreemarkerMvcFeature.class);

    // Web UI Pages
    register(RootPage.class);
    register(RedirectPage.class);
    register(ConfigPage.class);
    register(DrillbitsPage.class);
    register(CancelDrillbitPage.class);
    register(HistoryPage.class);
    register(ManagePage.class);
    register(ResizePage.class);
    register(StopPage.class);

    // REST API

    register(ConfigResource.class);
    register(StatusResource.class);
    register(StopResource.class);

    // Authorization

    if (config.getBoolean(DrillOnYarnConfig.HTTP_AUTH_ENABLED)) {
      register(LogInLogOutPages.class);
      register(AuthDynamicFeature.class);
      register(RolesAllowedDynamicFeature.class);
    }
  }

  public static Map<String,Object> toModel( Object base ) {
    Map<String,Object> model = new HashMap<>( );
    model.put( "model", base );
    model.put( "clusterName", clusterName );
    return model;
  }

}
