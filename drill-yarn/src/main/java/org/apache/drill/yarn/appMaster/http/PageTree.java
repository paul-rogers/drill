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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.drill.yarn.appMaster.Dispatcher;
import org.apache.drill.yarn.appMaster.http.ControllerModel.PoolModel;
import org.apache.drill.yarn.appMaster.http.TasksModel.TaskModel;
import org.apache.drill.yarn.core.DoYUtil;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.core.NameValuePair;
import org.apache.drill.yarn.zk.ZKClusterCoordinatorDriver;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.mvc.Viewable;
import org.glassfish.jersey.server.mvc.freemarker.FreemarkerMvcFeature;

public class PageTree extends ResourceConfig
{
  @Path("/")
  @PermitAll
  public static class RootPage
  {
    @GET
    public Viewable getRoot( ) {
      ControllerModel model = new ControllerModel( );
      dispatcher.getController().visit( model );
      return new Viewable( "/web/index.ftl", toModel( model ) );
    }
  }

  @Path("/config")
  @PermitAll
  public static class ConfigPage
  {
    @GET
    public Viewable getRoot( ) {
      return new Viewable( "/web/config.ftl", toModel( DrillOnYarnConfig.instance().getPairs() ) );
    }
  }

  @Path("/drillbits")
  @PermitAll
  public static class DrillbitsPage
  {
    @GET
    public Viewable getRoot( ) {
      TasksModel model = new TasksModel( );
      dispatcher.getController().visitTasks( model );
      return new Viewable( "/web/tasks.ftl", toModel( model.results ) );
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
      return new Viewable( "/web/manage.ftl", toModel( model ) );
    }
  }

  public static class Acknowledge
  {
    String type;
    Object value;

    public String getType( ) { return type; }
    public Object getValue( ) { return value; }
  }

  public static class ConfirmShrink
  {
    boolean isStop;
    int value;

    public boolean isStop( ) { return isStop; }
    public int getCount( ) { return value; }
  }

  @Path("/resize/")
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
        Acknowledge confirm = new Acknowledge( );
        confirm.value = n;
        confirm.type = "invalid-resize";
        return new Viewable( "/web/confirm.ftl", toModel( confirm ) );
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
        Acknowledge confirm = new Acknowledge( );
        confirm.value = type;
        confirm.type = "invalid-action";
        return new Viewable( "/web/confirm.ftl", toModel( confirm ) );
      }

      if ( curSize == newSize ) {
        Acknowledge confirm = new Acknowledge( );
        confirm.value = newSize;
        confirm.type = "null-resize";
        return new Viewable( "/web/confirm.ftl", toModel( confirm ) );
      }
      else if ( confirmed || curSize < newSize ) {
        Acknowledge confirm = new Acknowledge( );
        confirm.value = newSize;
        confirm.type = "resized";
        return new Viewable( "/web/confirm.ftl", toModel( confirm ) );
      }
      else {
        ConfirmShrink confirm = new ConfirmShrink( );
        confirm.isStop = false;
        confirm.value = curSize - newSize;
        return new Viewable( "/web/shrink-warning.ftl", toModel( confirm ) );
      }
    }
  }

  @Path("/stop/")
  @PermitAll
  public static class StopPage
  {
    @GET
    public Viewable requestStop( ) {
      ConfirmShrink confirm = new ConfirmShrink( );
      confirm.isStop = true;
      return new Viewable( "/web/shrink-warning.ftl", toModel( confirm ) );
    }

    @POST
    public Viewable doStop( ) {
      dispatcher.getController().shutDown();
      Acknowledge confirm = new Acknowledge( );
      confirm.type = "stopped";
      return new Viewable( "/web/confirm.ftl", toModel( confirm ) );
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

      TasksModel tasksModel = new TasksModel( );
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
      String masterKey = DrillOnYarnConfig.config( ).getString( DrillOnYarnConfig.AM_REST_KEY );
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
    clusterName = DrillOnYarnConfig.config( ).getString( DrillOnYarnConfig.APP_NAME );

    // Markup engine
    register(FreemarkerMvcFeature.class);

    // Web UI Pages
    register(RootPage.class);
    register(ConfigPage.class);
    register(DrillbitsPage.class);
    register(ManagePage.class);
    register(ResizePage.class);
    register(StopPage.class);

    // REST API

    register(ConfigResource.class);
    register(StatusResource.class);
    register(StopResource.class);
  }

  public static Map<String,Object> toModel( Object base ) {
    Map<String,Object> model = new HashMap<>( );
    model.put( "model", base );
    model.put( "clusterName", clusterName );
    return model;
  }

}
