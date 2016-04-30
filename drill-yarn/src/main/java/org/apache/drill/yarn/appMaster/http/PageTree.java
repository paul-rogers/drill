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

import java.util.HashMap;
import java.util.Map;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.apache.drill.yarn.appMaster.Dispatcher;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
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

  private static String clusterName;
  private static Dispatcher dispatcher;

  public PageTree( Dispatcher dispatcher ) {
    PageTree.dispatcher = dispatcher;
    clusterName = DrillOnYarnConfig.config( ).getString( DrillOnYarnConfig.APP_NAME );

    // Markup engine
    register(FreemarkerMvcFeature.class);

    // Pages
    register(RootPage.class);
    register(ConfigPage.class);
    register(DrillbitsPage.class);
    register(ManagePage.class);
    register(ResizePage.class);
    register(StopPage.class);
  }

  public static Map<String,Object> toModel( Object base ) {
    Map<String,Object> model = new HashMap<>( );
    model.put( "model", base );
    model.put( "clusterName", clusterName );
    return model;
  }

}
