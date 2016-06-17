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
package org.apache.drill.yarn.core;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.apache.drill.yarn.appMaster.TaskSpec;
import org.mortbay.log.Log;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

public class NodePool
{
  // The following keys are relative to the cluster pool definition

  public static final String POOL_NAME = "name";
  public static final String POOL_TYPE = "type";
  public static final String POOL_SIZE = "count";

  // For the labeled pool

  public static final String DRILLBIT_LABEL = "drillbit-label-expr";
  public static final String AM_LABEL = "am-label-expr";


  /**
   * Defined cluster types. The value of the type appears as the
   * value of the {@link $CLUSTER_TYPE} parameter in the config file.
   */

  public enum PoolType {
    BASIC( "basic" ),
    LABELED( "labeled" );

    private String value;

    private PoolType( String value ) {
      this.value = value;
    }

    public static PoolType toEnum( String value ) {
      for ( PoolType type : PoolType.values() ) {
        if ( type.value.equalsIgnoreCase( value ) ) {
          return type; }
      }
      return null;
    }

    public String toValue( ) { return value; }
  }

  public static class Pool
  {
    public String name;
    public int count;
    public PoolType type;

    public void getPairs(int index, List<NameValuePair> pairs) {
      String key = DrillOnYarnConfig.append( DrillOnYarnConfig.CLUSTER_POOLS, Integer.toString( index ) );
      addPairs( pairs, key );
    }

    protected void addPairs( List<NameValuePair> pairs, String key )
    {
      pairs.add( new NameValuePair( DrillOnYarnConfig.append( key, POOL_NAME ), name ) );
      pairs.add( new NameValuePair( DrillOnYarnConfig.append( key, POOL_TYPE ), type ) );
      pairs.add( new NameValuePair( DrillOnYarnConfig.append( key, POOL_SIZE ), count ) );
    }

    public void dump(String prefix, PrintStream out) {
      out.print( prefix );
      out.print( "name = " );
      out.println( name );
      out.print( prefix );
      out.print( "type = " );
      out.println( type.toValue() );
      out.print( prefix );
      out.print( "count = " );
      out.println( count );
    }

    public void load(Map<String,Object> pool, int index) {
      try {
        count = (Integer) pool.get( POOL_SIZE );
      }
      catch ( ClassCastException e ) {
        throw new IllegalArgumentException( "Expected an integer for " + POOL_SIZE + " for pool " + index );
      }
      Object nameValue = pool.get( POOL_NAME );
      if ( nameValue != null ) {
        name = nameValue.toString();
      }
      if ( DoYUtil.isBlank( name ) ) {
        name = "pool-" + Integer.toString( index );
      }
    }

    public void modifyTaskSpec(TaskSpec taskSpec) {
    }
  }

  public static class BasicPool extends Pool
  {

  }

  public static class LabeledPool extends Pool
  {
    public String drillbitLabelExpr;


    @Override
    public void load(Map<String,Object> pool, int index) {
      super.load( pool, index );
      drillbitLabelExpr = (String) pool.get( DRILLBIT_LABEL );
      if ( drillbitLabelExpr == null ) {
        Log.warn( "Labeled pool is missing the drillbit label expression (" +
                  DRILLBIT_LABEL + "), will treat pool as basic." );
      }
    }

    @Override
    public void dump(String prefix, PrintStream out) {
      out.print( prefix );
      out.print( "Drillbit label expr = " );
      out.println( (drillbitLabelExpr == null) ? "<none>" :  drillbitLabelExpr );
    }

    @Override
    protected void addPairs( List<NameValuePair> pairs, String key )
    {
      super.addPairs( pairs, key );
      pairs.add( new NameValuePair( DrillOnYarnConfig.append( key, DRILLBIT_LABEL ), drillbitLabelExpr ) );
    }

    @Override
    public void modifyTaskSpec(TaskSpec taskSpec) {
      taskSpec.containerSpec.nodeLabelExpr = drillbitLabelExpr;
    }
  }

  /**
   * Deserialize a node pool from the configuration file.
   *
   * @param n
   * @return
   */

  public static Pool getPool( Config config, int n ) {
    int index = n + 1;
    ConfigList pools = config.getList(DrillOnYarnConfig.CLUSTER_POOLS);
    ConfigValue value = pools.get( n );
    @SuppressWarnings("unchecked")
    Map<String,Object> pool = (Map<String,Object>) value.unwrapped();
    String type;
    try {
      type = pool.get( POOL_TYPE ).toString();
    }
    catch ( NullPointerException e ) {
      throw new IllegalArgumentException( "Pool type is required for pool " + index );
    }
    PoolType poolType = PoolType.toEnum( type );
    if ( poolType == null ) {
      throw new IllegalArgumentException( "Undefined type for pool " + index + ": " + type );
    }
    Pool poolDef;
    switch ( poolType ) {
    case BASIC:
      poolDef = new BasicPool( );
      break;
    case LABELED:
      poolDef = new LabeledPool( );
      break;
    default:
      assert false;
      throw new IllegalStateException( "Undefined pool type: " + poolType );
    }
    poolDef.type = poolType;
    poolDef.load( pool, index );
    return poolDef;
  }
}
