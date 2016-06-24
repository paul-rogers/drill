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

public class WorkerTier
{
  // The following keys are relative to the cluster pool definition

  public static final String TIER_NAME = "name";
  public static final String TIER_TYPE = "type";
  public static final String TIER_SIZE = "count";

  // For the labeled pool

  public static final String DRILLBIT_LABEL = "drillbit-label-expr";
  public static final String AM_LABEL = "am-label-expr";


  /**
   * Defined cluster tier types. The value of the type appears as the
   * value of the {@link $CLUSTER_TYPE} parameter in the config file.
   */

  public enum TierType {
    BASIC( "basic" ),
    LABELED( "labeled" );

    private String value;

    private TierType( String value ) {
      this.value = value;
    }

    public static TierType toEnum( String value ) {
      for ( TierType type : TierType.values() ) {
        if ( type.value.equalsIgnoreCase( value ) ) {
          return type; }
      }
      return null;
    }

    public String toValue( ) { return value; }
  }

  public static class Tier
  {
    public String name;
    public int count;
    public TierType type;

    public void getPairs(int index, List<NameValuePair> pairs) {
      String key = DrillOnYarnConfig.append( DrillOnYarnConfig.WORKER_TIERS, Integer.toString( index ) );
      addPairs( pairs, key );
    }

    protected void addPairs( List<NameValuePair> pairs, String key )
    {
      pairs.add( new NameValuePair( DrillOnYarnConfig.append( key, TIER_NAME ), name ) );
      pairs.add( new NameValuePair( DrillOnYarnConfig.append( key, TIER_TYPE ), type ) );
      pairs.add( new NameValuePair( DrillOnYarnConfig.append( key, TIER_SIZE ), count ) );
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
        count = (Integer) pool.get( TIER_SIZE );
      }
      catch ( ClassCastException e ) {
        throw new IllegalArgumentException( "Expected an integer for " + TIER_SIZE + " for tier " + index );
      }
      Object nameValue = pool.get( TIER_NAME );
      if ( nameValue != null ) {
        name = nameValue.toString();
      }
      if ( DoYUtil.isBlank( name ) ) {
        name = "tier-" + Integer.toString( index );
      }
    }

    public void modifyTaskSpec(TaskSpec taskSpec) {
    }
  }

  public static class BasicTier extends Tier
  {

  }

  public static class LabeledTier extends Tier
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
   * Deserialize a node tier from the configuration file.
   *
   * @param n
   * @return
   */

  public static Tier getTier( Config config, int n ) {
    int index = n + 1;
    ConfigList tiers = config.getList(DrillOnYarnConfig.WORKER_TIERS);
    ConfigValue value = tiers.get( n );
    @SuppressWarnings("unchecked")
    Map<String,Object> tier = (Map<String,Object>) value.unwrapped();
    String type;
    try {
      type = tier.get( TIER_TYPE ).toString();
    }
    catch ( NullPointerException e ) {
      throw new IllegalArgumentException( "Pool type is required for tier " + index );
    }
    TierType poolType = TierType.toEnum( type );
    if ( poolType == null ) {
      throw new IllegalArgumentException( "Undefined type for tier " + index + ": " + type );
    }
    Tier tierDef;
    switch ( poolType ) {
    case BASIC:
      tierDef = new BasicTier( );
      break;
    case LABELED:
      tierDef = new LabeledTier( );
      break;
    default:
      assert false;
      throw new IllegalStateException( "Undefined tier type: " + poolType );
    }
    tierDef.type = poolType;
    tierDef.load( tier, index );
    return tierDef;
  }
}
