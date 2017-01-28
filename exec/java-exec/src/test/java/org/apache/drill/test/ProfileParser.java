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
 ******************************************************************************/
package org.apache.drill.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.google.common.base.Preconditions;

/**
 * Parses a query profile and provides access to various bits of the profile
 * for diagnostic purposes during tests.
 */

public class ProfileParser {

  JsonObject profile;
  String query;
  List<String> plans;
  Map<Integer,FragInfo> fragments = new HashMap<>();

  public ProfileParser( File file ) throws IOException {
    try (FileReader fileReader = new FileReader(file);
         JsonReader reader = Json.createReader(fileReader)) {
      profile = (JsonObject) reader.read();
    }

    parse();
  }

  private void parse() {
    parseQuery();
    parsePlans();
    parseFragProfiles();
    mapOpProfiles();
    aggregateOpers();
    determineLocalLevels();
    determineBranches();
  }

  private void parseQuery() {
    query = profile.getString("query");
    query = query.replace("//n", "\n");
  }

  private void parsePlans() {
    String plan = getPlan( );
    plans = new ArrayList<>( );
    String parts[] = plan.split("\n");
    for (String part : parts) {
      plans.add(part);
      parsePlanOp(part);
    }
  }

  private void parsePlanOp(String plan) {
    OpDefInfo opDef = new OpDefInfo( plan );
    FragInfo major = fragments.get(opDef.majorId);
    if (major == null) {
      major = new FragInfo(opDef.majorId);
      fragments.put(opDef.majorId, major);
      if (opDef.majorId > 0) {
        assert opDef.stepId == 1;
        OpDefInfo sender = new OpDefInfo( major.id, 0 );
        sender.isInferred = true;
        sender.name = "Sender";
        major.ops.add(sender);
//        System.out.println( "Parse Plan: " + sender );
      }
    }
    if ( opDef.stepId > major.ops.size() ) {
      OpDefInfo unknown = new OpDefInfo( major.id, major.ops.size() );
      unknown.isInferred = true;
      unknown.name = "Unknown";
      major.ops.add(unknown);
    }
    assert opDef.stepId == major.ops.size();
    major.ops.add(opDef);
//    System.out.println( "Parse Plan: " + opDef );
  }

  private static List<FieldDef> parseCols(String cols) {
    String parts[] = cols.split( ", " );
    List<FieldDef> fields = new ArrayList<>( );
    for ( String part : parts ) {
      String halves[] = part.split( " " );
      fields.add( new FieldDef( halves[1], halves[0] ) );
    }
    return fields;
  }

  private void parseFragProfiles() {
    JsonArray frags = getFragmentProfile( );
    for (JsonObject fragProfile : frags.getValuesAs(JsonObject.class)) {
      int mId = fragProfile.getInt("majorFragmentId");
      FragInfo major = fragments.get(mId);
      major.parse(fragProfile);
    }
  }

  private void mapOpProfiles() {
    for (FragInfo major : fragments.values()) {
      for (MinorFragInfo minor : major.minors) {
        for (OperatorProfile op : minor.ops) {
          OpDefInfo opDef = major.ops.get(op.opId);
          if ( opDef == null ) {
            System.out.println( "Can't find operator def: " + major.id + "-" + op.opId);
            continue;
          }
          op.opName = CoreOperatorType.valueOf(op.type).name();
          op.opName = op.opName.replace("_", " ");
          op.name = opDef.name;
          if (op.name.equalsIgnoreCase(op.opName)) {
            op.opName = null;
          }
          op.defn = opDef;
          opDef.opName = op.opName;
          opDef.opExecs.add(op);
        }
      }
    }
  }

  private void aggregateOpers() {
    for (FragInfo major : fragments.values()) {
      for (OpDefInfo opDef : major.ops) {
        for ( OperatorProfile op : opDef.opExecs) {
          Preconditions.checkState( major.id == op.majorFragId );
          Preconditions.checkState( opDef.stepId == op.opId );
          System.out.println( major.id + "-" + opDef.stepId + "-" + op.minorFragId + " = " + op.records );
          opDef.actualRows += op.records;
          opDef.actualBatches += op.batches;
          opDef.actualMemory += op.peakMem * 1024 * 1024;
        }
      }
    }
  }

  private void determineLocalLevels() {
    for (FragInfo major : fragments.values()) {
      OpDefInfo base = major.ops.get(0);
      int bias = 0;
      if (base.isInferred) {
        base = major.ops.get(1);
        bias = 1;
      }
      major.baseLevel = base.globalLevel;
      for (OpDefInfo opDef : major.ops) {
        if (opDef.isInferred) {
          opDef.localLevel = 0;
        } else {
          opDef.localLevel = opDef.globalLevel - major.baseLevel + bias;
        }
//        System.out.println( opDef + " " + opDef.localLevel + ", " + opDef.globalLevel );
      }
    }
  }

  private void determineBranches() {
    for (FragInfo major : fragments.values()) {
      int branchId = 0;
      int currentLevel = 0;
      OpDefInfo opStack[] = new OpDefInfo[major.ops.size()];
      for (OpDefInfo opDef : major.ops) {
        if (opDef.isInferred && currentLevel > 0 ) {
          opStack[currentLevel - 1].children.add(opDef);
          continue;
        }
        if (opDef.localLevel < currentLevel) {
          OpDefInfo sibling = opStack[opDef.localLevel];
          if (! sibling.isBranchRoot) {
            sibling.isBranchRoot = true;
            sibling.branchId = ++branchId;
          }
          opDef.isBranchRoot = true;
          opDef.branchId = ++branchId;
        } else {
          opDef.branchId = branchId;
        }
        currentLevel = opDef.localLevel;
        opStack[currentLevel] = opDef;
        if (currentLevel > 0) {
          opStack[currentLevel - 1].children.add(opDef);
        }
      }
//      major.ops.get(0).printTree( "" );
    }
  }

  public String getQuery( ) {
    return profile.getString("query");
  }

  public String getPlan() {
    return profile.getString("plan");
  }

  public List<String> getPlans() {
    return plans;
  }

  public List<String> getScans( ) {
    List<String> scans = new ArrayList<>();
    int n = getPlans( ).size();
//    Pattern p = Pattern.compile( "\\d+-\\d+\\s+(\\w+)\\(" );
    for ( int i = n-1; i >= 0;  i-- ) {
      String plan = plans.get( i );
//      Matcher m = p.matcher( plan );
//      if ( ! m.find() ) { continue; }
      if ( plan.contains( " Scan(" ) ) {
        scans.add( plan );
      }
    }
    return scans;
  }

  public List<FieldDef> getColumns( String plan ) {
    Pattern p = Pattern.compile( "RecordType\\((.*)\\):" );
    Matcher m = p.matcher(plan);
    if ( ! m.find() ) { return null; }
    String frag = m.group(1);
    String parts[] = frag.split( ", " );
    List<FieldDef> fields = new ArrayList<>( );
    for ( String part : parts ) {
      String halves[] = part.split( " " );
      fields.add( new FieldDef( halves[1], halves[0] ) );
    }
    return fields;
  }

  public Map<Integer,String> getOperators( ) {
    Map<Integer,String> ops = new HashMap<>();
    int n = getPlans( ).size();
    Pattern p = Pattern.compile( "\\d+-(\\d+)\\s+(\\w+)" );
    for ( int i = n-1; i >= 0;  i-- ) {
      String plan = plans.get( i );
      Matcher m = p.matcher( plan );
      if ( ! m.find() ) { continue; }
      int index = Integer.parseInt(m.group(1));
      String op = m.group(2);
      ops.put(index,op);
    }
    return ops;
  }

  public JsonArray getFragmentProfile( ) {
    return profile.getJsonArray("fragmentProfile");
  }

  public static class FragInfo {
    public int baseLevel;
    int id;
    List<OpDefInfo> ops = new ArrayList<>( );
    List<MinorFragInfo> minors = new ArrayList<>( );

    public FragInfo(int majorId) {
      this.id = majorId;
    }

    public void parse(JsonObject fragProfile) {
      JsonArray minorList = fragProfile.getJsonArray("minorFragmentProfile");
      for ( JsonObject minorProfile : minorList.getValuesAs(JsonObject.class) ) {
        minors.add( new MinorFragInfo(id, minorProfile) );
      }
    }
  }

  public static class MinorFragInfo {
    int majorId;
    int id;
    List<OperatorProfile> ops = new ArrayList<>( );

    public MinorFragInfo(int majorId, JsonObject minorProfile) {
      this.majorId = majorId;
      id = minorProfile.getInt("minorFragmentId");
      JsonArray opList = minorProfile.getJsonArray("operatorProfile");
      for ( JsonObject opProfile : opList.getValuesAs(JsonObject.class)) {
        ops.add( new OperatorProfile( majorId, id, opProfile) );
      }
    }

  }

  public static class OperatorProfile {
    public OpDefInfo defn;
    public String opName;
    public int majorFragId;
    public int minorFragId;
    public int opId;
    public int type;
    public String name;
    public long processMs;
    public long waitMs;
    public long setupMs;
    public long peakMem;
    public Map<Integer,JsonNumber> metrics = new HashMap<>();
//    public Object plannerRows;
//    public Object actualRows;
    public long records;
    public int batches;
    public int schemas;

    public OperatorProfile(int majorId, int minorId, JsonObject opProfile) {
      majorFragId = majorId;
      minorFragId = minorId;
      opId = opProfile.getInt("operatorId");
//      System.out.println( "OP: " + majorId + "-" + opId );
      type = opProfile.getInt("operatorType");
      processMs = opProfile.getJsonNumber("processNanos").longValue() / 1_000_000;
      waitMs = opProfile.getJsonNumber("waitNanos").longValue() / 1_000_000;
      setupMs = opProfile.getJsonNumber("setupNanos").longValue() / 1_000_000;
      peakMem = opProfile.getJsonNumber("peakLocalMemoryAllocated").longValue() / (1024 * 1024);
      JsonArray array = opProfile.getJsonArray("inputProfile");
      if (array != null) {
        for (int i = 0; i < array.size(); i++) {
          JsonObject obj = array.getJsonObject(i);
          records += obj.getJsonNumber("records").longValue();
          batches += obj.getInt("batches");
          schemas += obj.getInt("schemas");
        }
      }
      array = opProfile.getJsonArray("metric");
      if (array != null) {
        for (int i = 0; i < array.size(); i++) {
          JsonObject metric = array.getJsonObject(i);
          metrics.put(metric.getJsonNumber("metricId").intValue(), metric.getJsonNumber("longValue"));
        }
      }
    }

    public long getMetric(int id) {
      JsonValue value = metrics.get(id);
      if (value == null) {
        return 0; }
      return ((JsonNumber) value).longValue();
    }
  }

  public static class OpDefInfo {
    public String opName;
    public boolean isInferred;
    public int majorId;
    public int stepId;
    public String args;
    public List<FieldDef> columns;
    public int globalLevel;
    public int localLevel;
    public int id;
    public int branchId;
    public boolean isBranchRoot;
    public double estMemoryCost;
    public double estNetCost;
    public double estIOCost;
    public double estCpuCost;
    public double estRowCost;
    public double estRows;
    public String name;
    public long actualMemory;
    public int actualBatches;
    public long actualRows;
    public List<OperatorProfile> opExecs = new ArrayList<>( );
    public List<OpDefInfo> children = new ArrayList<>( );

    // 00-00    Screen : rowType = RecordType(VARCHAR(10) Year, VARCHAR(65536) Month, VARCHAR(100) Devices, VARCHAR(100) Tier, VARCHAR(100) LOB, CHAR(10) Gateway, BIGINT Day, BIGINT Hour, INTEGER Week, VARCHAR(100) Week_end_date, BIGINT Usage_Cnt): \
    // rowcount = 100.0, cumulative cost = {7.42124276972414E9 rows, 7.663067406383167E10 cpu, 0.0 io, 2.24645048816E10 network, 2.692766612982188E8 memory}, id = 129302
    //
    // 00-01      Project(Year=[$0], Month=[$1], Devices=[$2], Tier=[$3], LOB=[$4], Gateway=[$5], Day=[$6], Hour=[$7], Week=[$8], Week_end_date=[$9], Usage_Cnt=[$10]) :
    // rowType = RecordType(VARCHAR(10) Year, VARCHAR(65536) Month, VARCHAR(100) Devices, VARCHAR(100) Tier, VARCHAR(100) LOB, CHAR(10) Gateway, BIGINT Day, BIGINT Hour, INTEGER Week, VARCHAR(100) Week_end_date, BIGINT Usage_Cnt): rowcount = 100.0, cumulative cost = {7.42124275972414E9 rows, 7.663067405383167E10 cpu, 0.0 io, 2.24645048816E10 network, 2.692766612982188E8 memory}, id = 129301

    public OpDefInfo(String plan) {
      Pattern p = Pattern.compile( "^(\\d+)-(\\d+)(\\s+)(\\w+)(?:\\((.*)\\))?\\s*:\\s*(.*)$" );
      Matcher m = p.matcher(plan);
      if (!m.matches()) {
        throw new IllegalStateException( "Could not parse plan: " + plan );
      }
      majorId = Integer.parseInt(m.group(1));
      stepId = Integer.parseInt(m.group(2));
      name = m.group(4);
      args = m.group(5);
      String tail = m.group(6);
      String indent = m.group(3);
      globalLevel = (indent.length() - 4) / 2;
//      System.out.println( majorId + "-" + stepId + ": " + name + ", level = " + globalLevel );

      p = Pattern.compile("rowType = RecordType\\((.*)\\): (rowcount .*)");
      m = p.matcher(tail);
      if ( m.matches() ) {
        columns = parseCols(m.group(1));
        tail = m.group(2);
      }

      p = Pattern.compile( "rowcount = ([\\d.E]+), cumulative cost = \\{([\\d.E]+) rows, ([\\d.E]+) cpu, ([\\d.E]+) io, ([\\d.E]+) network, ([\\d.E]+) memory\\}, id = (\\d+)");
      m = p.matcher(tail);
      if (! m.matches()) {
        throw new IllegalStateException("Could not parse costs: " + tail );
      }
      estRows = Double.parseDouble(m.group(1));
      estRowCost = Double.parseDouble(m.group(2));
      estCpuCost = Double.parseDouble(m.group(3));
      estIOCost = Double.parseDouble(m.group(4));
      estNetCost = Double.parseDouble(m.group(5));
      estMemoryCost = Double.parseDouble(m.group(6));
      id = Integer.parseInt(m.group(7));
    }

    public void printTree(String indent) {
      System.out.print( indent );
      System.out.println( toString() );
      String childIndent = indent;
      if (children.size() > 1) {
        childIndent = childIndent + "  ";
      }
      for (OpDefInfo child : children) {
        child.printTree(childIndent);
      }
    }

    public OpDefInfo(int major, int id) {
      majorId = major;
      stepId = id;
    }

    @Override
    public String toString() {
      return "[OpDefInfo " + majorId + "-" + stepId + ": " + name + "]";
    }

    public void printPlan(String indent, String label) {
      System.out.print( indent + "Operator " + stepId);
      if (label != null) {
        System.out.print( " - " + label);
      }
      System.out.print( ": " + name );
      if (opName != null) {
        System.out.print( " (" + opName + ")" );
      }
      System.out.println( );
      System.out.println( indent + "  Estimates" );
      System.out.println( String.format(indent + "    Rows: %,.0f", estRows ));
      System.out.println( String.format(indent + "    Memory: %,.0f", estMemoryCost ));
      System.out.println( indent + "  Actual" );
      System.out.println( String.format(indent + "    Batches: %,d", actualBatches ));
      System.out.println( String.format(indent + "    Rows: %,d", actualRows ));
      System.out.println( String.format(indent + "    Memory: %,d", actualMemory ));
      if (children.isEmpty())
        return;
      if (children.size() == 1) {
        children.get(0).printPlan(indent, null);
        return;
      }
      String childIndent = indent + "  ";
      if (name.equals("HashJoin") ) {
        System.out.println(childIndent + "Probe");
        children.get(0).printPlan(childIndent + "  ", null);
        System.out.println(childIndent + "Builder");
        children.get(1).printPlan(childIndent + "  ", null);
        return;
      }
      for (OpDefInfo child : children) {
        child.printPlan(childIndent, null);
      }
    }
  }

  public Map<Integer,OperatorProfile> getOpInfo( ) {
    Map<Integer,String> ops = getOperators( );
    Map<Integer,OperatorProfile> info = new HashMap<>( );
    JsonArray frags = getFragmentProfile( );
    JsonObject fragProfile = frags.getJsonObject(0).getJsonArray("minorFragmentProfile").getJsonObject(0);
    JsonArray opList = fragProfile.getJsonArray("operatorProfile");
    for ( JsonObject opProfile : opList.getValuesAs(JsonObject.class) ) {
      parseOpProfile( ops, info, opProfile );
    }
    return info;
  }

  public List<OperatorProfile> getOpsOfType(int type) {
    List<OperatorProfile> ops = new ArrayList<>();
    Map<Integer,OperatorProfile> opMap = getOpInfo();
    for (OperatorProfile op : opMap.values()) {
      if (op.type == type) {
        ops.add(op);
      }
    }
    return ops;
  }

  private void parseOpProfile(Map<Integer, String> ops,
      Map<Integer, OperatorProfile> info, JsonObject opProfile) {
    OperatorProfile opInfo = new OperatorProfile( 0, 0, opProfile );
    opInfo.name = ops.get(opInfo.opId);
    info.put(opInfo.opId, opInfo);
  }

  public void printPlan() {
    List<Integer> mKeys = new ArrayList<>();
    mKeys.addAll(fragments.keySet());
    Collections.sort(mKeys);
    for (Integer mKey : mKeys) {
      System.out.println("Fragment: " + mKey);
      FragInfo frag = fragments.get(mKey);
      frag.ops.get(0).printPlan( "  ", null );
    }
  }
  public void print() {
    Map<Integer, OperatorProfile> opInfo = getOpInfo();
    int n = opInfo.size();
    long totalSetup = 0;
    long totalProcess = 0;
    for ( int i = 0;  i <= n;  i++ ) {
      OperatorProfile op = opInfo.get(i);
      if ( op == null ) { continue; }
      totalSetup += op.setupMs;
      totalProcess += op.processMs;
    }
    long total = totalSetup + totalProcess;
    for ( int i = 0;  i <= n;  i++ ) {
      OperatorProfile op = opInfo.get(i);
      if ( op == null ) { continue; }
      System.out.print( "Op: " );
      System.out.print( op.opId );
      System.out.println( " " + op.name );
      System.out.print( "  Setup:   " + op.setupMs );
      System.out.print( " - " + percent(op.setupMs, totalSetup ) + "%" );
      System.out.println( ", " + percent(op.setupMs, total ) + "%" );
      System.out.print( "  Process: " + op.processMs );
      System.out.print( " - " + percent(op.processMs, totalProcess ) + "%" );
      System.out.println( ", " + percent(op.processMs, total ) + "%" );
      if (op.type == 17) {
        long value = op.getMetric(0);
        System.out.println( "  Spills: " + value );
      }
      if (op.waitMs > 0) {
        System.out.println( "  Wait:    " + op.waitMs );
      }
      if ( op.peakMem > 0) {
        System.out.println( "  Memory: " + op.peakMem );
      }
    }
    System.out.println( "Total:" );
    System.out.println( "  Setup:   " + totalSetup );
    System.out.println( "  Process: " + totalProcess );
  }

  public static long percent( long value, long total ) {
    if ( total == 0 ) {
      return 0; }
    return Math.round(value * 100 / total );
  }

}
