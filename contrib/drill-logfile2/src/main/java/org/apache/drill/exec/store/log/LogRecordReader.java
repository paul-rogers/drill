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
package org.apache.drill.exec.store.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;

public class LogRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogRecordReader.class);

  private abstract static class ColumnDefn {
    public final String name;
    public final int index;

    public ColumnDefn(String name, int index) {
      this.name = name;
      this.index = index;
    }

    public abstract void define(OutputMutator outputMutator) throws SchemaChangeException;
    public abstract void load(int rowIndex, String value);

    @Override
    public String toString(){
      return "Name: " + name + ", Index: " + index;
    }
  }

  private static class VarCharDefn extends ColumnDefn {

    private NullableVarCharVector.Mutator mutator;

    public VarCharDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(name,
          Types.optional(MinorType.VARCHAR));
      mutator = outputMutator.addField(field, NullableVarCharVector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      mutator.set(rowIndex, value.getBytes());
    }
  }

  private static class IntDefn extends ColumnDefn {

    private NullableIntVector.Mutator mutator;

    public IntDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(name,
          Types.optional(MinorType.INT));
      mutator = outputMutator.addField(field, NullableIntVector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      try {
        mutator.set(rowIndex, Integer.parseInt(value));
      } catch (NumberFormatException e) {
        throw UserException
          .dataReadError(e)
          .addContext("Failed to parse an INT field")
          .addContext("Column", name)
          .addContext("Position", index)
          .addContext("Value", value)
          .build(logger);
      }
    }
  }

  private static final int BATCH_SIZE = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  private final DrillFileSystem dfs;
  private final FileWork fileWork;
  private final String userName;
  private final LogFormatConfig formatConfig;
  private ColumnDefn columns[];
  private Pattern pattern;
  private BufferedReader reader;
  private int rowIndex;
  private int capturingGroups;
  private OutputMutator outputMutator;

  private int errorCount;

  private int maxErrors = 10;

  public LogRecordReader(FragmentContext context, DrillFileSystem dfs,
                         FileWork fileWork, List<SchemaPath> columns, String userName,
                         LogFormatConfig formatConfig) {
    this.dfs = dfs;
    this.fileWork = fileWork;
    this.userName = userName;
    this.formatConfig = formatConfig;

    // Ask the superclass to parse the projection list.
    setColumns(columns);
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) {
    this.outputMutator = output;

    setupPattern();
    openFile();
    setupProjection();
    defineVectors();
  }

  private void setupPattern() {
    try {
      pattern = Pattern.compile(formatConfig.regex);
      Matcher m = pattern.matcher("test");
      capturingGroups = m.groupCount();
    } catch (PatternSyntaxException e) {
      throw UserException
          .validationError(e)
          .message("Failed to parse regex: \"%s\"", formatConfig.regex)
          .build(logger);
    }
  }

  private void setupProjection() {
    if (isSkipQuery()) {
      projectNone();
    } else if (isStarQuery()) {
      projectAll();
    } else {
      projectSubset();
    }
  }

  private void projectNone() {
    columns = new ColumnDefn[]{new VarCharDefn("dummy", -1)};
  }

  private void openFile() {
    InputStream in;
    try {
      in = dfs.open(new Path(fileWork.getPath()));
    } catch (Exception e) {
      throw UserException
          .dataReadError(e)
          .message("Failed to open open input file: %s", fileWork.getPath())
          .addContext("User name", userName)
          .build(logger);
    }
    reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
  }

  private void projectAll() {
    List<String> fields = new ArrayList<>();
    fields.addAll(formatConfig.getFields());
    for (int i = fields.size(); i < capturingGroups; i++) {
      fields.add("field_" + i);
    }
    columns = new ColumnDefn[capturingGroups];

    for (int i = 0; i < capturingGroups; i++) {
      columns[i] = makeColumn(fields.get(i), i);
    }
  }

  private void projectSubset() {
    Collection<SchemaPath> project = this.getColumns();
    assert !project.isEmpty();
    columns = new ColumnDefn[project.size()];

    List<String> fields = formatConfig.getFields();
    int colIndex = 0;
    for (SchemaPath column : project) {
      if (column.getAsNamePart().hasChild()) {
        throw UserException
            .validationError()
            .message("The log format plugin supports only simple columns")
            .addContext("Projected column", column.toString())
            .build(logger);
      }
      String name = column.getAsNamePart().getName();
      int patternIndex = -1;
      for (int i = 0; i < fields.size(); i++) {
        if (fields.get(i).equalsIgnoreCase(name)) {
          patternIndex = i;
          break;
        }
      }
      columns[colIndex++] = makeColumn(name, patternIndex);
    }
  }

  private ColumnDefn makeColumn(String name, int patternIndex) {
    String typeName = null;
    if (patternIndex == -1) {
      // Use VARCHAR for missing columns
      // (instead of Drill standard of nullable int)
      typeName = MinorType.VARCHAR.name();
    }
    else if (patternIndex < formatConfig.dataTypes.size()) {
      typeName = formatConfig.dataTypes.get(patternIndex);
    }
    if (typeName == null) {
      // No type name. VARCHAR is a safe guess
      typeName = MinorType.VARCHAR.name();
    }
    MinorType type = MinorType.valueOf(typeName);
    switch (type) {
    case VARCHAR:
      return new VarCharDefn(name, patternIndex);
    case INT:
      return new IntDefn(name, patternIndex);
    default:
      throw UserException
        .validationError()
        .message("Undefined column types")
        .addContext("Position", patternIndex)
        .addContext("Field name", name)
        .addContext("Type", typeName)
        .build(logger);
    }
  }

  private void defineVectors() {
    for (int i = 0; i < columns.length; i++) {
      try {
        columns[i].define(outputMutator);
      } catch (SchemaChangeException e) {
        throw UserException
            .systemError(e)
            .message("Vector creation failed")
            .build(logger);
      }
    }
  }

  @Override
  public int next() {
    rowIndex = 0;
    while (nextLine()) {
    }
    return rowIndex;
  }

  private boolean nextLine() {
    String line;
    try {
      line = reader.readLine();
    } catch (IOException e) {
      throw UserException
          .dataReadError(e)
          .message("Error reading file:")
          .addContext("File", fileWork.getPath())
          .build(logger);
    }
    if (line == null) {
      return false;
    }
    Matcher m = pattern.matcher(line);
    if (m.matches()) {
      loadVectors(m);
      return rowIndex < BATCH_SIZE;
    }
    errorCount++;
    if (errorCount < maxErrors ) {
      logger.warn("Unmatached line: {}", line);
    }
    return true;
  }

  private void loadVectors(Matcher m) {
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].index >= 0) {
        String value = m.group(columns[i].index + 1);
        if (value != null) {
          columns[i].load(rowIndex, value);
        }
      }
    }
    rowIndex++;
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        logger.warn("Error when closing file: " + fileWork.getPath(), e);
      }
      reader = null;
    }
  }

}