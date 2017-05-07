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
package org.apache.drill.exec.physical.rowSet;

/**
 * Writes values into the current row or map by column index or name.
 * Column indexes and names are as defined by the schema.
 *
 * @see {@link SingleMapWriter}, the class which this class
 * replaces
 */

public interface TupleLoader {

  /**
   * Unchecked exception thrown when attempting to access a column loader
   * by name for an undefined columns. Readers that use a fixed schema
   * can simply omit catch blocks for the exception since it is unchecked
   * and won't be thrown if the schema can't evolve. Readers that can
   * discover new columns should catch the exception and define the
   * column.
   */

  @SuppressWarnings("serial")
  public static class UndefinedColumnException extends RuntimeException {
    public UndefinedColumnException(String msg) {
      super(msg);
    }
  }

  TupleSchema schema();
  ColumnLoader column(int colIndex);

  /**
   * Return the column loader for the given column name. Throws
   * the {@link UndefinedColumnException} exception if the column
   * is undefined. For readers, such as JSON, that work by name,
   * and discover columns as they appear on input,
   * first attempt to get the column loader. Catch the exception
   * if the column does not exist, define the column
   * then the column is undefined, and the code should add the
   * new column and again retrieve the loader.
   *
   * @param colName
   * @return the column loader for the column
   * @throws {@link UndefinedColumnException} if the column is
   * undefined.
   */
  ColumnLoader column(String colName);
}
