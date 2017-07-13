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
package org.apache.drill.exec.vector.accessor2;

import org.apache.drill.exec.record.MaterializedField;

public class Exp3 {

//  ResultSetWriter newWriter( ) { return null; }

//  void example() {
//    ResultSetWriter resultWriter = newWriter( );
//
//    ArrayWriter batchWriter = resultWriter.rows();
//    batchWriter.next();
//
//    TupleWriter rowWriter = batchWriter.tuple();
//
//    // Required Int
//
//    rowWriter.scalar("a").setInt(10);
//
//    // Repeated Int
//
//    ArrayWriter bArray = rowWriter.array("b");
//    ScalarWriter bValue = bArray.scalar();
//    bArray.next(); // Optional
//    bValue.setInt(10);
//    bArray.save(); // Optional
//    bArray.next(); // Optional
//    bValue.setInt(20);
//    bArray.save(); // Optional
//
//    // Repeated int, abbreviated
//
//    bValue = rowWriter.array("b").scalar();
//    bValue.setInt(30);
//    bValue.setInt(40);
//
//    // Map
//
//    TupleWriter c = rowWriter.tuple("c");
//    c.scalar("c1").setInt(100);
//    c.scalar("c2").setInt(200);
//
//    // Repeated map
//
//    ArrayWriter dArray = rowWriter.array("d");
//    TupleWriter dMap = dArray.tuple();
//    dArray.next();
//    dMap.scalar("d1").setInt(300);
//
//    // List of repeated map
//
//    ArrayWriter eOuter = rowWriter.array("e");
//    ArrayWriter eInner = eOuter.array();
//    TupleWriter eMap = eInner.tuple();
//    eOuter.next();
//    eInner.next();
//    eMap.scalar("e1").setInt(400);
//
//
//    batchWriter.save();
//
//    resultWriter.done();
//    // Get row set, or whatever
//  }
}
