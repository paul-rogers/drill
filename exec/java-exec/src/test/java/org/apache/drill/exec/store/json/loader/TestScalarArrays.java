package org.apache.drill.exec.store.json.loader;

import static org.apache.drill.test.rowSet.RowSetUtilities.boolArray;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.json.loader.BaseJsonLoaderTest.JsonLoaderFixture;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

public class TestScalarArrays extends BaseJsonLoaderTest {

  @Test
  public void testBoolean() {
    String json =
        "{a: [true, false]} {a: []} {a: null} " +
        "{a: true} {a: false} " +
        "{a: [0, 1.0, \"true\", \"\"]}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIT)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(boolArray(true, false))
        .addRow(boolArray(true))
        .addRow(boolArray(true))
        .addRow(false)  // false
        .addRow((Boolean) null)   // null
        .addRow(true)   // 1
        .addRow(false)  // 0
        .addRow(true)   // 1.0
        .addRow(false)  // 0.0
        .addRow(true)   // "true"
        .addRow(false)  // ""
        .addRow(false)  // "false"
        .addRow(false)  // "other"
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }
}
