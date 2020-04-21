package org.apache.drill.exec.store.easy.json.loader;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.BaseJsonLoaderTest.JsonLoaderFixture;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

public class TestExtendedTypes extends BaseJsonLoaderTest {

  @Test
  public void testLong() {
    String json =
        "{ a: { \"$numberLong\": 10 } }\n" +
        "{ a: null }\n" +
        "{ a: { \"$numberLong\": \"30\" } }\n" +
        "{ a: 40 }\n" +
        "{ a: \"50\" }\n";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.enableExtendedTypes = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(10)
        .addSingleCol(null)
        .addRow(30)
        .addRow(40)
        .addRow(50)
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testNonExtended() {
    String json =
        "{ a: 10, b: { }, c: { d: 30 } }";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.enableExtendedTypes = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addMap("b")
          .resumeSchema()
        .addMap("c")
          .addNullable("d", MinorType.BIGINT)
          .resumeSchema()
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(10, mapValue(), mapValue(30))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testUnknownType() {
    String json =
        "{ a: { \"$bogus\": 10 } }";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.enableExtendedTypes = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .addNullable("$bogus", MinorType.BIGINT)
          .resumeSchema()
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(mapValue(10))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testInvalidTypeToken() {
    String json =
        "{ a: { \"$numberLong\": 10 } }\n" +
        "{ a: [ ] }";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.enableExtendedTypes = true;
    loader.open(json);
    try {
      loader.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("<^{\"$numberLong\": scalar}>"));
    }
    loader.close();
  }

  @Test
  public void testInvalidTypeObject() {
    String json =
        "{ a: { \"$numberLong\": 10 } }\n" +
        "{ a: { } }";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.enableExtendedTypes = true;
    loader.open(json);
    try {
      loader.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("<{ ^\"$numberLong\": scalar}>"));
    }
    loader.close();
  }

  @Test
  public void testInvalidTypeName() {
    String json =
        "{ a: { \"$numberLong\": 10 } }\n" +
        "{ a: { \"$bogus\": 20 } }";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.enableExtendedTypes = true;
    loader.open(json);
    try {
      loader.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("<{ ^\"$numberLong\": scalar}>"));
    }
    loader.close();
  }

  @Test
  public void testInvalidValueToken() {
    String json =
        "{ a: { \"$numberLong\": 10 } }\n" +
        "{ a: { \"$numberLong\": [ ] } }";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.enableExtendedTypes = true;
    loader.open(json);
    try {
      loader.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("<{\"$numberLong\": ^scalar }>"));
    }
    loader.close();
  }

  @Test
  public void testInvalidValue() {
    String json =
        "{ a: { \"$numberLong\": 10 } }\n" +
        "{ a: { \"$numberLong\": 20.3 } }";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.enableExtendedTypes = true;
    loader.open(json);
    try {
      loader.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("Unexpected JSON value: VALUE_NUMBER_FLOAT"));
    }
    loader.close();
  }

  @Test
  public void testExtraField() {
    String json =
        "{ a: { \"$numberLong\": 10 } }\n" +
        "{ a: { \"$numberLong\": 20, bogus: 30 } }";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.enableExtendedTypes = true;
    loader.open(json);
    try {
      loader.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("<{\"$numberLong\": scalar ^}>"));
    }
    loader.close();
  }
}
