package org.apache.drill.exec.store.mock;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MajorType.Builder;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.mock.MockTableDef.MockColumn;
import org.apache.drill.exec.store.mock.MockTableDef.MockScanEntry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Structure of a mock table definition file. Yes, using Jackson deserialization to parse
 * the file is brittle, but this is for testing so we're favoring convenience
 * over robustness.
 */

@JsonTypeName("mock-table")
public class MockTableDef {
  public static class MockScanEntry {

    final int records;
    private final MockTableDef.MockColumn[] types;

    @JsonCreator
    public MockScanEntry(@JsonProperty("records") int records, @JsonProperty("types") MockTableDef.MockColumn[] types) {
      this.records = records;
      this.types = types;
    }

    public int getRecords() {
      return records;
    }

    public MockTableDef.MockColumn[] getTypes() {
      return types;
    }

    @Override
    public String toString() {
      return "MockScanEntry [records=" + records + ", columns=" + Arrays.toString(types) + "]";
    }
  }

  @JsonInclude(Include.NON_NULL)
  public static class MockColumn {
    @JsonProperty("type") public MinorType minorType;
    public String name;
    public DataMode mode;
    public Integer width;
    public Integer precision;
    public Integer scale;
    public String generator;
    public Integer repeat;
    public Map<String,Object> properties;

    @JsonCreator
    public MockColumn(@JsonProperty("name") String name,
                      @JsonProperty("type") MinorType minorType,
                      @JsonProperty("mode") DataMode mode,
                      @JsonProperty("width") Integer width,
                      @JsonProperty("precision") Integer precision,
                      @JsonProperty("scale") Integer scale,
                      @JsonProperty("generator") String generator,
                      @JsonProperty("repeat") Integer repeat,
                      @JsonProperty("properties") Map<String,Object> properties) {
      this.name = name;
      this.minorType = minorType;
      this.mode = mode;
      this.width = width;
      this.precision = precision;
      this.scale = scale;
      this.generator = generator;
      this.repeat = repeat;
      this.properties = properties;
    }

    @JsonProperty("type")
    public MinorType getMinorType() { return minorType; }
    public String getName() { return name; }
    public DataMode getMode() { return mode; }
    public Integer getWidth() { return width; }
    public Integer getPrecision() { return precision; }
    public Integer getScale() { return scale; }
    public String getGenerator( ) { return generator; }
    public Integer getRepeat() { return repeat; }
    @JsonIgnore
    public int getRepeatCount() { return repeat == null ? 1 : repeat; }
    public Map<String,Object> getProperties() { return properties; }

    @JsonIgnore
    public MajorType getMajorType() {
      MajorType.Builder b = MajorType.newBuilder();
      b.setMode(mode);
      b.setMinorType(minorType);
      if (precision != null) {
        b.setPrecision(precision);
      }
      if (width != null) {
        b.setWidth(width);
      }
      if (scale != null) {
        b.setScale(scale);
      }
      return b.build();
    }

    @Override
    public String toString() {
      return "MockColumn [minorType=" + minorType + ", name=" + name + ", mode=" + mode + "]";
    }
  }

  private String descrip;
  List<MockTableDef.MockScanEntry> entries;

  public MockTableDef(@JsonProperty("descrip") final String descrip,
                      @JsonProperty("entries") final List<MockTableDef.MockScanEntry> entries) {
    this.descrip = descrip;
    this.entries = entries;
  }

  /**
   * Description of this data source. Ignored by the scanner, purely
   * for the convenience of the author.
   */

  public String getDescrip( ) { return descrip; }

  /**
   * The set of entries that define the groups within the file. Each
   * group can have a distinct schema; each may be read in a separate
   * fragment.
   * @return
   */

  public List<MockTableDef.MockScanEntry> getEntries() { return entries; }
}
