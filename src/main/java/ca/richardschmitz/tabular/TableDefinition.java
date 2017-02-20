package ca.richardschmitz.tabular;

import java.util.ArrayList;
import java.util.List;

public class TableDefinition {
  private final String schema;
  private final String name;
  private final List<ColumnDefinition> columns = new ArrayList<>();

  public TableDefinition(String schema, String name) {
    this.schema = schema;
    this.name = name;
  }

  public String getFullyQualifiedName() {
    if (schema == null) {
      return getName();
    } else {
      return getSchema() + "." + getName();
    }
  }

  public String getSchema() {
    return schema;
  }

  public String getName() {
    return name;
  }

  public List<ColumnDefinition> getColumns() {
    return columns;
  }

  public void addColumn(int index, String name, int type) {
    ColumnDefinition column = new ColumnDefinition();
    column.index = index;
    column.name = name;
    column.type = type;

    columns.add(index, column);
  }

  public static class ColumnDefinition {
    private int index;
    private String name;
    private int type;

    public int getIndex() {
      return index;
    }

    public String getName() {
      return name;
    }

    public int getType() {
      return type;
    }
  }
}
