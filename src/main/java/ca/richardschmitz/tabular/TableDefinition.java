package ca.richardschmitz.tabular;

import java.util.ArrayList;
import java.util.List;

public class TableDefinition {
  private final String name;
  private final List<ColumnDefinition> columns = new ArrayList<>();

  public TableDefinition(String name) {
    this.name = name;
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
