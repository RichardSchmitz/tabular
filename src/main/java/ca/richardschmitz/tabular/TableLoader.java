package ca.richardschmitz.tabular;

import ca.richardschmitz.tabular.mapper.TypeMapperRegistry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

public class TableLoader {
  private final TableDefinition tableDefinition;
  private final TypeMapperRegistry typeMapperRegistry;

  public TableLoader(TableDefinition tableDefinition) {
    this(tableDefinition, TypeMapperRegistry.getDefault());
  }

  public TableLoader(TableDefinition tableDefinition, TypeMapperRegistry typeMapperRegistry) {
    this.tableDefinition = tableDefinition;
    this.typeMapperRegistry = typeMapperRegistry;
  }

  public void loadRow(Connection con, Map<Integer, String> row) throws SQLException {
    PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO " + tableDefinition.getFullyQualifiedName() + "(" + getColumnNames() +") VALUES (" + getParameterPlaceholders() + ")");
    row.forEach((index, value) -> {
      int sqlIndex = index + 1;
      int colType = tableDefinition.getColumns().get(index).getType();
      try {
        if ("".equals(value)) {
          preparedStatement.setNull(sqlIndex, colType);
        } else {
          typeMapperRegistry.setValue(preparedStatement, sqlIndex, value, colType);
        }
      } catch (SQLException ex) {
        throw new RuntimeException(ex);
      }
    });

    preparedStatement.execute();
  }

  private String getColumnNames() {
    return tableDefinition.getColumns().stream().map(c -> c.getName()).collect(Collectors.joining(", "));
  }

  private String getParameterPlaceholders() {
    return tableDefinition.getColumns().stream().map(c -> "?").collect(Collectors.joining(", "));
  }
}
