package ca.richardschmitz.tabular;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.stream.Collectors;

public class TableLoader {
  private final TableDefinition tableDefinition;

  public TableLoader(TableDefinition tableDefinition) {
    this.tableDefinition = tableDefinition;
  }

  public void loadRow(Connection con, Map<Integer, String> row) throws SQLException {
    PreparedStatement preparedStatement = con.prepareStatement("insert into " + tableDefinition.getFullyQualifiedName() + "(" + getColumnNames() +") VALUES (" + getParameterPlaceholders() + ")");
    row.forEach((index, value) -> {
      int sqlIndex = index + 1;
      try {
        int colType = tableDefinition.getColumns().get(index).getType();
        switch (colType) {
          case Types.VARCHAR:
            preparedStatement.setString(sqlIndex, value);
            break;
          case Types.INTEGER:
            preparedStatement.setInt(sqlIndex, Integer.parseInt(value));
            break;
          default:
            throw new RuntimeException("Unhandled column type: " + colType);
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
