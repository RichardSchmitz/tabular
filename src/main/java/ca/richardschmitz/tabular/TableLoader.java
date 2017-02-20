package ca.richardschmitz.tabular;

import java.math.BigDecimal;
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
    PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO " + tableDefinition.getFullyQualifiedName() + "(" + getColumnNames() +") VALUES (" + getParameterPlaceholders() + ")");
    row.forEach((index, value) -> {
      int sqlIndex = index + 1;
      int colType = tableDefinition.getColumns().get(index).getType();
      try {
        if ("".equals(value)) {
          preparedStatement.setNull(sqlIndex, colType);
        } else {
          switch (colType) {
            case Types.VARCHAR:
              preparedStatement.setString(sqlIndex, value);
              break;
            case Types.TINYINT:
              preparedStatement.setByte(sqlIndex, Byte.parseByte(value));
              break;
            case Types.SMALLINT:
              preparedStatement.setShort(sqlIndex, Short.parseShort(value));
              break;
            case Types.INTEGER:
              preparedStatement.setInt(sqlIndex, Integer.parseInt(value));
              break;
            case Types.BIGINT:
              preparedStatement.setLong(sqlIndex, Long.parseLong(value));
              break;
            case Types.REAL:
              preparedStatement.setFloat(sqlIndex, Float.parseFloat(value));
              break;
            case Types.DOUBLE:
              preparedStatement.setDouble(sqlIndex, Double.parseDouble(value));
              break;
            case Types.NUMERIC:
            case Types.DECIMAL:
              preparedStatement.setBigDecimal(sqlIndex, new BigDecimal(value));
              break;
            case Types.BOOLEAN:
              boolean parsedValue;
              if ("Y".equals(value) || "T".equals(value)) {
                parsedValue = true;
              } else if ("N".equals(value) || "F".equals(value)) {
                parsedValue = false;
              } else {
                parsedValue = Boolean.parseBoolean(value);
              }
              preparedStatement.setBoolean(sqlIndex, parsedValue);
              break;
            default:
              throw new RuntimeException("Unhandled column type: " + colType);
          }
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
