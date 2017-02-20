package ca.richardschmitz.tabular.mapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class BooleanMapper implements TypeMapper {
  @Override
  public boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException {
    boolean parsedValue;
    if ("Y".equals(rawValue) || "T".equals(rawValue)) {
      parsedValue = true;
    } else if ("N".equals(rawValue) || "F".equals(rawValue)) {
      parsedValue = false;
    } else {
      parsedValue = Boolean.parseBoolean(rawValue);
    }
    statement.setBoolean(sqlIndex, parsedValue);

    return true;
  }

  @Override
  public Integer[] getApplicableTypes() {
    return new Integer[] {Types.BOOLEAN};
  }
}
