package ca.richardschmitz.tabular.mapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class LongMapper implements TypeMapper {
  @Override
  public boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException {
    try {
      statement.setLong(sqlIndex, Long.parseLong(rawValue));
      return true;
    } catch (NumberFormatException ex) {
      return false;
    }
  }

  @Override
  public Integer[] getApplicableTypes() {
    return new Integer[] {Types.BIGINT};
  }
}
