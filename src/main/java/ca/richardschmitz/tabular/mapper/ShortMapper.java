package ca.richardschmitz.tabular.mapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class ShortMapper implements TypeMapper {
  @Override
  public boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException {
    try {
      statement.setShort(sqlIndex, Short.parseShort(rawValue));
      return true;
    } catch (NumberFormatException ex) {
      return false;
    }
  }

  @Override
  public Integer[] getApplicableTypes() {
    return new Integer[] {Types.SMALLINT};
  }
}
