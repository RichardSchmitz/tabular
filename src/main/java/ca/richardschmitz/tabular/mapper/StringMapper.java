package ca.richardschmitz.tabular.mapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class StringMapper implements TypeMapper {
  @Override
  public boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException {
    statement.setString(sqlIndex, rawValue);
    return true;
  }

  @Override
  public Integer[] getApplicableTypes() {
    return new Integer[] {Types.VARCHAR};
  }
}
