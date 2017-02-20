package ca.richardschmitz.tabular.mapper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class BigDecimalMapper implements TypeMapper {
  @Override
  public boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException {
    try {
      statement.setBigDecimal(sqlIndex, new BigDecimal(rawValue));
      return true;
    } catch (NumberFormatException ex) {
      return false;
    }
  }

  @Override
  public Integer[] getApplicableTypes() {
    return new Integer[] {Types.NUMERIC, Types.DECIMAL};
  }
}
