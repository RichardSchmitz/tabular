package ca.richardschmitz.tabular.mapper;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;

public class DateMapper implements TypeMapper {
  @Override
  public boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException {
    try {
      statement.setDate(sqlIndex, Date.valueOf(LocalDate.parse(rawValue)));
      return true;
    } catch (DateTimeParseException ex) {
      return false;
    }
  }

  @Override
  public Integer[] getApplicableTypes() {
    return new Integer[] {Types.DATE};
  }
}
