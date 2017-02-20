package ca.richardschmitz.tabular.mapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;

public class TimeMapper implements TypeMapper {
  @Override
  public boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException {
    try {
      statement.setTime(sqlIndex, Time.valueOf(LocalTime.parse(rawValue)));
      return true;
    } catch (DateTimeParseException ex) {
      return false;
    }
  }

  @Override
  public Integer[] getApplicableTypes() {
    return new Integer[] {Types.TIME};
  }
}
