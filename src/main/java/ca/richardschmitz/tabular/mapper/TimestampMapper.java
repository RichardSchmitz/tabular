package ca.richardschmitz.tabular.mapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

public class TimestampMapper implements TypeMapper {
  @Override
  public boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException {
    try {
      statement.setTimestamp(sqlIndex, Timestamp.from(ZonedDateTime.parse(rawValue).toInstant()));
      return true;
    } catch (DateTimeParseException ex) {
      return false;
    }
  }

  @Override
  public Integer[] getApplicableTypes() {
    return new Integer[] {Types.TIMESTAMP};
  }
}
