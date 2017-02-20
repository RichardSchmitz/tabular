package ca.richardschmitz.tabular.mapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface TypeMapper {
  /**
   *
   * @param statement
   * @param sqlIndex
   * @param rawValue
   * @return true if the rawValue was successfully mapped into the statement, false otherwise
   */
  boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException;

  Integer[] getApplicableTypes();
}
