package ca.richardschmitz.tabular;

import org.apache.commons.io.IOUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

public class TablePopulator {
  private final DataSource dataSource;

  public TablePopulator(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void populateTable(String tableName, InputStream markdown) throws SQLException, IOException {
    populateTable(tableName, IOUtils.toString(markdown));
  }

  public void populateTable(String tableName, String markdown) throws SQLException {
    try(Connection con = dataSource.getConnection()) {

    }
  }
}
