package ca.richardschmitz.tabular;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.sql.DataSource;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TablePopulatorTest {
  @Test
  public void testPopulateTable() throws Exception {
    InputStream inputStream = getClass().getResourceAsStream("people.md");
    DataSource dataSource = JdbcConnectionPool.create("jdbc:h2:mem:test", "sa", "sa");
    DBI dbi = new DBI(dataSource);

    TablePopulator tablePopulator = new TablePopulator(dataSource);

    try (Handle handle = dbi.open()) {
      handle.execute("CREATE TABLE people (first_name VARCHAR, last_name VARCHAR, age INT, occupation VARCHAR)");
      tablePopulator.populateTable("people", inputStream);

      List<Map<String, Object>> rows = handle.createQuery("select * from people order by last_name").list();
      assertThat(rows).size().isEqualTo(2);

      Map<String, Object> firstRow = rows.get(0);
      assertThat(firstRow)
        .containsEntry("first_name", "Rodney")
        .containsEntry("last_name", "Barbosa")
        .containsEntry("age", 33)
        .containsEntry("occupation", "Engineer");

      Map<String, Object> secondRow = rows.get(1);
      assertThat(secondRow)
        .containsEntry("first_name", "Celia")
        .containsEntry("last_name", "Cabbage")
        .containsEntry("age", 29)
        .containsEntry("occupation", "Journalist");
    }
  }
}
