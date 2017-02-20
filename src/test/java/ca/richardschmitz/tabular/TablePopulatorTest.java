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
  private static final String COL_FIRST_NAME = "first_name";
  private static final String COL_LAST_NAME = "last_name";
  private static final String COL_AGE = "age";
  private static final String COL_OCCUPATION = "occupation";

  @Test
  public void testPopulateTable() throws Exception {
    InputStream inputStream = getClass().getResourceAsStream("people.md");
    DataSource dataSource = JdbcConnectionPool.create("jdbc:h2:mem:test", "sa", "sa");
    DBI dbi = new DBI(dataSource);

    TablePopulator tablePopulator = new TablePopulator(dataSource);

    try (Handle handle = dbi.open()) {
      handle.execute("CREATE SCHEMA myschema");
      handle.execute("CREATE TABLE myschema.people (first_name VARCHAR, last_name VARCHAR, age INT, occupation VARCHAR)");
      tablePopulator.populateTable("myschema", "people", inputStream);

      List<Map<String, Object>> rows = handle.createQuery("select * from myschema.people order by last_name").list();
      assertThat(rows).size().isEqualTo(3);

      Map<String, Object> firstRow = rows.get(0);
      assertThat(firstRow)
        .containsEntry(COL_FIRST_NAME, "Rodney")
        .containsEntry(COL_LAST_NAME, "Barbosa")
        .containsEntry(COL_AGE, 33)
        .containsEntry(COL_OCCUPATION, "Engineer");

      Map<String, Object> secondRow = rows.get(1);
      assertThat(secondRow)
        .containsEntry(COL_FIRST_NAME, "Celia")
        .containsEntry(COL_LAST_NAME, "Cabbage")
        .containsEntry(COL_AGE, 29)
        .containsEntry(COL_OCCUPATION, "Journalist");

      Map<String, Object> thirdRow = rows.get(2);
      assertThat(thirdRow)
        .containsEntry(COL_FIRST_NAME, "Henry")
        .containsEntry(COL_LAST_NAME, "Dozer")
        .containsEntry(COL_AGE, 17)
        .containsEntry(COL_OCCUPATION, null);
    }
  }
}
