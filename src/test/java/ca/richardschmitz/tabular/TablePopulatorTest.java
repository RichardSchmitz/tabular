package ca.richardschmitz.tabular;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TablePopulatorTest {
  private static final String COL_FIRST_NAME = "first_name";
  private static final String COL_LAST_NAME = "last_name";
  private static final String COL_AGE = "age";
  private static final String COL_OCCUPATION = "occupation";
  private static final String COL_DEGREE = "has_degree";

  private static final String COL_HOURS = "hours";
  private static final String COL_MINUTES = "minutes";
  private static final String COL_SECONDS = "seconds";
  private static final String COL_MILLIS = "milliseconds";

  private JdbcConnectionPool dataSource;
  private DBI dbi;
  private TablePopulator tablePopulator;

  @Before
  public void setUp() {
    dataSource = JdbcConnectionPool.create("jdbc:h2:mem:test", "sa", "sa");
    dbi = new DBI(dataSource);
    try (Handle handle = dbi.open()) {
      handle.execute("CREATE SCHEMA myschema");
    }
    tablePopulator = new TablePopulator(dataSource);
  }

  @After
  public void tearDown() {
    dataSource.dispose();
  }

  @Test
  public void testBasicTable() throws Exception {
    InputStream inputStream = getInput("people.md");
    try (Handle handle = dbi.open()) {
      handle.execute("CREATE TABLE myschema.people (" +
        "  first_name VARCHAR," +
        "  last_name VARCHAR," +
        "  age INT," +
        "  occupation VARCHAR," +
        "  has_degree BOOLEAN" +
        ")");

      tablePopulator.populateTable("myschema", "people", inputStream);

      List<Map<String, Object>> rows = handle.createQuery("select * from myschema.people order by last_name").list();
      assertThat(rows).size().isEqualTo(3);

      Map<String, Object> firstRow = rows.get(0);
      assertThat(firstRow)
        .containsEntry(COL_FIRST_NAME, "Rodney")
        .containsEntry(COL_LAST_NAME, "Barbosa")
        .containsEntry(COL_AGE, 33)
        .containsEntry(COL_OCCUPATION, "Engineer")
        .containsEntry(COL_DEGREE, true);

      Map<String, Object> secondRow = rows.get(1);
      assertThat(secondRow)
        .containsEntry(COL_FIRST_NAME, "Celia")
        .containsEntry(COL_LAST_NAME, "Cabbage")
        .containsEntry(COL_AGE, 29)
        .containsEntry(COL_OCCUPATION, "Journalist")
        .containsEntry(COL_DEGREE, true);

      Map<String, Object> thirdRow = rows.get(2);
      assertThat(thirdRow)
        .containsEntry(COL_FIRST_NAME, "Henry")
        .containsEntry(COL_LAST_NAME, "Dozer")
        .containsEntry(COL_AGE, 17)
        .containsEntry(COL_OCCUPATION, null)
        .containsEntry(COL_DEGREE, false);
    }
  }

  @Test
  public void testNumericTable() throws Exception {
    InputStream inputStream = getInput("durations.md");
    try (Handle handle = dbi.open()) {
      handle.execute("CREATE TABLE myschema.durations (" +
        "  hours TINYINT," +
        "  minutes SMALLINT," +
        "  SECONDS INT," +
        "  MILLISECONDS BIGINT" +
        ")");

      tablePopulator.populateTable("myschema", "durations", inputStream);

      List<Map<String, Object>> rows = handle.createQuery("select * from myschema.durations order by milliseconds").list();
      assertThat(rows).size().isEqualTo(1);

      Map<String, Object> firstRow = rows.get(0);
      assertThat(firstRow)
        .containsEntry(COL_HOURS, (byte) 1)
        .containsEntry(COL_MINUTES, (short) 60)
        .containsEntry(COL_SECONDS, 3600)
        .containsEntry(COL_MILLIS, 3600000L);
    }
  }

  private InputStream getInput(String filename) {
    return getClass().getResourceAsStream(filename);
  }
}
