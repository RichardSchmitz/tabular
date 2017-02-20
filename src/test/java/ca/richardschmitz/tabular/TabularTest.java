package ca.richardschmitz.tabular;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TabularTest {
  private static final String COL_FIRST_NAME = "first_name";
  private static final String COL_LAST_NAME = "last_name";
  private static final String COL_AGE = "age";
  private static final String COL_OCCUPATION = "occupation";
  private static final String COL_DEGREE = "has_degree";

  private static final String COL_HOURS = "hours";
  private static final String COL_MINUTES = "minutes";
  private static final String COL_SECONDS = "seconds";
  private static final String COL_MILLIS = "milliseconds";

  private static final String COL_METERS = "meters";
  private static final String COL_CMS = "cms";
  private static final String COL_INCHES = "inches";

  private static final String COL_BINARY = "binary_data";

  private static final String COL_DATE = "local_date";
  private static final String COL_TIME = "local_time";
  private static final String COL_TIMESTAMP = "timestamp";

  private JdbcConnectionPool dataSource;
  private DBI dbi;
  private Tabular tabular;

  @Before
  public void setUp() {
    dataSource = JdbcConnectionPool.create("jdbc:h2:mem:test", "sa", "sa");
    dbi = new DBI(dataSource);
    try (Handle handle = dbi.open()) {
      handle.execute("CREATE SCHEMA myschema");
    }
    tabular = new Tabular(dataSource);
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

      tabular.populateTable("myschema", "people", inputStream);

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

      tabular.populateTable("myschema", "durations", inputStream);

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

  @Test
  public void testDecimalTable() throws Exception {
    InputStream inputStream = getInput("distances.md");
    try (Handle handle = dbi.open()) {
      handle.execute("CREATE TABLE myschema.distances (" +
        "  meters REAL," +
        "  cms DOUBLE," +
        "  inches NUMERIC" +
        ")");

      tabular.populateTable("myschema", "distances", inputStream);

      List<Map<String, Object>> rows = handle.createQuery("select * from myschema.distances order by meters").list();
      assertThat(rows).size().isEqualTo(1);

      Map<String, Object> firstRow = rows.get(0);
      assertThat(firstRow)
        .containsEntry(COL_METERS, (float) 1.5437)
        .containsEntry(COL_CMS, 154.37d)
        .containsEntry(COL_INCHES, new BigDecimal("60.7755906"));
    }
  }

  @Test
  public void testBinaryTable() throws Exception {
    InputStream inputStream = getInput("binary.md");

    try (Handle handle = dbi.open()) {
      handle.execute("CREATE TABLE myschema.binary (" +
        "  binary_data VARBINARY" +
        ")");

      tabular.populateTable("myschema", "binary", inputStream);

      List<Map<String, Object>> rows = handle.createQuery("select * from myschema.binary order by binary_data").list();
      assertThat(rows).size().isEqualTo(3);

      Map<String, Object> firstRow = rows.get(0);
      assertThat(firstRow)
        .containsEntry(COL_BINARY, new byte[] {0x00, 0x01, 0x02, 0x03});

      Map<String, Object> secondRow = rows.get(1);
      assertThat(secondRow)
        .containsEntry(COL_BINARY, new byte[] {0x00, (byte) 0xFF, 0x00});

      Map<String, Object> thirdRow = rows.get(2);
      assertThat(thirdRow)
        .containsEntry(COL_BINARY, new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE});
    }
  }

  @Test
  public void testTimeTable() throws Exception {
    InputStream inputStream = getInput("times.md");

    try (Handle handle = dbi.open()) {
      handle.execute("CREATE TABLE myschema.times (" +
        "  local_date DATE," +
        "  local_time TIME," +
        "  timestamp TIMESTAMP" +
        ")");

      tabular.populateTable("myschema", "times", inputStream);

      List<Map<String, Object>> rows = handle.createQuery("select * from myschema.times order by timestamp").list();
      assertThat(rows).size().isEqualTo(1);

      Map<String, Object> firstRow = rows.get(0);
      assertThat(firstRow)
        .containsEntry(COL_DATE, Date.valueOf(LocalDate.parse("2017-02-20")))
        .containsEntry(COL_TIME, Time.valueOf(LocalTime.parse("15:31:20")))
        .containsEntry(COL_TIMESTAMP, Timestamp.from(ZonedDateTime.parse("2017-02-20T15:31:20-08:00").toInstant()));
    }
  }

  private InputStream getInput(String filename) {
    return getClass().getResourceAsStream(filename);
  }
}
