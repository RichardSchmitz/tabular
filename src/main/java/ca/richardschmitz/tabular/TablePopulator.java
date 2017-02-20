package ca.richardschmitz.tabular;

import org.apache.commons.io.IOUtils;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.node.Node;
import org.commonmark.node.Text;
import org.commonmark.parser.Parser;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TablePopulator {
  private final DataSource dataSource;
  private final Parser parser;

  public TablePopulator(DataSource dataSource) {
    this.dataSource = dataSource;
    this.parser = Parser.builder().extensions(Arrays.asList(TablesExtension.create())).build();
  }

  public void populateTable(String tableName, InputStream markdown) throws SQLException, IOException {
    populateTable(tableName, IOUtils.toString(markdown));
  }

  public void populateTable(String tableName, String markdown) throws SQLException {
    Node document = parser.parse(markdown);

    try(Connection con = dataSource.getConnection()) {
      DatabaseMetaData metaData = con.getMetaData();
      TableDefinition tableDefinition = new TableDefinition(tableName);
      ResultSet rs = metaData.getColumns(null, null, tableName.toUpperCase(), null);
      int numColumns = 0;
      Map<String, Integer> columnType = new HashMap<>();
      while (rs.next()) {
        columnType.put(rs.getString("COLUMN_NAME").toUpperCase(), rs.getInt("DATA_TYPE"));
      }

      Node tableHead = document.getFirstChild().getFirstChild();

      Node columnHeader = tableHead.getFirstChild().getFirstChild();
      String columnName = ((Text) columnHeader.getFirstChild()).getLiteral().toUpperCase();
      tableDefinition.addColumn(numColumns++, columnName, columnType.get(columnName));

      while (columnHeader.getNext() != null) {
        columnHeader = columnHeader.getNext();
        columnName = ((Text) columnHeader.getFirstChild()).getLiteral().toUpperCase();
        tableDefinition.addColumn(numColumns++, columnName, columnType.get(columnName));
      }

      TableLoader tableLoader = new TableLoader(tableDefinition);
      Node tableBody = tableHead.getNext();

      Node row = tableBody.getFirstChild();
      while (row != null) {
        Map<Integer, String> columnValues = new HashMap<>();
        for (int i = 0; i < numColumns; i++) {
          TableDefinition.ColumnDefinition columnDefinition = tableDefinition.getColumns().get(i);
          Node cell = getNthChild(row, i);
          columnValues.put(i, ((Text) cell.getFirstChild()).getLiteral());
        }
        tableLoader.loadRow(con, columnValues);
        row = row.getNext();
      }

    }
  }

  private Node getNthChild(Node node, int n) {
    if (n == 0) {
      return node.getFirstChild();
    } else if (n > 0) {
      Node child = node.getFirstChild();
      for (int i = 0; i < n; i++) {
        child = child.getNext();
      }
      return child;

    } else {
      throw new RuntimeException("Child index must be non-negative. Got " + n);
    }
  }
}
