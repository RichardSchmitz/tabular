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

public class Tabular {
  private final DataSource dataSource;
  private final Parser parser;

  public Tabular(DataSource dataSource) {
    this.dataSource = dataSource;
    this.parser = Parser.builder().extensions(Arrays.asList(TablesExtension.create())).build();
  }

  public void populateTable(String schema, String tableName, InputStream markdown) throws SQLException, IOException {
    populateTable(schema, tableName, IOUtils.toString(markdown));
  }

  public void populateTable(String schema, String tableName, String markdown) throws SQLException {
    Node document = parser.parse(markdown);
    Node tableHead = document.getFirstChild().getFirstChild();

    try(Connection con = dataSource.getConnection()) {
      TableDefinition tableDefinition = createTableDefinition(con, schema, tableName, tableHead);

      TableLoader tableLoader = new TableLoader(tableDefinition);
      Node tableBody = tableHead.getNext();

      Node row = tableBody.getFirstChild();
      while (row != null) {
        Map<Integer, String> columnValues = new HashMap<>();
        for (int i = 0; i < tableDefinition.getColumns().size(); i++) {
          Node cell = getNthChild(row, i);
          String value = "";
          Text textNode = (Text) cell.getFirstChild();
          if (textNode != null) {
            value = textNode.getLiteral();
          }
          columnValues.put(i, value);
        }
        tableLoader.loadRow(con, columnValues);
        row = row.getNext();
      }

    }
  }

  private TableDefinition createTableDefinition(Connection con, String schema, String tableName, Node tableHead) throws SQLException {
    TableDefinition tableDefinition = new TableDefinition(schema, tableName);
    DatabaseMetaData metaData = con.getMetaData();
    ResultSet rs = metaData.getColumns(null, schema.toUpperCase(), tableName.toUpperCase(), null);
    int numColumns = 0;
    Map<String, Integer> columnType = new HashMap<>();
    while (rs.next()) {
      columnType.put(rs.getString("COLUMN_NAME").toUpperCase(), rs.getInt("DATA_TYPE"));
    }

    Node columnHeader = tableHead.getFirstChild().getFirstChild();

    while (columnHeader != null) {
      String columnName = ((Text) columnHeader.getFirstChild()).getLiteral().toUpperCase();
      Integer colType = columnType.get(columnName);
      if (colType == null) {
        throw new RuntimeException(String.format("Column '%s' was found in the document but not in the database.", columnName));
      }
      tableDefinition.addColumn(numColumns++, columnName, colType);

      columnHeader = columnHeader.getNext();
    }

    return tableDefinition;
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
