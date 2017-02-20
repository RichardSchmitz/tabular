package ca.richardschmitz.tabular.mapper;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class BinaryMapper implements TypeMapper {
  @Override
  public boolean setValue(PreparedStatement statement, int sqlIndex, String rawValue) throws SQLException {
    try {
      String strToDecode = rawValue;
      if (rawValue.substring(0, 2).equalsIgnoreCase("0x")) {
        strToDecode = rawValue.substring(2, rawValue.length());
      }
      statement.setBytes(sqlIndex, Hex.decodeHex(strToDecode.toCharArray()));
      return true;
    } catch (DecoderException ex) {
      return false;
    }
  }

  @Override
  public Integer[] getApplicableTypes() {
    return new Integer[] {Types.VARBINARY};
  }
}
