package ca.richardschmitz.tabular.mapper;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TypeMapperRegistry {
  public static TypeMapperRegistry getDefault() {
    return new TypeMapperRegistry()
      .register(new BooleanMapper())
      .register(new ByteMapper())
      .register(new ShortMapper())
      .register(new IntMapper())
      .register(new LongMapper())
      .register(new FloatMapper())
      .register(new DoubleMapper())
      .register(new BigDecimalMapper())
      .register(new StringMapper())
      .register(new BinaryMapper());
  }

  private Map<Integer, List<TypeMapper>> registeredMappers = new HashMap<>();

  public void setValue(PreparedStatement statement, int sqlIndex, String rawValue, int type) throws SQLException {
    List<TypeMapper> mappers = registeredMappers.get(type);
    if (mappers == null || mappers.isEmpty()) {
      abort(type, rawValue);
    }

    for (TypeMapper mapper : mappers) {
      boolean successfullyMappedValue = mapper.setValue(statement, sqlIndex, rawValue);

      if (successfullyMappedValue) {
        return;
      }
    }

    abort(type, rawValue);
  }

  public TypeMapperRegistry register(TypeMapper mapper) {
    Integer[] applicableTypes = mapper.getApplicableTypes();

    for (int type : Arrays.asList(applicableTypes)) {
      List<TypeMapper> mappers = registeredMappers.get(type);
      if (mappers == null) {
        registeredMappers.put(type, new ArrayList<>());
        mappers = registeredMappers.get(type);
      }

      mappers.add(mapper);
    }

    return this;
  }

  private void abort(int type, String rawValue) {
    throw new RuntimeException(String.format("No TypeMappers could handle converting '%s' to %s", rawValue, JDBCType.valueOf(type)));
  }
}
