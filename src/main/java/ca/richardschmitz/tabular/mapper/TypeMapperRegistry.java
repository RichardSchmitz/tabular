package ca.richardschmitz.tabular.mapper;

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
      .register(new StringMapper());
  }

  private Map<Integer, List<TypeMapper>> registeredMappers = new HashMap<>();

  public void setValue(PreparedStatement statement, int sqlIndex, String rawValue, int type) throws SQLException {
    List<TypeMapper> mappers = registeredMappers.get(type);
    if (mappers == null || mappers.isEmpty()) {
      throw new RuntimeException("Unhandled column type: " + type);
    }

    for (TypeMapper mapper : mappers) {
      boolean successfullyMappedValue = mapper.setValue(statement, sqlIndex, rawValue);

      if (successfullyMappedValue) {
        return;
      }
    }

    throw new RuntimeException("Unhandled column type: " + type);
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
}
