package fr.epsi.orm.myorm.lib;

import java.sql.*;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by fteychene on 16/05/17.
 */
public class NamedPreparedStatement {

    private PreparedStatement statement;
    private List<Parameter> parameters;

    private NamedPreparedStatement(PreparedStatement aStatement, List<Parameter> aParameters) {
        super();
        statement = aStatement;
        parameters = aParameters;
    }

    public static NamedPreparedStatement prepare(Connection connection, String sql, int generatedKeys) throws SQLException {
        return new NamedPreparedStatement(connection.prepareStatement(sql, generatedKeys), parseQuery(sql));
    }

    public static NamedPreparedStatement prepare(Connection connection, String sql) throws SQLException {
        return prepare(connection, sql, Statement.NO_GENERATED_KEYS);
    }

    public static NamedPreparedStatement prepareWithKey(Connection connection, String sql) throws SQLException {
        return prepare(connection, sql, Statement.RETURN_GENERATED_KEYS);
    }

    public void setParameter(final String parameterName, Object value) {
        try {
            Optional<Parameter> parameter = parameters.stream()
                    .filter((p) -> p.name.equals(parameterName)).findFirst();
            if (parameter.isPresent()) parameter.get().setParameter(statement, value);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Error when applying parameter to NamedPreparedStatement "+parameterName+" value "+value.toString(), e);
        }
    }

    public void setParameters(final Map<String, Object> parameters) {
        parameters.forEach(this::setParameter);
    }

    public static List<Parameter> parseQuery(String sql) {
        List<String> params = Arrays.stream(sql.split("[ ,=.()]")).filter((s) -> s.startsWith(":")).map((s) -> s.substring(1)).collect(Collectors.toList());
        return IntStream.range(0, params.size())
                .mapToObj((index) -> new Parameter(params.get(index), index+1))
                .collect(Collectors.toList());
    }

    public ResultSet executeQuery() throws SQLException {
        return statement.executeQuery();
    }

    public void execute() throws SQLException {
        statement.execute();
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return statement.getGeneratedKeys();
    }

    public int executeUpdate() throws SQLException {
        return statement.executeUpdate();
    }


    static class Parameter {
        public final String name;
        public final Integer index;

        public Parameter(String aName, Integer aIndex) {
            name = aName;
            index = aIndex;
        }

        public void setParameter(PreparedStatement statement, Object value) throws SQLException {
            if (value == null) {
                statement.setNull(index, Types.INTEGER);
            } else if (value.getClass().equals(String.class)) {
                statement.setString(index, (String) value);
            } else if (value.getClass().equals(Integer.class)) {
                statement.setInt(index, (Integer) value);
            } else if (value.getClass().equals(Long.class)) {
                statement.setLong(index, (Long) value);
            } else if (value.getClass().equals(LocalDate.class)) {
                statement.setDate(index, Date.valueOf((LocalDate) value));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Parameter parameter = (Parameter) o;

            if (name != null ? !name.equals(parameter.name) : parameter.name != null) return false;
            return index != null ? index.equals(parameter.index) : parameter.index == null;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (index != null ? index.hashCode() : 0);
            return result;
        }
    }
}
