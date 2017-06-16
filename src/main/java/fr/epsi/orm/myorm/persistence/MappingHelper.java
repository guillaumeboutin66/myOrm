package fr.epsi.orm.myorm.persistence;

import fr.epsi.orm.myorm.annotation.Id;
import fr.epsi.orm.myorm.annotation.Transient;
import fr.epsi.orm.myorm.lib.ReflectionUtil;
import javaslang.Predicates;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by fteychene on 14/05/17.
 */
public class MappingHelper {

    public static Object getFromResultSetByType(Class<?> type, ResultSet rs, String columnName) throws SQLException {
        if (type.equals(String.class)) {
            return rs.getString(columnName);
        } else if (type.equals(Integer.class)) {
            return rs.getInt(columnName);
        } else if (type.equals(Long.class)) {
            return rs.getLong(columnName);
        } else if (type.equals(LocalDate.class)) {
            return rs.getDate(columnName).toLocalDate();
        }
        return null;
    }


    public static <T> T mapToInstance(ResultSet rs, Class<T> entityClass) throws SQLException {
        T instance = ReflectionUtil.instanciate(entityClass).get();
        List<Field> fields = ReflectionUtil.getFieldsWithoutTransient(instance.getClass()).collect(Collectors.toList());
        for (Field field : fields) {
            ReflectionUtil.setValue(field, instance, MappingHelper.getFromResultSetByType(field.getType(), rs, SqlGenerator.getColumnNameForField(field)));
        }
        return instance;
    }

    public static <T> List<T> mapFromResultSet(Class<? extends T> entityClass, ResultSet rs) throws SQLException {
        List<T> result = new ArrayList<>();
        while (rs.next()) {
            result.add(mapToInstance(rs, entityClass));
        }
        return result;
    }

    // TODO : use predicate
    public static Map<String, Object> entityToParams(Object entity, Predicate<Field>... predicates) {
        Map res = new HashMap<>();
        // get Fields
        Object[] fields = ReflectionUtil.getFields(entity.getClass()).toArray();
        for (Object f: fields) {
            // if field and value exist and field is not the Id field
            if(!ReflectionUtil.getAnnotationForField((Field) f, Transient.class).isPresent() && ReflectionUtil.getValue((Field) f, entity).isPresent() && !SqlGenerator.getColumnNameForField((Field) f).equals("id")){
                // Add in Map
                res.put(SqlGenerator.getColumnNameForField((Field) f),ReflectionUtil.getValue((Field) f, entity).get());
            }
        }
        return res;
    }

}
