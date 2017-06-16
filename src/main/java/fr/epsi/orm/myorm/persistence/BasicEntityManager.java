package fr.epsi.orm.myorm.persistence;

import com.sun.deploy.util.StringUtils;
import fr.epsi.orm.myorm.annotation.Entity;
import fr.epsi.orm.myorm.annotation.Id;
import fr.epsi.orm.myorm.annotation.Transient;
import fr.epsi.orm.myorm.lib.NamedPreparedStatement;
import fr.epsi.orm.myorm.lib.ReflectionUtil;
import javaslang.Predicates;
import sun.reflect.Reflection;
import sun.rmi.runtime.Log;

import javax.sql.DataSource;
import javax.swing.text.html.Option;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static fr.epsi.orm.myorm.persistence.MappingHelper.*;
import static fr.epsi.orm.myorm.persistence.SqlGenerator.*;

/**
 * Created by fteychene on 14/05/17.
 */
public class BasicEntityManager implements EntityManager {

    private final DataSource datasource;
    private final Set<Class<?>> persistentClasses;


    private BasicEntityManager(DataSource aDataSource, Set<Class<?>> aPersistentClasses) {
        datasource = aDataSource;
        persistentClasses = aPersistentClasses;
    }

    /**
     * Check the Persistent classes to be managed by the EntityManager to have the minimal configuration.
     * <p>
     * Each class should respect the following rules :
     * - Class should be annotated with @Entity
     * - Class should have one and only one field with the @Id annotation
     *
     * @param persistentClasses
     * @throws IllegalArgumentException if a class does not match the conditions
     */
    private static void checkPersistentClasses(Set<Class<?>> persistentClasses) {
        persistentClasses.forEach(entity -> {
            ReflectionUtil.getAnnotationForClass(entity, Entity.class)
                    .orElseThrow(() -> new IllegalArgumentException("Illegal Class"));
            if(ReflectionUtil.getFieldsDeclaringAnnotation(entity, Id.class).count() != 1 ){
                throw new IllegalArgumentException("Illegal Class");
            }
        });
    }

    /**
     * Check id a Class is managed by this EntityManager
     *
     * @param checkClass
     */
    private void isManagedClass(Class<?> checkClass) {
        if (!persistentClasses.contains(checkClass)) {
            throw new IllegalArgumentException("The class " + checkClass.getName() + " is not managed by this EntityManager ...");
        }
    }

    /**
     * Create a BasicEntityManager and check the persistents classes
     *
     * @param dataSource        The Datasource to use for connecting to DB
     * @param persistentClasses The Set of Classes to be managed in this EntityManager
     * @return The BasicEntityManager created
     */
    public static BasicEntityManager create(DataSource dataSource, Set<Class<?>> persistentClasses) {
        checkPersistentClasses(persistentClasses);
        return new BasicEntityManager(dataSource, persistentClasses);
    }

    /**
     * @see EntityManager#find(Class, Object)
     */
    @Override
    public <T> Optional<T> find(Class<T> entityClass, Object id) {
        // if we manage
        isManagedClass(entityClass);
        // Request prepare and execute if @Id Annotation exist, SQL request generated with param HashMap
        return ReflectionUtil.getFieldDeclaringAnnotation(entityClass, Id.class).map((idField) -> {
            List<T> result = executeQuery(entityClass, SqlGenerator.generateSelectSql(entityClass, idField), new HashMap<String, Object>() {{
                put("id", id);
            }});
            // Return le result or null
            return result.isEmpty() ? null : result.get(0);
        });
    }

    /**
     * @see EntityManager#findAll(Class)
     */
    @Override
    public <T> List<T> findAll(Class<T> entityClass) {
        // if we manage
        isManagedClass(entityClass);
        try {
            // Request prepare, request build with SELECT * FROM + Name Of Table Of Entity + ;
            NamedPreparedStatement named = NamedPreparedStatement.prepare(datasource.getConnection(), "SELECT * FROM " + SqlGenerator.getTableForEntity(entityClass) + ";");
            // Conversion named to Array with MappingHelper
            return MappingHelper.mapFromResultSet(entityClass, named.executeQuery());
        }catch (SQLException se){
            System.out.println("Error findAll : " + se.getMessage());
            return new ArrayList<T>();
        }
    }

    /**
     * @see EntityManager#save(Object)
     */
    @Override
    public <T> Optional<T> save(T entity) {
        isManagedClass(entity.getClass());
        // Need construct a resquest INSERT INTO, with all fiel key and values separated

        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();

        // Use MappingHelper for get list of Key and Values
        // TODO : entityToParams, To finish : Predicates
        Map<String, Object> listKeysValues = MappingHelper.entityToParams(entity);

        keys.addAll(listKeysValues.keySet());
        for(Object o:listKeysValues.values()){
            values.add("'" +o.toString()+ "'");
        }

        // Generate Request
        // Step : generate keys request
        String keyString = "(id, " + String.join(", ", keys) + ")";
        // Step : generate values request
        String valueString = "(default, "  + String.join( ", ", values) + ")";

        // if the table exist
        if(!SqlGenerator.getTableForEntity(entity.getClass()).equals("")){
            String tableName = SqlGenerator.getTableForEntity(entity.getClass());

            String request = "INSERT INTO "+ tableName + " " + keyString + " VALUES " + valueString + ";";

            System.out.println("Log request "+request);
            try{
                NamedPreparedStatement named = NamedPreparedStatement.prepareWithKey(datasource.getConnection(), request);
                // Request execute for insert
                named.execute();
                // TODO : How to get Id ? -> getGeneratedKeys
                ResultSet resultKey = named.getGeneratedKeys();
                resultKey.next();
                // Get id, field
                Long id = resultKey.getLong(1);
                Field field = ReflectionUtil.getFieldByName(entity.getClass(), "id").get();
                // Set the new Value
                ReflectionUtil.setValue(field, entity,  id);
                return Optional.of(entity);
            }catch (Exception se){
                se.printStackTrace();
                System.out.println("Error insert : " + se.getMessage());
            }
        }else{
            throw new IllegalArgumentException("Illegal Class");
        }
        return Optional.empty();
    }

    private <T> List<T> executeQuery(Class<T> entityClass, String sql, Map<String, Object> parameters) {
        try {
            // Request prepare
            NamedPreparedStatement statement = NamedPreparedStatement.prepare(datasource.getConnection(), sql);
            // Request prepare parameters
            statement.setParameters(parameters);
            // Request execute for select
            ResultSet result = statement.executeQuery();
            // Conversion result to Array with MappingHelper
            return MappingHelper.mapFromResultSet(entityClass, result);
        } catch (SQLException se) {
            System.out.println("Error executeQuery : " + se.getMessage());
            return new ArrayList<T>();
        }
    }

    private <T> int executeUpdate(String sql, Map<String, Object> parameters) {
        try {
            // Request prepare
            NamedPreparedStatement statement = NamedPreparedStatement.prepare(datasource.getConnection(), sql);
            // Request prepare parameters
            statement.setParameters(parameters);
            // Request execute for update
            return statement.executeUpdate();
        } catch (SQLException se) {
            System.out.println("Error executeUpdate : " + se.getMessage());
            return -1;
        }
    }

    /**
     * @see EntityManager#delete(Object)
     */
    @Override
    public <T> boolean delete(T entity) {
        isManagedClass(entity.getClass());
        try {
            Field idField = ReflectionUtil.getFieldDeclaringAnnotation(entity.getClass(), Id.class).get();
            String sql = SqlGenerator.generateDeleteSql(entity.getClass());
            int affectedRows = executeUpdate(sql, new HashMap<String, Object>() {{
                put(idField.getName(), ReflectionUtil.getValue(idField, entity).get());
            }});
            return affectedRows > 0;
        } catch (Exception e) {
            System.out.println("Error delete : " + e.getMessage());
            return false;
        }
    }

}
