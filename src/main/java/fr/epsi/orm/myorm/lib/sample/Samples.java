package fr.epsi.orm.myorm.lib.sample;

import com.zaxxer.hikari.HikariDataSource;
import fr.epsi.orm.myorm.annotation.Column;
import fr.epsi.orm.myorm.annotation.Entity;
import fr.epsi.orm.myorm.annotation.Id;
import fr.epsi.orm.myorm.lib.ReflectionUtil;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by fteychene on 14/05/17.
 */
public class Samples {

private static final String CREATE_TEST_TABLE =
        "CREATE TABLE test (" +
        "  id BIGINT IDENTITY PRIMARY KEY," +
        "  name VARCHAR(30)," +
        "  value INTEGER" +
        ")";

    public static void createDatasourceAndExecuteQuery() throws SQLException {
        System.out.println("############################################");
        System.out.println("Creating datasource and query database");
        System.out.println("############################################");
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:hsqldb:mem:mymemdb");
        ds.setUsername("sa");
        ds.setLogWriter(new PrintWriter(System.out));

        if (ds.getConnection().prepareStatement(CREATE_TEST_TABLE).execute()) {
            throw new SQLException("Error while creating table test");
        }

        ds.getConnection().prepareStatement("INSERT INTO test(name, value) VALUES('first', 1)").execute();

        ResultSet rs = ds.getConnection().prepareStatement("SELECT * FROM test").executeQuery();
        while (rs.next()) {
            System.out.println("Value in test table : "+rs.getString("name") +" = "+ rs.getInt("value"));
        }
        System.out.println("############################################");
    }

    public static void readAnnotationAtRuntime() {
        System.out.println("############################################");
        System.out.println("Read annotations at runtime");
        System.out.println("############################################");

        Class<?> userClass = new User().getClass();

        System.out.println("Detect fields by reflection for "+userClass.getName());
        ReflectionUtil.getFields(userClass)
                .forEach((f) -> System.out.println("Has field "+f.getName()));

        System.out.println("Search for annotation for class "+userClass.getName());
        ReflectionUtil.getAnnotationForClass(userClass, Entity.class)
                .ifPresent((a) -> System.out.println("Is annotated with "+ a.toString()));

        System.out.println("Search for annotation on fields for "+userClass.getName());
        ReflectionUtil.getFieldDeclaringAnnotation(userClass, Id.class)
                .ifPresent((f) -> System.out.println("Is annotated by "+Id.class.getName()+" on field "+f.getName()));
        ReflectionUtil.getFieldsDeclaringAnnotation(userClass, Column.class)
                .forEach((f) -> System.out.println("Is annotated by "+Id.class.getName()+" on field "+f.getName()));

        System.out.println("############################################");
    }

    public static void changeValuesByReflection() {
        System.out.println("############################################");
        System.out.println("Change values by reflection");
        System.out.println("############################################");

        System.out.println("Instanciate by reflection");
        User user = ReflectionUtil.instanciate(User.class).get();
        user.setId(1l);
        user.setFirstName("Linus");
        user.setLastName("Torvald");
        user.setEmail("linus.torvald@linux.org");
        user.setConnectionCount(0);

        ReflectionUtil.getFields(user.getClass())
                .forEach((f) -> System.out.println(("Field detected "+f.getName()+" value : "+ ReflectionUtil.getValue(f, user).orElse("Error accessing field"))));

        System.out.println("Writing to user Johnny in firstName property");
        Field firstNameField = ReflectionUtil.getFieldByName(User.class, "firstName").get();
        ReflectionUtil.setValue(firstNameField, user, "Johnny");
        System.out.println("Writing to user 5 in connectionCount property");
        Field connectionCountField = ReflectionUtil.getFieldByName(User.class, "connectionCount").get();
        ReflectionUtil.setValue(connectionCountField, user, 5);

        System.out.println(user.toString());
        System.out.println("############################################");
    }



    public static void main(String[] args) throws SQLException {
        createDatasourceAndExecuteQuery();
        for (int i =0; i<3; i++) System.out.println();
        readAnnotationAtRuntime();
        for (int i =0; i<3; i++) System.out.println();
        changeValuesByReflection();
    }
}
