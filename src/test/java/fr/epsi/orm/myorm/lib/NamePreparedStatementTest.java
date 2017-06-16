package fr.epsi.orm.myorm.lib;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static fr.epsi.orm.myorm.lib.NamedPreparedStatement.*;
import static org.junit.Assert.*;

/**
 * Created by fteychene on 16/05/17.
 */
public class NamePreparedStatementTest {

    @Test
    public void testParseQuery_oneParam() {
        List<Parameter> expected = Stream.of(new Parameter("it", 1)).collect(Collectors.toList());
        assertEquals(expected, NamedPreparedStatement.parseQuery("SELECT * from test where test = :it"));
    }

    @Test
    public void testParseQuery_noParam() {
        List<Parameter> expected = new ArrayList<>();
        assertEquals(expected, NamedPreparedStatement.parseQuery("SELECT * from test where test = 1"));
        assertEquals(expected, NamedPreparedStatement.parseQuery("SELECT * from test"));
    }

    @Test
    public void testParseQuery_multipleParams() {
        List<Parameter> expected = Stream.of(
                new Parameter("1", 1),
                new Parameter("2", 2),
                new Parameter("3", 3))
                .collect(Collectors.toList());
        assertEquals(expected, NamedPreparedStatement.parseQuery("SELECT * from test where test = :1 AND test = :2 and test = :3"));
    }

    @Test
    public void testParseQuery_testDelimiters() {
        List<Parameter> expected = Stream.of(new Parameter("it", 1)).collect(Collectors.toList());
        assertEquals(expected, NamedPreparedStatement.parseQuery("SELECT * from test where test = :it"));
        assertEquals(expected, NamedPreparedStatement.parseQuery("SELECT * from test where test =:it"));
        assertEquals(expected, NamedPreparedStatement.parseQuery("SELECT * from test a where test=a.:it"));
        assertEquals(expected, NamedPreparedStatement.parseQuery("INSERT INTO test VALUES(:it)"));
    }
}
