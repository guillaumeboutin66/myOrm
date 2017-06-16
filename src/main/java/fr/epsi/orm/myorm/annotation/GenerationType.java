package fr.epsi.orm.myorm.annotation;

/**
 * Created by fteychene on 14/05/17.
 *
 * Defines the types of primary key generation strategies.
 */
public enum GenerationType {

    /**
     * Indicates that the persistence provider must assign
     * primary keys for the entity using a database identity column.
     */
    IDENTITY,
    /**
     * Indicate that the application must assign primary keys.
     */
    NONE;
}
