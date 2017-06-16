package fr.epsi.orm.myorm.persistence;

import java.util.List;
import java.util.Optional;

/**
 * Created by fteychene on 14/05/17.
 */

public interface EntityManager {

    /**
     * Find a entity in the database with its id
     * @param entityClass the Class of the Entity to search in the database
     * @param id the value of the searched Id
     * @param <T> The entity class (redudant with entityClass but can't access to generics type due to type erasure)
     * @return an Optional with the value from the database as an instance of entityClass
     * @throws IllegalArgumentException if entityClass is not managed by the entity manager
     */
    <T> Optional<T> find(Class<T> entityClass, Object id);

    /**
     * Return all the databases rows as entity for an entity class
     * @param entityClass the Class of the Entity to search in the database
     * @param <T> The entity class (redudant with entityClass but can't access to generics type due to type erasure)
     * @return a List with th rows from database empty List if there is none
     * @throws IllegalArgumentException if entityClass is not managed by the entity manager
     */
    <T> List<T> findAll(Class<T> entityClass);

    /**
     * Save an entity in the database
     * @param entity the entity to save in database
     * @param <T> The entity class (redudant with entityClass but can't access to generics type due to type erasure)
     * @return an Optional with the persisted entity (with generated id if needed), Optional.empty() if the save has failed
     * @throws IllegalArgumentException if entityClass is not managed by the entity manager
     */
    <T> Optional<T> save(T entity);

    /**
     * Delete an entity from the database
     * @param entity the entity to delete in database
     * @param <T> The entity class (redudant with entityClass but can't access to generics type due to type erasure)
     * @return true if a row have been deleted during the request, false instead
     * @throws IllegalArgumentException if entityClass is not managed by the entity manager
     */
    <T> boolean delete(T entity);

}
