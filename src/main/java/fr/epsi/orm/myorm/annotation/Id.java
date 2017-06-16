package fr.epsi.orm.myorm.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by fteychene on 14/05/17.
 *
 * Specifies the primary key of an entity.
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Id {

    /**
     * (Optional) The primary key generation strategy, default to None
     * @return The generation strategy
     */
    GenerationType generation() default GenerationType.NONE;
}
