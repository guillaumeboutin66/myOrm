package fr.epsi.orm.myorm.lib;

import fr.epsi.orm.myorm.annotation.GenerationType;
import fr.epsi.orm.myorm.annotation.Id;
import fr.epsi.orm.myorm.annotation.Transient;
import javaslang.Predicates;

import javax.swing.text.html.Option;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by fteychene on 14/05/17.
 */
public final class ReflectionUtil {

    public static <T extends Annotation> Optional<T> getAnnotationForClass(Class<?> entityClass, Class<T> annotation) {
        if (!entityClass.isAnnotationPresent(annotation)) {
            return Optional.empty();
        }
        return Optional.of(entityClass.getAnnotation(annotation));
    }

    public static <T extends Annotation> Optional<T> getAnnotationForField(Field field, Class<T> annotation) {
        if (!field.isAnnotationPresent(annotation)) {
            return Optional.empty();
        }
        return Optional.of(field.getAnnotation(annotation));
    }

    @SafeVarargs
    public static Stream<Field> getFields(Class<?> entityClass, Predicate<Field>... filters) {
        return Stream.of(entityClass.getDeclaredFields()).filter(Predicates.allOf(filters));
    }
    
    public static Stream<Field> getFieldsWithoutTransient(Class<?> entityClass) {
        return getFields(entityClass, f -> !getAnnotationForField(f, Transient.class).isPresent());
    }

    public static Optional<Field> getFieldByName(Class<?> entityClass, String fieldName) {
        return getFields(entityClass, f -> f.getName().equals(fieldName)).findFirst();
    }

    public static <T extends Annotation> Stream<Field> getFieldsDeclaringAnnotation(Class<?> entityClass, Class<T> annotation) {
        return getFields(entityClass, field -> field.isAnnotationPresent(annotation));
    }

    public static <T extends Annotation> Optional<Field> getFieldDeclaringAnnotation(Class<?> entityClass, Class<T> annotation) {
        return getFieldsDeclaringAnnotation(entityClass, annotation).findFirst();
    }

    public static <T> Optional<T> getByDirectAccessValue(Field field, Object instance) {
        try {
            return Optional.of((T) field.get(instance));
        } catch (IllegalAccessException e) {
            return Optional.empty();
        }
    }

    private static Optional<PropertyDescriptor> getPropertyDescriptorForField(Class<?> entityClass, Field field) {
        try {
            return Stream.of(Introspector.getBeanInfo(entityClass).getPropertyDescriptors())
                    .filter(pd -> pd.getName().equals(field.getName()) && !"class".equals(pd.getName()))
                    .findFirst();
        } catch (IntrospectionException e) {
            return Optional.empty();
        }
    }

    private static Optional<Method> getGetterMethod(Field field, Object instance) {
        return getPropertyDescriptorForField(instance.getClass(), field)
                .map((pd) -> pd.getReadMethod() != null ? pd.getReadMethod(): null);
    }

    private static Optional<Method> getSetterMethod(Field field, Object instance) {
        return getPropertyDescriptorForField(instance.getClass(), field)
                .map((pd) -> pd.getWriteMethod() != null ? pd.getWriteMethod(): null);
    }

    public static <T> Optional<T> getValue(Field field, Object instance) {
        Optional<T> getterAccess = getGetterMethod(field, instance).flatMap((method) -> {
            try {
                return Optional.ofNullable((T) method.invoke(instance));
            } catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
                e.printStackTrace();
                return Optional.empty();
            }
        });
        return getterAccess.isPresent() ? getterAccess : getByDirectAccessValue(field, instance);
    }

    public static void setValue(Field field, Object instance, final Object newValue) {
        getSetterMethod(field, instance).ifPresent((method) -> {
            try {
                method.invoke(instance, newValue);
            } catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
                e.printStackTrace();
            }
        });
    }

    public static <T> Optional<T> instanciate(Class<? extends T> entityClass) {
        try {
            return Optional.of(entityClass.newInstance());
        } catch (IllegalAccessException|InstantiationException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    public static boolean isFieldIdGenerated(Field field) {
        return getAnnotationForField(field, Id.class)
                .map((id) -> id.generation().equals(GenerationType.IDENTITY))
                .orElse(false);
    }
}
