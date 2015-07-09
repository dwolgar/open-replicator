package com.tuniu.dbsync.format;

import java.lang.annotation.Annotation;
import java.util.Set;

public interface AnnotationFormatterFactory {

	/**
	 * The types of fields that may be annotated with the &lt;A&gt; annotation.
	 */
	Set<Class<?>> getFieldTypes();

	/**
	 * Get the Parser to parse a submitted value for a field of {@code fieldType} annotated with {@code annotation}.
	 * If the object the parser returns is not assignable to {@code fieldType}, a coersion to {@code fieldType} will be attempted before the field is set.
	 * @param annotation the annotation instance
	 * @param fieldType the type of field that was annotated
	 * @return the parser
	 */
	Parser<?> getParser(Annotation annotation, Class<?> fieldType);

}
