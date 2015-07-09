package com.tuniu.dbsync.format;

import java.text.ParseException;
import java.util.Locale;

public interface Parser<T> {

	/**
	 * Parse a text String to produce a T.
	 * @param text the text string
	 * @param locale the current user locale
	 * @return an instance of T
	 * @throws ParseException when a parse exception occurs in a java.text parsing library
	 * @throws IllegalArgumentException when a parse exception occurs
	 */
	T parse(String text, Locale locale) throws ParseException;

}
