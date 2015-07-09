package com.tuniu.dbsync.format;
public interface FormatterRegistrar {

	/**
	 * Register Formatters and Converters with a FormattingConversionService
	 * through a FormatterRegistry SPI.
	 * @param registry the FormatterRegistry instance to use.
	 */
	void registerFormatters(FormatterRegistry registry);

}