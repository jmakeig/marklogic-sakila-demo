package org.example.migration;

public class MigrateFilms {

	public static void main(String[] args) {
		// Instantiate our class that uses Spring Batch to migrate data from a SQL database to MarkLogic
		SqlMigrator m = new SqlMigrator();

		// Set JDBC connection details
		m.setJdbcDriver("com.mysql.jdbc.Driver");
		m.setJdbcUrl("jdbc:mysql://localhost:3306/sakila");
		m.setJdbcUsername("root");
		m.setJdbcPassword("admin");

		// Set MarkLogic connection details
		m.setMlHost("localhost");
		m.setMlPort(8510);
		m.setMlUsername("admin");
		m.setMlPassword("admin");

		// Migrate!
		String sql = "SELECT film.*, film_text.description as filmText, category.name as category, language.name as language \n" +
			" FROM film \n" +
			" LEFT JOIN film_category ON film.film_id = film_category.film_id \n" +
			" LEFT JOIN category ON film_category.category_id = category.category_id \n" +
			" LEFT JOIN film_text ON film.film_id = film_text.film_id \n" +
			" LEFT JOIN language ON film.language_id = language.language_id\n" +
			" ORDER BY film.film_id";
		String rootElementName = "film";
		String permissions = "dvd-store-reader,read,dvd-store-writer,update";
		String[] additionalCollections = new String[]{"sakila"};
		m.migrate(sql, rootElementName, permissions, additionalCollections);
	}
}
