package org.example.migration;

public class MigrateFilmsWithActors {

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
		String sql = "SELECT film.*, film_text.description as filmText, category.name as category, language.name as language, \n" +
			"actor.actor_id as \"actor/id\", actor.first_name as \"actor/firstName\", actor.last_name as \"actor/lastName\"\n" +
			"FROM film LEFT JOIN film_category ON film.film_id = film_category.film_id \n" +
			"LEFT JOIN category ON film_category.category_id = category.category_id \n" +
			"LEFT JOIN film_actor ON film.film_id = film_actor.film_id \n" +
			"LEFT JOIN actor ON film_actor.actor_id = actor.actor_id  \n" +
			"LEFT JOIN film_text ON film.film_id = film_text.film_id \n" +
			"LEFT JOIN language ON film.language_id = language.language_id \n" +
			"ORDER BY film.film_id\n";
		String rootElementName = "film";
		String[] additionalCollections = new String[]{"sakila"};
		m.migrate(sql, rootElementName, additionalCollections);
	}
}
