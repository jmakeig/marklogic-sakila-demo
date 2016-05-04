package org.example.migration;

public class MigrateActors {

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
		String sql = "SELECT * FROM actor";
		String rootElementName = "actor";
		String[] additionalCollections = new String[]{"sakila"};
		m.migrate(sql, rootElementName, additionalCollections);
	}


}
