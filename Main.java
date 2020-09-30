package jdbcdemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Main {
	private static void demoNotificationEvent1(Statement statement) throws SQLException {
		// create a table
		//System.out.println("USE Schema");
		statement.executeUpdate("USE Schema PUBLIC;");

		long startTime = System.nanoTime();

		// insert a row
		System.out.println("Insert 'Events'");
		statement.executeUpdate("insert into Events values ('123', 'all data', 'DELIVERED', '12/12/2020')");

		System.out.println("Done inserting");

		long endTime = System.nanoTime();

		System.out.println("write time : " + (endTime - startTime) / 1000000);

		// query the data
		System.out.println("Query Events");
		startTime = System.nanoTime();
		ResultSet resultSet = statement.executeQuery("SELECT * FROM Events");
		endTime = System.nanoTime();
		System.out.println("read time : " + (endTime - startTime) / 1000000);

		while (resultSet.next()) {
			StringBuilder resultText = new StringBuilder();

			resultText.append("ID: ").append(resultSet.getString(1)).append(" Event: ").append(resultSet.getString(2))
					.append(" STATUS: ").append(resultSet.getString(3)).append(" UpdatedDateTime: ")
					.append(resultSet.getString(4));

			//System.out.println(resultText.toString());
		}

	}

	public static void main(String[] args) throws Exception {
		// get connection
		System.out.println("Create JDBC connection");
		try (Connection connection = getConnection()) {
			System.out.println("Done creating JDBC connectionn");

			try (Statement statement = connection.createStatement()) {
				for (int i = 0; i < 5; i++) {
					// demoTableCreation(connection);
					demoNotificationEvent1(statement);
				}
			}
		}
	}

	private static Connection getConnection() throws SQLException {
		try {
			Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
		} catch (ClassNotFoundException ex) {
			System.err.println("Driver not found");
		}
		// build connection properties
		Properties properties = new Properties();
		properties.put("user", ""); // replace "" with your username
		properties.put("password", ""); // replace "" with your password
		properties.put("db", ""); // replace "" with target database name
		properties.put("schema", ""); // replace "" with target schema name
		// properties.put("tracing", "on");

		// create a new connection
		String connectStr = "jdbc:snowflake://EBA18774.snowflakecomputing.com";

		return DriverManager.getConnection(connectStr, properties);
	}
}
