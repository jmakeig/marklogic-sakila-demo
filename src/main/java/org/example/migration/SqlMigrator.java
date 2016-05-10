package org.example.migration;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.helper.LoggingObject;
import com.marklogic.spring.batch.columnmap.PathAwareColumnMapProcessor;
import com.marklogic.spring.batch.item.ColumnMapItemWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps Spring Batch and provides a bunch of configuration options.
 */
public class SqlMigrator extends LoggingObject {

	private DataSource dataSource;
	private DatabaseClient databaseClient;
	private RowMapper<Map<String, Object>> rowMapper;
	private ItemProcessor<Map<String, Object>, Map<String, Object>> itemProcessor;

	private JobBuilderFactory jobBuilderFactory;
	private StepBuilderFactory stepBuilderFactory;
	private JobLauncher jobLauncher;

	private int chunkSize = 100;
	private boolean useRootLocalNameAsCollection = true;

	private String jdbcDriver;
	private String jdbcUrl;
	private String jdbcUsername;
	private String jdbcPassword;

	private String mlHost;
	private Integer mlPort;
	private String mlUsername;
	private String mlPassword;

	/**
	 * Default constructor will assemble all the Spring Batch components based on sensible defaults.
	 */
	public SqlMigrator() {
		initializeDefaultSpringBatchComponents();
		this.rowMapper = new ColumnMapRowMapper();
		this.itemProcessor = new PathAwareColumnMapProcessor();
	}

	/**
	 * Does all the migration work.
	 *
	 * @param sql           the SQL query to execute
	 * @param rootLocalName the local name for the root element in the XML document that's inserted into MarkLogic
	 * @param collections   optional array of collections to add each document to
	 */
	public void migrate(String sql, String rootLocalName, String permissions, String... collections) {
		initializeDataSource();
		initializeDatabaseClient();

		JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource);
		reader.setRowMapper(rowMapper);
		reader.setSql(sql);

		ColumnMapItemWriter writer = new ColumnMapItemWriter(databaseClient, rootLocalName);
		List<String> list = new ArrayList<>();
		if (useRootLocalNameAsCollection) {
			list.add(rootLocalName);
		}
		for (String s : collections) {
			list.add(s);
		}
		writer.setCollections(list.toArray(new String[]{}));

		TaskletStep step = stepBuilderFactory.get("migrationStep-" + System.currentTimeMillis())
			.<Map<String, Object>, Map<String, Object>>chunk(chunkSize).reader(reader).processor(itemProcessor)
			.writer(writer).build();

		Job job = jobBuilderFactory.get("migrationJob-" + System.currentTimeMillis()).start(step).build();

		Map<String, JobParameter> parameters = new HashMap<String, JobParameter>();
		parameters.put("SQL", new JobParameter(sql));
		JobParameters jobParams = new JobParameters(parameters);

		try {
			jobLauncher.run(job, jobParams);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			databaseClient.release();
			databaseClient = null;
		}
	}

	/**
	 * Initialize a JDBC data source. Using the ever-so-simple DriverManagerDataSource. Better
	 * approach is to use commons-dbcp2 and commons-pool2.
	 */
	private void initializeDataSource() {
		if (this.dataSource == null) {
			DriverManagerDataSource dmds = new DriverManagerDataSource();
			dmds.setDriverClassName(jdbcDriver);
			dmds.setUrl(jdbcUrl);
			dmds.setUsername(jdbcUsername);
			dmds.setPassword(jdbcPassword);
			this.dataSource = dmds;
		}
	}

	/**
	 * Initialize the ML Java API connection.
	 */
	private void initializeDatabaseClient() {
		if (this.databaseClient == null) {
			this.databaseClient = DatabaseClientFactory.newClient(mlHost, mlPort, mlUsername, mlPassword, Authentication.DIGEST);
		}
	}

	private void initializeDefaultSpringBatchComponents() {
		ResourcelessTransactionManager transactionManager = new ResourcelessTransactionManager();
		MapJobRepositoryFactoryBean f = new MapJobRepositoryFactoryBean(transactionManager);
		try {
			f.afterPropertiesSet();
			JobRepository jobRepository = f.getObject();
			this.jobBuilderFactory = new JobBuilderFactory(jobRepository);
			this.stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
			SimpleJobLauncher jbl = new SimpleJobLauncher();
			jbl.setJobRepository(jobRepository);
			jbl.afterPropertiesSet();
			this.jobLauncher = jbl;
		} catch (Exception ex) {
			throw new RuntimeException("Unable to initialize SqlMigrator, cause: " + ex.getMessage(), ex);
		}
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public void setUseRootLocalNameAsCollection(boolean useRootElementNameAsCollection) {
		this.useRootLocalNameAsCollection = useRootElementNameAsCollection;
	}

	public void setJdbcDriver(String jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public void setJdbcUsername(String jdbcUsername) {
		this.jdbcUsername = jdbcUsername;
	}

	public void setJdbcPassword(String jdbcPassword) {
		this.jdbcPassword = jdbcPassword;
	}

	public void setMlHost(String mlHost) {
		this.mlHost = mlHost;
	}

	public void setMlPort(Integer mlPort) {
		this.mlPort = mlPort;
	}

	public void setMlUsername(String mlUsername) {
		this.mlUsername = mlUsername;
	}

	public void setMlPassword(String mlPassword) {
		this.mlPassword = mlPassword;
	}

	public void setItemProcessor(ItemProcessor<Map<String, Object>, Map<String, Object>> itemProcessor) {
		this.itemProcessor = itemProcessor;
	}
}
