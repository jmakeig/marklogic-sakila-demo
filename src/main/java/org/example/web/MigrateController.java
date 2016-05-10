package org.example.web;

import com.marklogic.client.helper.DatabaseClientConfig;
import com.marklogic.client.helper.LoggingObject;
import org.example.migration.SqlMigrator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.ArrayList;
import java.util.List;

/**
 * Controller for invoking the SqlMigrator program in marklogic-spring-batch.
 */
@Controller
public class MigrateController extends LoggingObject {

	@Autowired
	private DatabaseClientConfig config;

	@RequestMapping(value = "/v1/migrate", method = RequestMethod.PUT)
	public ResponseEntity<?> upload(@RequestBody MigrateData data) throws Exception {
		SqlMigrator m = new SqlMigrator();
		m.setJdbcDriver(data.getJdbcDriver());
		m.setJdbcUrl(data.getJdbcUrl());
		m.setJdbcUsername(data.getJdbcUsername());
		m.setJdbcPassword(data.getJdbcPassword());

		m.setMlHost(config.getHost());
		m.setMlPort(config.getPort());
		m.setMlUsername(config.getUsername());
		m.setMlPassword(config.getPassword());

		String[] collections = data.getCollections() != null ? data.getCollections().split(",") : null;
		m.migrate(data.getSql(), data.getRootLocalName(), "dvd-store-reader,read,dvd-store-writer,update", collections);

		return null;
	}
}
