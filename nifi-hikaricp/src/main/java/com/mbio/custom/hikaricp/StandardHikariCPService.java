/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mbio.custom.hikaricp;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.MetricRegistry;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;

@Tags({"DBCP", "SQL", "Database", "HikariCP", "NICE"})
@CapabilityDescription("Database connection pooling using HikariCP API")
@DynamicProperty(name = "DataSource property", value = "The value to set it to",
    description = "Property for the selected DataSource")
public class StandardHikariCPService extends AbstractControllerService implements HikariCPService {

    private AtomicReference<HikariDataSource> ds = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
      return ConfigUtil.getProperties();
    }
  
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(
        String propertyDescriptorName) {
  
      return ConfigUtil.getDynamicProperty(propertyDescriptorName);
    }
  
    /**
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
  
      HikariDataSource ds = new HikariDataSource();
  
      final String dsClassName = context.getProperty(ConfigUtil.DATASOURCE_CLASSNAME).getValue();
      final String userName = context.getProperty(ConfigUtil.USERNAME).getValue();
      final String password = context.getProperty(ConfigUtil.PASSWORD).getValue();
      final boolean autoCommit = context.getProperty(ConfigUtil.AUTO_COMMIT).asBoolean();
      final boolean metrics = context.getProperty(ConfigUtil.METRICS).asBoolean();
  
      ds.setDataSourceClassName(dsClassName);
      ds.setUsername(userName);
      ds.setPassword(password);
      ds.setAutoCommit(autoCommit);
  
      if (metrics) {
        ds.setMetricRegistry(new MetricRegistry());
      }
  
      context.getProperties().entrySet().parallelStream().filter(e -> e.getKey().isDynamic())
          .forEach(e -> {
            ds.addDataSourceProperty(e.getKey().getName(), e.getValue());
          });
  
      this.ds.set(ds);
    }
  
    @OnDisabled
    public void shutdown() {
      ds.get().close();
    }
  
    @Override
    public Connection getConnection() throws ProcessException {
      try {
        return ds.get().getConnection();
      }catch(SQLException e) {
        getLogger().error("Could not get connecdtion from datasources", e);
      }
      return null;
    }

}
