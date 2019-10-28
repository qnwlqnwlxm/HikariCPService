package com.mbio.custom.hikaricp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.derby.jdbc.ClientDataSource;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.postgresql.ds.PGSimpleDataSource;

public enum ConfigUtil {

    INSTANCE;
  
    private static List<PropertyDescriptor> properties;
  
    private static final AllowableValue DERBY_DS = new AllowableValue(
        ClientDataSource.class.getName(), "Apache Derby", "Apache Derby Datasource");
  
    private static final AllowableValue PGSQL_DS =
        new AllowableValue(PGSimpleDataSource.class.getName(), "PostgreSQL", "PostgreSQL Datasource");
  
    public static final PropertyDescriptor DATASOURCE_CLASSNAME = new PropertyDescriptor.Builder()
        .name("dataSourceClassName").displayName("Datasource classname")
        .description("Fully qualified classname of datasource")
        .allowableValues(getAllowableValue()).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true).build();
  
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("userName").displayName("User Name").description("Database account user name")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();
  
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("password").displayName("Password").description("Database account password")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).sensitive(true).build();
  
    public static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
        .name("autoCommit").displayName("Auto commit").description("Database account user name")
        .allowableValues("true", "false").defaultValue("true")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR).required(false).build();
  
    public static final PropertyDescriptor METRICS = new PropertyDescriptor.Builder().name("metrics")
        .displayName("Metrics").description("Whether or not to log metrics")
        .allowableValues("true", "false").defaultValue("false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR).required(false).build();
  
    static {
      List<PropertyDescriptor> properties = new ArrayList<>();
      properties.add(DATASOURCE_CLASSNAME);
      properties.add(USERNAME);
      properties.add(PASSWORD);
      properties.add(AUTO_COMMIT);
      properties.add(METRICS);
      ConfigUtil.properties = Collections.unmodifiableList(properties);
    }
  
    public static List<PropertyDescriptor> getProperties() {
      return properties;
    }
  
    public static PropertyDescriptor getDynamicProperty(String propertyDescriptorName) {
      return new PropertyDescriptor.Builder().name(propertyDescriptorName).required(false)
          .addValidator(new DataSourcePropertyValidator()).dynamic(true).build();
    }
    
    private static AllowableValue[] getAllowableValue() {
      List<AllowableValue> values = new ArrayList<AllowableValue>();
      values.add(DERBY_DS);
      values.add(PGSQL_DS);
      
      return values.toArray(new AllowableValue[values.size()]);
    }
  
  }
  