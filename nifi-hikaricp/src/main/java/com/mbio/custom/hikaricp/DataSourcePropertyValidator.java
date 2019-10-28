package com.mbio.custom.hikaricp;

import java.beans.Introspector;
import java.lang.reflect.Method;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

public class DataSourcePropertyValidator implements Validator {

  @Override
  public ValidationResult validate(String propertyName, String PropertyValue,
      ValidationContext context) {

    String dsClassName = context.getProperty(ConfigUtil.DATASOURCE_CLASSNAME).getValue();
    return new ValidationResult.Builder().subject(propertyName).input(PropertyValue)
        .valid(isValidProperty(dsClassName, propertyName))
        .explanation(propertyName + " is not a valid property").build();
  }

  private boolean isValidProperty(final String dsClassName, final String propertyName) {
    try {
      Class<?> dsClass = Class.forName(dsClassName);

      Method[] methods = dsClass.getMethods();

      for (Method method : methods) {

        String methodName = method.getName();
        if (methodName.startsWith("set")) {
          String dsProperty = Introspector.decapitalize(methodName.substring(3));
          if (dsProperty.equals(propertyName)) {
            return true;
          }
        }
      }

    } catch (Exception e) {
      return false;
    }
    return false;
  }
}
