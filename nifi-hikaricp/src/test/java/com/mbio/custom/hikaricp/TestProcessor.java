package com.mbio.custom.hikaricp;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public class TestProcessor extends AbstractProcessor {

    public static final PropertyDescriptor DS_PROP = new PropertyDescriptor.Builder().name("dbcp")
            .description("HikariCPService test processor").identifiesControllerService(HikariCPService.class)
            .required(true).build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(DS_PROP);
        return propDescs;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        HikariCPService dbcpService = (HikariCPService) context.getProperty(DS_PROP).asControllerService();

        try (Connection conn = dbcpService.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("values 1")) {

            while (rs.next()) {
                getLogger().info("Results : " + rs.getObject(1));
            }
        } catch (Exception e) {
            getLogger().error("Problem testing connection", e);
        }
    }
}
