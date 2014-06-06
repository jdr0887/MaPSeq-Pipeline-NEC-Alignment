package edu.unc.mapseq.commands.nec.alignment;

import java.io.IOException;
import java.io.StringWriter;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.apache.karaf.shell.console.AbstractAction;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBean;

@Command(scope = "nec-alignment", name = "run-workflow", description = "Run NEC Alignment Workflow")
public class RunNECAlignmentWorkflowAction extends AbstractAction {

    @Argument(index = 0, name = "workflowRunName", description = "WorkflowRun.name", required = true, multiValued = false)
    private String workflowRunName;

    @Argument(index = 1, name = "htsfSampleId", description = "htsfSampleId", required = true, multiValued = false)
    private Long htsfSampleId;

    private MaPSeqDAOBean maPSeqDAOBean;

    private MaPSeqConfigurationService maPSeqConfigurationService;

    public RunNECAlignmentWorkflowAction() {
        super();
    }

    @Override
    public Object doExecute() {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                maPSeqConfigurationService.getWebServiceHost("localhost")));

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/nec.alignment");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            StringWriter sw = new StringWriter();

            JsonGenerator generator = new JsonFactory().createGenerator(sw);

            generator.writeStartObject();
            generator.writeStringField("accountName", System.getProperty("user.name"));
            generator.writeArrayFieldStart("entities");

            generator.writeStartObject();
            generator.writeStringField("entityType", "HTSFSample");
            generator.writeStringField("guid", htsfSampleId.toString());
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("entityType", "WorkflowRun");
            generator.writeStringField("name", workflowRunName);
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.flush();
            generator.close();

            sw.flush();
            sw.close();

            producer.send(session.createTextMessage(sw.toString()));

        } catch (JMSException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    public Long getHtsfSampleId() {
        return htsfSampleId;
    }

    public void setHtsfSampleId(Long htsfSampleId) {
        this.htsfSampleId = htsfSampleId;
    }

    public String getWorkflowRunName() {
        return workflowRunName;
    }

    public void setWorkflowRunName(String workflowRunName) {
        this.workflowRunName = workflowRunName;
    }

    public MaPSeqDAOBean getMaPSeqDAOBean() {
        return maPSeqDAOBean;
    }

    public void setMaPSeqDAOBean(MaPSeqDAOBean maPSeqDAOBean) {
        this.maPSeqDAOBean = maPSeqDAOBean;
    }

    public MaPSeqConfigurationService getMaPSeqConfigurationService() {
        return maPSeqConfigurationService;
    }

    public void setMaPSeqConfigurationService(MaPSeqConfigurationService maPSeqConfigurationService) {
        this.maPSeqConfigurationService = maPSeqConfigurationService;
    }

}
