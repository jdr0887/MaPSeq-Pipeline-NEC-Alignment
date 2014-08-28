package edu.unc.mapseq.messaging;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.Test;

import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.ws.SampleService;

public class NECAlignmentMessageTest {

    @Test
    public void testQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "biodev2.its.unc.edu"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/nec.alignment");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            String format = "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%d\"},{\"entityType\":\"WorkflowRun\",\"name\":\"%s-%s\"}]}";

            producer.send(session.createTextMessage(String.format(format, 67401, "jdr-nec-alignment",
                    DateFormatUtils.ISO_TIME_NO_T_FORMAT.format(new Date()))));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(session.createTextMessage(String.format(format, 67467, "jdr-nec-alignment",
                    DateFormatUtils.ISO_TIME_NO_T_FORMAT.format(new Date()))));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 67499, "jdr-nec-alignment",
                    DateFormatUtils.ISO_TIME_NO_T_FORMAT.format(new Date()))));

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void testEntireFlowcellBySample() throws IOException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "biodev2.its.unc.edu"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/nec.alignment");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            String format = "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%s\"},{\"entityType\":\"WorkflowRun\",\"name\":\"%s-%s\"}]}";
            InputStream is = NECAlignmentMessageTest.class.getResourceAsStream("htsf_sample_id_list.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;

            while ((line = br.readLine()) != null) {
                producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", line.trim(),
                        "jdr-test-nec-alignment", DateFormatUtils.ISO_TIME_NO_T_FORMAT.format(new Date()))));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void testQueueStress() {

        QName serviceQName = new QName("http://ws.mapseq.unc.edu", "SampleService");
        QName portQName = new QName("http://ws.mapseq.unc.edu", "SamplePort");
        Service service = Service.create(serviceQName);
        String host = "biodev2.its.unc.edu";
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_MTOM_BINDING,
                String.format("http://%s:%d/cxf/SampleService", host, 8181));
        SampleService sampleService = service.getPort(SampleService.class);

        List<Sample> sampleList = new ArrayList<Sample>();

        // sampleList.addAll(sampleService.findByFlowcellId(191541L));
        // sampleList.addAll(sampleService.findByFlowcellId(191738L));
        // sampleList.addAll(sampleService.findByFlowcellId(190345L));
        // sampleList.addAll(sampleService.findByFlowcellId(190520L));
        // sampleList.addAll(sampleService.findByFlowcellId(191372L));
        sampleList.addAll(sampleService.findByFlowcellId(192405L));
        sampleList.addAll(sampleService.findByFlowcellId(191192L));

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "biodev2.its.unc.edu"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/nec.alignment");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            String format = "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%d\"},{\"entityType\":\"WorkflowRun\",\"name\":\"%s_L%d_%s_BWA\"}]}";
            for (Sample sample : sampleList) {

                if ("Undetermined".equals(sample.getBarcode())) {
                    continue;
                }

                Flowcell flowcell = sample.getFlowcell();
                String message = String.format(format, sample.getId(), flowcell.getName(), sample.getLaneIndex(),
                        sample.getName());
                System.out.println(message);
                producer.send(session.createTextMessage(message));
            }

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }
}
