package edu.unc.mapseq.messaging.nec.alignment;

import java.io.IOException;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.WorkflowRunDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.dao.model.WorkflowRunAttemptStatusType;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.impl.AbstractMessageListener;
import edu.unc.mapseq.workflow.model.WorkflowMessage;

public class NECAlignmentMessageListener extends AbstractMessageListener {

    private final Logger logger = LoggerFactory.getLogger(NECAlignmentMessageListener.class);

    public NECAlignmentMessageListener() {
        super();
    }

    @Override
    public void onMessage(Message message) {
        logger.debug("ENTERING onMessage(Message)");

        String messageValue = null;

        try {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                messageValue = textMessage.getText();
                logger.debug("Received TextMessage: {}", messageValue);
            }
        } catch (JMSException e2) {
            e2.printStackTrace();
        }

        if (StringUtils.isEmpty(messageValue)) {
            logger.warn("message value is empty");
            return;
        }

        logger.info("messageValue: {}", messageValue);

        ObjectMapper mapper = new ObjectMapper();
        WorkflowMessage workflowMessage = null;

        try {
            workflowMessage = mapper.readValue(messageValue, WorkflowMessage.class);
            if (workflowMessage.getEntities() == null) {
                logger.error("json lacks entities");
                return;
            }
        } catch (IOException e) {
            logger.error("BAD JSON format", e);
            return;
        }

        MaPSeqDAOBean daoBean = getWorkflowBeanService().getMaPSeqDAOBean();
        WorkflowDAO workflowDAO = daoBean.getWorkflowDAO();
        WorkflowRunDAO workflowRunDAO = daoBean.getWorkflowRunDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = daoBean.getWorkflowRunAttemptDAO();

        Workflow workflow = null;
        try {
            List<Workflow> workflowList = workflowDAO.findByName("NECAlignment");
            if (workflowList == null || (workflowList != null && workflowList.isEmpty())) {
                logger.error("No Workflow Found: {}", "NECAlignment");
                return;
            }
            workflow = workflowList.get(0);
        } catch (MaPSeqDAOException e) {
            logger.error("ERROR", e);
        }

        try {
            WorkflowRun workflowRun = createWorkflowRun(workflowMessage, workflow);

            Long workflowRunId = workflowRunDAO.save(workflowRun);
            workflowRun.setId(workflowRunId);

            WorkflowRunAttempt attempt = new WorkflowRunAttempt();
            attempt.setStatus(WorkflowRunAttemptStatusType.PENDING);
            attempt.setWorkflowRun(workflowRun);
            workflowRunAttemptDAO.save(attempt);

        } catch (WorkflowException | MaPSeqDAOException e1) {
            logger.error(e1.getMessage(), e1);
        }

    }

}
