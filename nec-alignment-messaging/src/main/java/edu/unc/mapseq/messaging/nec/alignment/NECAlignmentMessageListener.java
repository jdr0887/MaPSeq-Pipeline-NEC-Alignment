package edu.unc.mapseq.messaging.nec.alignment;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.unc.mapseq.dao.AccountDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowPlanDAO;
import edu.unc.mapseq.dao.WorkflowRunDAO;
import edu.unc.mapseq.dao.model.Account;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowPlan;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunStatusType;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.impl.AbstractMessageListener;
import edu.unc.mapseq.workflow.model.WorkflowEntity;
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
            if (StringUtils.isEmpty(workflowMessage.getAccountName())) {
                logger.error("json lacks account_name");
                return;
            }
            if (workflowMessage.getEntities() == null) {
                logger.error("json lacks entities");
                return;
            }
        } catch (IOException e) {
            logger.error("BAD JSON format", e);
            return;
        }

        MaPSeqDAOBean daoBean = getWorkflowBeanService().getMaPSeqDAOBean();
        AccountDAO accountDAO = daoBean.getAccountDAO();
        WorkflowDAO workflowDAO = daoBean.getWorkflowDAO();
        WorkflowRunDAO workflowRunDAO = daoBean.getWorkflowRunDAO();
        WorkflowPlanDAO workflowPlanDAO = daoBean.getWorkflowPlanDAO();

        SequencerRun sequencerRun = null;
        Set<HTSFSample> htsfSampleSet = new HashSet<HTSFSample>();
        WorkflowRun workflowRun = null;
        Account account = null;

        String accountName = workflowMessage.getAccountName();

        try {
            account = accountDAO.findByName(accountName);
        } catch (MaPSeqDAOException e) {
        }

        if (account == null) {
            logger.error("Must register account first");
            return;
        }

        Workflow workflow = null;
        String workflowName = "NECAlignment";
        try {
            workflow = workflowDAO.findByName(workflowName);
        } catch (MaPSeqDAOException e) {
            logger.error("ERROR", e);
        }

        if (workflow == null) {
            logger.error("No Workflow Found: {}", workflowName);
            return;
        }

        try {

            for (WorkflowEntity entity : workflowMessage.getEntities()) {

                if (StringUtils.isNotEmpty(entity.getEntityType())) {

                    if (SequencerRun.class.getSimpleName().equals(entity.getEntityType())) {
                        sequencerRun = getSequencerRun(entity);
                    }

                    if (HTSFSample.class.getSimpleName().equals(entity.getEntityType())) {
                        HTSFSample htsfSample = getHTSFSample(entity);
                        htsfSampleSet.add(htsfSample);
                    }

                    if (WorkflowRun.class.getSimpleName().equals(entity.getEntityType())) {
                        workflowRun = getWorkflowRun(workflow, entity, account);
                    }

                }

            }
        } catch (WorkflowException e1) {
            logger.error(e1.getMessage(), e1);
            return;
        }

        if (workflowRun == null) {
            logger.error("workflowRun is null, not running anything");
            return;
        }

        if (sequencerRun == null && htsfSampleSet.size() == 0) {
            logger.error("Invalid JSON...not running anything");
            workflowRun.setStatus(WorkflowRunStatusType.FAILED);
        }

        try {
            Long workflowRunId = workflowRunDAO.save(workflowRun);
            workflowRun.setId(workflowRunId);

            WorkflowPlan workflowPlan = new WorkflowPlan();
            workflowPlan.setWorkflowRun(workflowRun);
            if (htsfSampleSet.size() > 0) {
                workflowPlan.setHTSFSamples(htsfSampleSet);
            }
            if (sequencerRun != null) {
                workflowPlan.setSequencerRun(sequencerRun);
            }
            workflowPlanDAO.save(workflowPlan);
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

}
