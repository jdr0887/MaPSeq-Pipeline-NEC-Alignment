package edu.unc.mapseq.messaging.nec.alignment;

import java.util.HashSet;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Account;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.dao.model.WorkflowPlan;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunStatusType;
import edu.unc.mapseq.workflow.EntityUtil;
import edu.unc.mapseq.workflow.WorkflowBeanService;

public class NECAlignmentMessageListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(NECAlignmentMessageListener.class);

    private WorkflowBeanService workflowBeanService;

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

        JSONObject jsonMessage = null;

        try {
            jsonMessage = new JSONObject(messageValue);
            if (!jsonMessage.has("entities") || !jsonMessage.has("account_name")) {
                logger.error("json lacks entities or account_name");
                return;
            }
        } catch (JSONException e) {
            logger.error("BAD JSON format", e);
            return;
        }

        SequencerRun sequencerRun = null;
        Set<HTSFSample> htsfSampleSet = new HashSet<HTSFSample>();
        WorkflowRun workflowRun = null;
        Account account = null;

        try {

            String accountName = jsonMessage.getString("account_name");

            try {
                account = this.workflowBeanService.getMaPSeqDAOBean().getAccountDAO().findByName(accountName);
            } catch (MaPSeqDAOException e) {
            }

            if (account == null) {
                logger.error("Must register account first");
                return;
            }

            JSONArray entityArray = jsonMessage.getJSONArray("entities");

            for (int i = 0; i < entityArray.length(); ++i) {

                JSONObject entityJSONObject = entityArray.getJSONObject(i);

                if (entityJSONObject.has("entity_type")) {

                    String entityType = entityJSONObject.getString("entity_type");

                    if ("Sequencer run".equals(entityType) || SequencerRun.class.getSimpleName().equals(entityType)) {
                        sequencerRun = EntityUtil.getSequencerRun(workflowBeanService.getMaPSeqDAOBean(),
                                entityJSONObject);
                    }

                    if ("HTSF Sample".equals(entityType) || HTSFSample.class.getSimpleName().equals(entityType)) {
                        HTSFSample htsfSample = EntityUtil.getHTSFSample(workflowBeanService.getMaPSeqDAOBean(),
                                entityJSONObject);
                        htsfSampleSet.add(htsfSample);
                    }

                    if ("Workflow run".equals(entityType) || WorkflowRun.class.getSimpleName().equals(entityType)) {
                        workflowRun = EntityUtil.getWorkflowRun(workflowBeanService.getMaPSeqDAOBean(),
                                "NECAlignment", entityJSONObject, account);
                    }

                }

            }
        } catch (JSONException e1) {
            e1.printStackTrace();
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
            Long workflowRunId = workflowBeanService.getMaPSeqDAOBean().getWorkflowRunDAO().save(workflowRun);
            workflowRun.setId(workflowRunId);

            WorkflowPlan workflowPlan = new WorkflowPlan();
            workflowPlan.setWorkflowRun(workflowRun);
            if (htsfSampleSet.size() > 0) {
                workflowPlan.setHTSFSamples(htsfSampleSet);
            }
            if (sequencerRun != null) {
                workflowPlan.setSequencerRun(sequencerRun);
            }
            this.workflowBeanService.getMaPSeqDAOBean().getWorkflowPlanDAO().save(workflowPlan);
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public WorkflowBeanService getWorkflowBeanService() {
        return workflowBeanService;
    }

    public void setWorkflowBeanService(WorkflowBeanService workflowBeanService) {
        this.workflowBeanService = workflowBeanService;
    }

}
