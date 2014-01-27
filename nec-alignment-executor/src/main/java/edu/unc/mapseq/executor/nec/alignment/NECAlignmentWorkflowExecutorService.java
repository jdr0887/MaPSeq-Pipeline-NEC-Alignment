package edu.unc.mapseq.executor.nec.alignment;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NECAlignmentWorkflowExecutorService {

    private final Logger logger = LoggerFactory.getLogger(NECAlignmentWorkflowExecutorService.class);

    private final Timer mainTimer = new Timer();

    private NECAlignmentWorkflowExecutorTask task;

    private Long period = 5L;

    public NECAlignmentWorkflowExecutorService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        long delay = 1 * 60 * 1000;
        mainTimer.scheduleAtFixedRate(task, delay, period * 60 * 1000);
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NECAlignmentWorkflowExecutorTask getTask() {
        return task;
    }

    public void setTask(NECAlignmentWorkflowExecutorTask task) {
        this.task = task;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

}
