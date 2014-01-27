package edu.unc.mapseq.workflow.nec.alignment;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.RunModeType;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.module.bwa.BWAAlignCLI;
import edu.unc.mapseq.module.bwa.BWASAMPairedEndCLI;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.core.WriteVCFHeaderCLI;
import edu.unc.mapseq.module.fastqc.FastQCCLI;
import edu.unc.mapseq.module.fastqc.IgnoreLevelType;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.picard.PicardSortOrderType;
import edu.unc.mapseq.module.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.workflow.AbstractWorkflow;
import edu.unc.mapseq.workflow.IRODSBean;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowJobFactory;
import edu.unc.mapseq.workflow.WorkflowUtil;

public class NECAlignmentWorkflow extends AbstractWorkflow {

    private final Logger logger = LoggerFactory.getLogger(NECAlignmentWorkflow.class);

    public NECAlignmentWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return NECAlignmentWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/nec/alignment/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        Set<HTSFSample> htsfSampleSet = getAggregateHTSFSampleSet();
        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample,
                    getName().replace("Alignment", ""), getVersion());

            logger.debug("htsfSample: {}", htsfSample.toString());
            List<File> readPairList = WorkflowUtil.getReadPairList(htsfSample.getFileDatas(), sequencerRun.getName(),
                    htsfSample.getLaneIndex());
            logger.debug("readPairList.size(): {}", readPairList.size());

            // assumption: a dash is used as a delimiter between a participantId
            // and the external code
            int idx = htsfSample.getName().lastIndexOf("-");
            String sampleName = idx != -1 ? htsfSample.getName().substring(0, idx) : htsfSample.getName();

            if (readPairList.size() != 2) {
                throw new WorkflowException("readPairList != 2");
            }

            File r1FastqFile = readPairList.get(0);
            String r1FastqRootName = WorkflowUtil.getRootFastqName(r1FastqFile.getName());

            File r2FastqFile = readPairList.get(1);
            String r2FastqRootName = WorkflowUtil.getRootFastqName(r2FastqFile.getName());

            String fastqLaneRootName = StringUtils.removeEnd(r2FastqRootName, "_R2");

            File writeVCFHeaderOut;
            File fastqcR1Output;
            File fastqcR2Output;
            try {
                // new job
                CondorJob writeVCFHeaderJob = WorkflowJobFactory.createJob(++count, WriteVCFHeaderCLI.class,
                        getWorkflowPlan(), htsfSample);
                writeVCFHeaderJob.setSiteName(siteName);
                writeVCFHeaderJob.addArgument(WriteVCFHeaderCLI.BARCODE, htsfSample.getBarcode());
                writeVCFHeaderJob.addArgument(WriteVCFHeaderCLI.RUN, sequencerRun.getName());
                writeVCFHeaderJob.addArgument(WriteVCFHeaderCLI.SAMPLENAME, sampleName);
                writeVCFHeaderJob.addArgument(WriteVCFHeaderCLI.STUDYNAME, htsfSample.getStudy().getName());
                writeVCFHeaderJob.addArgument(WriteVCFHeaderCLI.LANE, htsfSample.getLaneIndex().toString());
                writeVCFHeaderJob.addArgument(WriteVCFHeaderCLI.LABNAME, "kwilhelmsen");
                String flowcellProper = sequencerRun.getName().substring(sequencerRun.getName().length() - 9,
                        sequencerRun.getName().length());
                writeVCFHeaderJob.addArgument(WriteVCFHeaderCLI.FLOWCELL, flowcellProper);
                writeVCFHeaderOut = new File(outputDirectory, fastqLaneRootName + ".vcf.hdr");
                writeVCFHeaderJob.addArgument(WriteVCFHeaderCLI.OUTPUT, writeVCFHeaderOut.getAbsolutePath());
                graph.addVertex(writeVCFHeaderJob);

                // new job
                CondorJob fastQCR1Job = WorkflowJobFactory.createJob(++count, FastQCCLI.class, getWorkflowPlan(),
                        htsfSample);
                fastQCR1Job.setSiteName(siteName);
                fastQCR1Job.addArgument(FastQCCLI.INPUT, r1FastqFile.getAbsolutePath());
                fastqcR1Output = new File(outputDirectory, r1FastqRootName + ".fastqc.zip");
                fastQCR1Job.addArgument(FastQCCLI.OUTPUT, fastqcR1Output.getAbsolutePath());
                fastQCR1Job.addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString());
                graph.addVertex(fastQCR1Job);

                // new job
                CondorJob bwaAlignR1Job = WorkflowJobFactory.createJob(++count, BWAAlignCLI.class, getWorkflowPlan(),
                        htsfSample, false);
                bwaAlignR1Job.setSiteName(siteName);
                bwaAlignR1Job.addArgument(BWAAlignCLI.THREADS, "4");
                bwaAlignR1Job.setNumberOfProcessors(4);
                bwaAlignR1Job.addArgument(BWAAlignCLI.FASTQ, r1FastqFile.getAbsolutePath());
                bwaAlignR1Job.addArgument(BWAAlignCLI.FASTADB, referenceSequence);
                File saiR1OutFile = new File(outputDirectory, r1FastqRootName + ".sai");
                bwaAlignR1Job.addArgument(BWAAlignCLI.OUTFILE, saiR1OutFile.getAbsolutePath());
                graph.addVertex(bwaAlignR1Job);
                graph.addEdge(fastQCR1Job, bwaAlignR1Job);

                // new job
                CondorJob fastQCR2Job = WorkflowJobFactory.createJob(++count, FastQCCLI.class, getWorkflowPlan(),
                        htsfSample);
                fastQCR2Job.setSiteName(siteName);
                fastQCR2Job.addArgument(FastQCCLI.INPUT, r2FastqFile.getAbsolutePath());
                fastqcR2Output = new File(outputDirectory, r2FastqRootName + ".fastqc.zip");
                fastQCR2Job.addArgument(FastQCCLI.OUTPUT, fastqcR2Output.getAbsolutePath());
                fastQCR2Job.addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString());
                graph.addVertex(fastQCR2Job);

                // new job
                CondorJob bwaAlignR2Job = WorkflowJobFactory.createJob(++count, BWAAlignCLI.class, getWorkflowPlan(),
                        htsfSample, false);
                bwaAlignR2Job.setSiteName(siteName);
                bwaAlignR2Job.addArgument(BWAAlignCLI.THREADS, "4");
                bwaAlignR2Job.setNumberOfProcessors(4);
                bwaAlignR2Job.addArgument(BWAAlignCLI.FASTQ, r2FastqFile.getAbsolutePath());
                bwaAlignR2Job.addArgument(BWAAlignCLI.FASTADB, referenceSequence);
                File saiR2OutFile = new File(outputDirectory, r2FastqRootName + ".sai");
                bwaAlignR2Job.addArgument(BWAAlignCLI.OUTFILE, saiR2OutFile.getAbsolutePath());
                graph.addVertex(bwaAlignR2Job);
                graph.addEdge(fastQCR2Job, bwaAlignR2Job);

                // new job
                CondorJob bwaSAMPairedEndJob = WorkflowJobFactory.createJob(++count, BWASAMPairedEndCLI.class,
                        getWorkflowPlan(), htsfSample, false);
                bwaSAMPairedEndJob.setSiteName(siteName);
                bwaSAMPairedEndJob.addArgument(BWASAMPairedEndCLI.FASTADB, referenceSequence);
                bwaSAMPairedEndJob.addArgument(BWASAMPairedEndCLI.FASTQ1, r1FastqFile.getAbsolutePath());
                bwaSAMPairedEndJob.addArgument(BWASAMPairedEndCLI.FASTQ2, r2FastqFile.getAbsolutePath());
                bwaSAMPairedEndJob.addArgument(BWASAMPairedEndCLI.SAI1, saiR1OutFile.getAbsolutePath());
                bwaSAMPairedEndJob.addArgument(BWASAMPairedEndCLI.SAI2, saiR2OutFile.getAbsolutePath());
                File bwaSAMPairedEndOutFile = new File(outputDirectory, fastqLaneRootName + ".sam");
                bwaSAMPairedEndJob.addArgument(BWASAMPairedEndCLI.OUTFILE, bwaSAMPairedEndOutFile.getAbsolutePath());
                graph.addVertex(bwaSAMPairedEndJob);
                graph.addEdge(bwaAlignR1Job, bwaSAMPairedEndJob);
                graph.addEdge(bwaAlignR2Job, bwaSAMPairedEndJob);
                graph.addEdge(writeVCFHeaderJob, bwaSAMPairedEndJob);

                // new job
                CondorJob removeSAIR1Job = WorkflowJobFactory.createJob(++count, RemoveCLI.class, getWorkflowPlan(),
                        htsfSample, false);
                removeSAIR1Job.setSiteName(siteName);
                removeSAIR1Job.addArgument(RemoveCLI.FILE, saiR1OutFile.getAbsolutePath());
                graph.addVertex(removeSAIR1Job);
                graph.addEdge(bwaSAMPairedEndJob, removeSAIR1Job);

                // new job
                CondorJob removeSAIR2Job = WorkflowJobFactory.createJob(++count, RemoveCLI.class, getWorkflowPlan(),
                        htsfSample, false);
                removeSAIR2Job.setSiteName(siteName);
                removeSAIR2Job.addArgument(RemoveCLI.FILE, saiR2OutFile.getAbsolutePath());
                graph.addVertex(removeSAIR2Job);
                graph.addEdge(bwaSAMPairedEndJob, removeSAIR2Job);

                // new job
                CondorJob picardAddOrReplaceReadGroupsJob = WorkflowJobFactory.createJob(++count,
                        PicardAddOrReplaceReadGroupsCLI.class, getWorkflowPlan(), htsfSample);
                picardAddOrReplaceReadGroupsJob.setSiteName(siteName);
                picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.INPUT,
                        bwaSAMPairedEndOutFile.getAbsolutePath());
                File picardAddOrReplaceReadGroupsOuput = new File(outputDirectory, bwaSAMPairedEndOutFile.getName()
                        .replace(".sam", ".fixed-rg.bam"));
                picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT,
                        picardAddOrReplaceReadGroupsOuput.getAbsolutePath());
                picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.SORTORDER,
                        PicardSortOrderType.COORDINATE.toString().toLowerCase());
                picardAddOrReplaceReadGroupsJob.addArgument(
                        PicardAddOrReplaceReadGroupsCLI.READGROUPID,
                        String.format("%s-%s_L%03d", sequencerRun.getName(), htsfSample.getBarcode(),
                                htsfSample.getLaneIndex()));
                picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPLIBRARY,
                        sampleName);
                picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORM,
                        sequencerRun.getPlatform().getInstrument());
                picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORMUNIT,
                        htsfSample.getBarcode());
                picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPSAMPLENAME,
                        sampleName);
                picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPCENTERNAME, "UNC");
                graph.addVertex(picardAddOrReplaceReadGroupsJob);
                graph.addEdge(bwaSAMPairedEndJob, picardAddOrReplaceReadGroupsJob);

                // new job
                CondorJob removeBWASAMPairedEndOutFileJob = WorkflowJobFactory.createJob(++count, RemoveCLI.class,
                        getWorkflowPlan(), htsfSample, false);
                removeBWASAMPairedEndOutFileJob.setSiteName(siteName);
                removeBWASAMPairedEndOutFileJob.addArgument(RemoveCLI.FILE, bwaSAMPairedEndOutFile.getAbsolutePath());
                graph.addVertex(removeBWASAMPairedEndOutFileJob);
                graph.addEdge(picardAddOrReplaceReadGroupsJob, removeBWASAMPairedEndOutFileJob);

                // new job
                CondorJob samtoolsIndexJob = WorkflowJobFactory.createJob(++count, SAMToolsIndexCLI.class,
                        getWorkflowPlan(), htsfSample);
                samtoolsIndexJob.setSiteName(siteName);
                samtoolsIndexJob.addArgument(SAMToolsIndexCLI.INPUT,
                        picardAddOrReplaceReadGroupsOuput.getAbsolutePath());
                File samtoolsIndexOutput = new File(outputDirectory, picardAddOrReplaceReadGroupsOuput.getName()
                        .replace(".bam", ".bai"));
                samtoolsIndexJob.addArgument(SAMToolsIndexCLI.OUTPUT, samtoolsIndexOutput.getAbsolutePath());
                graph.addVertex(samtoolsIndexJob);
                graph.addEdge(picardAddOrReplaceReadGroupsJob, samtoolsIndexJob);

            } catch (Exception e) {
                throw new WorkflowException(e);
            }
        }

        return graph;
    }

    @Override
    public void postRun() throws WorkflowException {

        RunModeType runMode = getWorkflowBeanService().getMaPSeqConfigurationService().getRunMode();

        String irodsHome = System.getenv("NEC_IRODS_HOME");
        if (StringUtils.isEmpty(irodsHome)) {
            logger.error("NEC_IRODS_HOME is not set");
            throw new WorkflowException("NEC_IRODS_HOME is not set");
        }

        Set<HTSFSample> htsfSampleSet = getAggregateHTSFSampleSet();
        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = new File(htsfSample.getOutputDirectory());
            File tmpDir = new File(outputDirectory, "tmp");
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }

            logger.debug("htsfSample: {}", htsfSample.toString());
            List<File> readPairList = WorkflowUtil.getReadPairList(htsfSample.getFileDatas(), sequencerRun.getName(),
                    htsfSample.getLaneIndex());
            logger.debug("readPairList.size(): {}", readPairList.size());

            String iRODSDirectory;

            switch (runMode) {
                case DEV:
                case STAGING:
                    iRODSDirectory = String.format("/genomicsDataGridZone/sequence_data/%s/nec/%s/%s", runMode
                            .toString().toLowerCase(), htsfSample.getSequencerRun().getName(), htsfSample
                            .getLaneIndex().toString());
                    break;
                case PROD:
                default:
                    iRODSDirectory = String.format("/genomicsDataGridZone/sequence_data/nec/%s/%s", htsfSample
                            .getSequencerRun().getName(), htsfSample.getLaneIndex().toString());
                    break;
            }

            if (readPairList.size() == 2) {

                File r1FastqFile = readPairList.get(0);
                String r1FastqRootName = WorkflowUtil.getRootFastqName(r1FastqFile.getName());

                File r2FastqFile = readPairList.get(1);
                String r2FastqRootName = WorkflowUtil.getRootFastqName(r2FastqFile.getName());

                String fastqLaneRootName = StringUtils.removeEnd(r2FastqRootName, "_R2");

                CommandOutput commandOutput = null;

                List<CommandInput> commandInputList = new ArrayList<CommandInput>();
                CommandInput commandInput = new CommandInput();
                commandInput.setCommand(String.format("%s/bin/imkdir -p %s", irodsHome, iRODSDirectory));
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                commandInput = new CommandInput();
                commandInput.setCommand(String.format("%s/bin/imeta add -C %s Project NEC", irodsHome,
                        iRODSDirectory));
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                List<IRODSBean> files2RegisterToIRODS = new ArrayList<IRODSBean>();
                File writeVCFHeaderOut = new File(outputDirectory, fastqLaneRootName + ".vcf.hdr");
                files2RegisterToIRODS.add(new IRODSBean(writeVCFHeaderOut, "VcfHdr", null, null, runMode));
                files2RegisterToIRODS.add(new IRODSBean(r1FastqFile, "fastq", null, null, runMode));
                files2RegisterToIRODS.add(new IRODSBean(r2FastqFile, "fastq", null, null, runMode));
                File fastqcR1Output = new File(outputDirectory, r1FastqRootName + ".fastqc.zip");
                files2RegisterToIRODS.add(new IRODSBean(fastqcR1Output, "fastqc", null, null, runMode));
                File fastqcR2Output = new File(outputDirectory, r2FastqRootName + ".fastqc.zip");
                files2RegisterToIRODS.add(new IRODSBean(fastqcR2Output, "fastqc", null, null, runMode));

                for (IRODSBean bean : files2RegisterToIRODS) {

                    commandInput = new CommandInput();
                    commandInput.setExitImmediately(Boolean.FALSE);

                    StringBuilder registerCommandSB = new StringBuilder();
                    String registrationCommand = String.format("%s/bin/ireg -f %s %s/%s", irodsHome, bean.getFile()
                            .getAbsolutePath(), iRODSDirectory, bean.getFile().getName());
                    String deRegistrationCommand = String.format("%s/bin/irm -U %s/%s", irodsHome, iRODSDirectory, bean
                            .getFile().getName());
                    registerCommandSB.append(registrationCommand).append("\n");
                    registerCommandSB.append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand,
                            registrationCommand));
                    commandInput.setCommand(registerCommandSB.toString());
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                    commandInput = new CommandInput();
                    commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s FileType %s NEC", irodsHome,
                            iRODSDirectory, bean.getFile().getName(), bean.getType()));
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                    commandInput = new CommandInput();
                    commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s System %s NEC", irodsHome,
                            iRODSDirectory, bean.getFile().getName(),
                            StringUtils.capitalize(bean.getRunMode().toString().toLowerCase())));
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                }

                File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
                Executor executor = BashExecutor.getInstance();

                for (CommandInput ci : commandInputList) {
                    try {
                        commandOutput = executor.execute(ci, mapseqrc);
                        logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
                        logger.debug("commandOutput.getStdout(): {}", commandOutput.getStdout());
                    } catch (ExecutorException e) {
                        if (commandOutput != null) {
                            logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                        }
                    }
                }

            }
        }
    }

}
