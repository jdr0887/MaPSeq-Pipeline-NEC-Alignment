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
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.RunModeType;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.bwa.BWAAlignCLI;
import edu.unc.mapseq.module.bwa.BWASAMPairedEndCLI;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.core.WriteVCFHeaderCLI;
import edu.unc.mapseq.module.fastqc.FastQCCLI;
import edu.unc.mapseq.module.fastqc.IgnoreLevelType;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.picard.PicardSortOrderType;
import edu.unc.mapseq.module.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowUtil;
import edu.unc.mapseq.workflow.impl.AbstractSampleWorkflow;
import edu.unc.mapseq.workflow.impl.IRODSBean;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

public class NECAlignmentWorkflow extends AbstractSampleWorkflow {

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

        Set<Sample> sampleSet = getAggregatedSamples();
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");
        String readGroupPlatform = getWorkflowBeanService().getAttributes().get("readGroupPlatform");
        String readGroupPlatformUnit = getWorkflowBeanService().getAttributes().get("readGroupPlatformUnit");

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            logger.debug(sample.toString());

            Flowcell flowcell = sample.getFlowcell();
            File outputDirectory = new File(sample.getOutputDirectory(), getName());
            File tmpDirectory = new File(outputDirectory, "tmp");
            tmpDirectory.mkdirs();

            List<File> readPairList = WorkflowUtil.getReadPairList(sample.getFileDatas(), flowcell.getName(),
                    sample.getLaneIndex());
            logger.debug("readPairList.size(): {}", readPairList.size());

            // assumption: a dash is used as a delimiter between a participantId
            // and the external code
            int idx = sample.getName().lastIndexOf("-");
            String sampleName = idx != -1 ? sample.getName().substring(0, idx) : sample.getName();

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
                CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, WriteVCFHeaderCLI.class,
                        attempt.getId(), sample.getId()).siteName(siteName);
                String flowcellProper = flowcell.getName().substring(flowcell.getName().length() - 9,
                        flowcell.getName().length());
                writeVCFHeaderOut = new File(outputDirectory, fastqLaneRootName + ".vcf.hdr");
                builder.addArgument(WriteVCFHeaderCLI.BARCODE, sample.getBarcode())
                        .addArgument(WriteVCFHeaderCLI.RUN, flowcell.getName())
                        .addArgument(WriteVCFHeaderCLI.SAMPLENAME, sampleName)
                        .addArgument(WriteVCFHeaderCLI.STUDYNAME, sample.getStudy().getName())
                        .addArgument(WriteVCFHeaderCLI.LANE, sample.getLaneIndex().toString())
                        .addArgument(WriteVCFHeaderCLI.LABNAME, "kwilhelmsen")
                        .addArgument(WriteVCFHeaderCLI.FLOWCELL, flowcellProper)
                        .addArgument(WriteVCFHeaderCLI.OUTPUT, writeVCFHeaderOut.getAbsolutePath());
                CondorJob writeVCFHeaderJob = builder.build();
                logger.info(writeVCFHeaderJob.toString());
                graph.addVertex(writeVCFHeaderJob);

                // new job
                builder = WorkflowJobFactory.createJob(++count, FastQCCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                fastqcR1Output = new File(outputDirectory, r1FastqRootName + ".fastqc.zip");
                builder.addArgument(FastQCCLI.INPUT, r1FastqFile.getAbsolutePath())
                        .addArgument(FastQCCLI.OUTPUT, fastqcR1Output.getAbsolutePath())
                        .addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString());
                CondorJob fastQCR1Job = builder.build();
                logger.info(fastQCR1Job.toString());
                graph.addVertex(fastQCR1Job);

                // new job
                builder = WorkflowJobFactory
                        .createJob(++count, BWAAlignCLI.class, attempt.getId(), sample.getId(), false)
                        .siteName(siteName).numberOfProcessors(4);
                File saiR1OutFile = new File(outputDirectory, r1FastqRootName + ".sai");
                builder.addArgument(BWAAlignCLI.THREADS, "4")
                        .addArgument(BWAAlignCLI.FASTQ, r1FastqFile.getAbsolutePath())
                        .addArgument(BWAAlignCLI.FASTADB, referenceSequence)
                        .addArgument(BWAAlignCLI.OUTFILE, saiR1OutFile.getAbsolutePath());
                CondorJob bwaAlignR1Job = builder.build();
                logger.info(bwaAlignR1Job.toString());
                graph.addVertex(bwaAlignR1Job);
                graph.addEdge(fastQCR1Job, bwaAlignR1Job);

                // new job
                builder = WorkflowJobFactory.createJob(++count, FastQCCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                fastqcR2Output = new File(outputDirectory, r2FastqRootName + ".fastqc.zip");
                builder.addArgument(FastQCCLI.INPUT, r2FastqFile.getAbsolutePath())
                        .addArgument(FastQCCLI.OUTPUT, fastqcR2Output.getAbsolutePath())
                        .addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString());
                CondorJob fastQCR2Job = builder.build();
                logger.info(fastQCR2Job.toString());
                graph.addVertex(fastQCR2Job);

                // new job
                builder = WorkflowJobFactory
                        .createJob(++count, BWAAlignCLI.class, attempt.getId(), sample.getId(), false)
                        .siteName(siteName).numberOfProcessors(4);
                File saiR2OutFile = new File(outputDirectory, r2FastqRootName + ".sai");
                builder.addArgument(BWAAlignCLI.THREADS, "4")
                        .addArgument(BWAAlignCLI.FASTQ, r2FastqFile.getAbsolutePath())
                        .addArgument(BWAAlignCLI.FASTADB, referenceSequence)
                        .addArgument(BWAAlignCLI.OUTFILE, saiR2OutFile.getAbsolutePath());
                CondorJob bwaAlignR2Job = builder.build();
                logger.info(bwaAlignR2Job.toString());
                graph.addVertex(bwaAlignR2Job);
                graph.addEdge(fastQCR2Job, bwaAlignR2Job);

                // new job
                builder = WorkflowJobFactory.createJob(++count, BWASAMPairedEndCLI.class, attempt.getId(),
                        sample.getId(), false).siteName(siteName);
                File bwaSAMPairedEndOutFile = new File(outputDirectory, fastqLaneRootName + ".sam");
                builder.addArgument(BWASAMPairedEndCLI.FASTADB, referenceSequence)
                        .addArgument(BWASAMPairedEndCLI.FASTQ1, r1FastqFile.getAbsolutePath())
                        .addArgument(BWASAMPairedEndCLI.FASTQ2, r2FastqFile.getAbsolutePath())
                        .addArgument(BWASAMPairedEndCLI.SAI1, saiR1OutFile.getAbsolutePath())
                        .addArgument(BWASAMPairedEndCLI.SAI2, saiR2OutFile.getAbsolutePath())
                        .addArgument(BWASAMPairedEndCLI.OUTFILE, bwaSAMPairedEndOutFile.getAbsolutePath());
                CondorJob bwaSAMPairedEndJob = builder.build();
                logger.info(bwaSAMPairedEndJob.toString());
                graph.addVertex(bwaSAMPairedEndJob);
                graph.addEdge(bwaAlignR1Job, bwaSAMPairedEndJob);
                graph.addEdge(bwaAlignR2Job, bwaSAMPairedEndJob);
                graph.addEdge(writeVCFHeaderJob, bwaSAMPairedEndJob);

                // new job
                builder = WorkflowJobFactory
                        .createJob(++count, RemoveCLI.class, attempt.getId(), sample.getId(), false).siteName(siteName);
                builder.addArgument(RemoveCLI.FILE, saiR1OutFile.getAbsolutePath()).addArgument(RemoveCLI.FILE,
                        saiR2OutFile.getAbsolutePath());
                CondorJob removeSAIJob = builder.build();
                logger.info(removeSAIJob.toString());
                graph.addVertex(removeSAIJob);
                graph.addEdge(bwaSAMPairedEndJob, removeSAIJob);

                // new job
                builder = WorkflowJobFactory.createJob(++count, PicardAddOrReplaceReadGroupsCLI.class, attempt.getId(),
                        sample.getId()).siteName(siteName);
                File picardAddOrReplaceReadGroupsOuput = new File(outputDirectory, bwaSAMPairedEndOutFile.getName()
                        .replace(".sam", ".fixed-rg.bam"));
                builder.addArgument(PicardAddOrReplaceReadGroupsCLI.INPUT, bwaSAMPairedEndOutFile.getAbsolutePath())
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT,
                                picardAddOrReplaceReadGroupsOuput.getAbsolutePath())
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.SORTORDER,
                                PicardSortOrderType.COORDINATE.toString().toLowerCase())
                        .addArgument(
                                PicardAddOrReplaceReadGroupsCLI.READGROUPID,
                                String.format("%s-%s_L%03d", flowcell.getName(), sample.getBarcode(),
                                        sample.getLaneIndex()))
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPLIBRARY, sampleName)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORM, readGroupPlatform)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORMUNIT, sample.getBarcode())
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPSAMPLENAME, sampleName)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPCENTERNAME, "UNC");
                CondorJob picardAddOrReplaceReadGroupsJob = builder.build();
                logger.info(picardAddOrReplaceReadGroupsJob.toString());
                graph.addVertex(picardAddOrReplaceReadGroupsJob);
                graph.addEdge(bwaSAMPairedEndJob, picardAddOrReplaceReadGroupsJob);

                // new job
                builder = WorkflowJobFactory
                        .createJob(++count, RemoveCLI.class, attempt.getId(), sample.getId(), false).siteName(siteName);
                builder.addArgument(RemoveCLI.FILE, bwaSAMPairedEndOutFile.getAbsolutePath());
                CondorJob removeBWASAMPairedEndOutFileJob = builder.build();
                logger.info(removeBWASAMPairedEndOutFileJob.toString());
                graph.addVertex(removeBWASAMPairedEndOutFileJob);
                graph.addEdge(picardAddOrReplaceReadGroupsJob, removeBWASAMPairedEndOutFileJob);

                // new job
                builder = WorkflowJobFactory
                        .createJob(++count, SAMToolsIndexCLI.class, attempt.getId(), sample.getId()).siteName(siteName);
                File samtoolsIndexOutput = new File(outputDirectory, picardAddOrReplaceReadGroupsOuput.getName()
                        .replace(".bam", ".bai"));
                builder.addArgument(SAMToolsIndexCLI.INPUT, picardAddOrReplaceReadGroupsOuput.getAbsolutePath())
                        .addArgument(SAMToolsIndexCLI.OUTPUT, samtoolsIndexOutput.getAbsolutePath());
                CondorJob samtoolsIndexJob = builder.build();
                logger.info(samtoolsIndexJob.toString());
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

        Set<Sample> sampleSet = getAggregatedSamples();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            Flowcell flowcell = sample.getFlowcell();
            File outputDirectory = new File(sample.getOutputDirectory(), getName());

            File tmpDir = new File(outputDirectory, "tmp");
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }

            logger.debug(sample.toString());
            List<File> readPairList = WorkflowUtil.getReadPairList(sample.getFileDatas(), flowcell.getName(),
                    sample.getLaneIndex());
            logger.debug("readPairList.size(): {}", readPairList.size());

            String iRODSDirectory;

            switch (runMode) {
                case DEV:
                case STAGING:
                    iRODSDirectory = String.format("/genomicsDataGridZone/sequence_data/%s/nec/%s/%s", runMode
                            .toString().toLowerCase(), flowcell.getName(), sample.getLaneIndex().toString());
                    break;
                case PROD:
                default:
                    iRODSDirectory = String.format("/genomicsDataGridZone/sequence_data/nec/%s/%s", flowcell.getName(),
                            sample.getLaneIndex().toString());
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
                commandInput.setCommand(String.format("%s/bin/imeta add -C %s Project NEC", irodsHome, iRODSDirectory));
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                commandInput = new CommandInput();
                commandInput.setCommand(String.format("%s/bin/imeta rm -C %s Project NEC", irodsHome, iRODSDirectory));
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

    @Override
    public void cleanUp() throws WorkflowException {
        // do nothing
    }

}
