package edu.unc.mapseq.workflow;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

public class Scratch {

    @Test
    public void testSubstringFlowcell() {
        String asdf = "120517_UNC15-SN850_0212_AD0VD1ACXX";
        assertTrue("D0VD1ACXX".equals(asdf.substring(asdf.length() - 9, asdf.length())));
    }

    @Test
    public void testSampleDirectory() {
        String saHome = "/proj/renci/sequence_analysis";
        File projectDirectory = new File(saHome, "NEC");
        File cohortDirectory = new File(projectDirectory, "UCSF");
        String sampleName = "051420Sm";
        File firstCharSampleDirectory = new File(cohortDirectory, sampleName.substring(0, 1));
        File sampleNameDirectory = new File(firstCharSampleDirectory, sampleName.substring(0, sampleName.length() - 2));
        System.out.println(sampleNameDirectory.getAbsolutePath());
    }

    @Test
    public void testFindFlowcellDirectory() {
        File r1FastqFile = new File(
                "/home/jdr0887/tmp/120110_UNC13-SN749_0141_AD0J7WACXX/CASAVA/TCGA-B0-4836-01A-01R-1305-07_TCGA001900_NoIndex_L001_R1.fastq.gz");
        Path flowcellPath = null;
        Path path = r1FastqFile.toPath();
        while (flowcellPath == null) {
            if (path.endsWith("120110_UNC13-SN749_0141_AD0J7WACXX")) {
                flowcellPath = path;
                break;
            } else {
                path = path.getParent();
            }
        }
        System.out.println(flowcellPath.toString());
    }

    @Test
    public void testSymlink() {
        try {
            File src = new File(
                    "/home/jdr0887/tmp/120110_UNC13-SN749_0141_AD0J7WACXX/CASAVA/TCGA-B0-4836-01A-01R-1305-07_TCGA001900_NoIndex_L001_R1.fastq.gz");
            File dest = new File("/home/jdr0887/tmp/120110_UNC13-SN749_0141_AD0J7WACXX/CASAVA/test");
            Files.createSymbolicLink(dest.toPath(), src.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTokenReplacement() {
        String a = String.format("if [ ! -e %2$s ]; then /bin/ln -s %1$s %2$s; fi", "asdf", "qwer");
        assertTrue(a.equals("if [ ! -e qwer ]; then /bin/ln -s asdf qwer; fi"));
    }
}
