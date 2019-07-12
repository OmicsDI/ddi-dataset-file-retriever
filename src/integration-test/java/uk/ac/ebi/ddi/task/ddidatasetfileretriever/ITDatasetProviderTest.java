package uk.ac.ebi.ddi.task.ddidatasetfileretriever;

import org.junit.Assert;
import org.junit.Test;
import uk.ac.ebi.ddi.ddidomaindb.database.DB;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.providers.*;

import java.io.IOException;
import java.util.Set;

public class ITDatasetProviderTest {
    private IDatasetFileUrlRetriever retriever = new DefaultDatasetFileUrlRetriever();

    public ITDatasetProviderTest() {
        retriever = new ArrayExpressFileUrlRetriever(retriever);
        retriever = new GEOFileUrlRetriever(retriever);
        retriever = new BioModelsFileUrlRetriever(retriever);
        retriever = new ExpressionAtlasFileUrlRetriever(retriever);
        retriever = new DbGapFileUrlRetriever(retriever);
        retriever = new GNPSFileUrlRetriever(retriever);
        retriever = new JPostFileUrlRetriever(retriever);
        retriever = new MassIVEFileUrlRetriever(retriever);
        retriever = new LincsFileUrlRetriever(retriever);
        retriever = new PeptideAtlasFileUrlRetriever(retriever);
        retriever = new MetabolightsFileUrlRetriever(retriever);
        retriever = new EVAFileUrlRetriever(retriever);
        retriever = new MetabolomicsWorkbenchFileUrlRetriever(retriever);
        retriever = new ENAFileUrlRetriever(retriever);
    }

    @Test
    public void testArrayExpress() throws IOException {
        Set<String> files = retriever.getDatasetFiles("E-MEXP-2224", DB.ARRAY_EXPRESS.getDBName());
        Assert.assertEquals(7, files.size());

        files = retriever.getDatasetFiles("E-GEOD-18213", DB.ARRAY_EXPRESS.getDBName());
        Assert.assertEquals(4, files.size());
    }

    @Test
    public void testGEODatabase() throws IOException {
        Set<String> files = retriever.getDatasetFiles("GSE4745", DB.GEO.getDBName());
        Assert.assertEquals(2, files.size());

        files = retriever.getDatasetFiles("GSE2096", DB.GEO.getDBName());
        Assert.assertEquals(0, files.size());
    }

    @Test
    public void testBioModelsDatabase() throws IOException {
        Set<String> files = retriever.getDatasetFiles("BIOMD0000000652", DB.BIOMODELS.getDBName());
        Assert.assertEquals(10, files.size());
    }

    @Test
    public void testExpressionAtlas() throws IOException {
        Set<String> files = retriever.getDatasetFiles("E-GEOD-4745", DB.EXPRESSION_ATLAS.getDBName());
        Assert.assertEquals(91, files.size());
    }

    @Test
    public void testDBGap() throws IOException {
        Set<String> files = retriever.getDatasetFiles("phs000703", DB.DB_GAP.getDBName());
        Assert.assertEquals(21, files.size());
    }

    @Test
    public void testGNPSDatabase() throws IOException {
        Set<String> files = retriever.getDatasetFiles("MSV000080113", DB.GNPS.getDBName());
        Assert.assertEquals(14, files.size());
    }

    @Test
    public void testJPost() throws IOException {
        Set<String> files = retriever.getDatasetFiles("PXD004621", DB.JPOST.getDBName());
        Assert.assertEquals(62, files.size());
    }

    @Test
    public void testMassIVE() throws IOException {
        Set<String> files = retriever.getDatasetFiles("MSV000078822", DB.MASSIVE.getDBName());
        Assert.assertEquals(153, files.size());
    }

    @Test
    public void testLincs() throws IOException {
        Set<String> files = retriever.getDatasetFiles("LDS-1372", DB.LINCS.getDBName());
        Assert.assertEquals(4, files.size());

        files = retriever.getDatasetFiles("LDS-1226", DB.LINCS.getDBName());
        Assert.assertEquals(1, files.size());

        files = retriever.getDatasetFiles("LDS-1237", DB.LINCS.getDBName());
        Assert.assertEquals(0, files.size());
    }

    @Test
    public void testPeptideAtlas() throws IOException {
        Set<String> files = retriever.getDatasetFiles("PAe000572", DB.PEPTIDEATLAS.getDBName());
        Assert.assertEquals(6, files.size());
    }

    @Test
    public void testMetabolights() throws IOException {
        Set<String> files = retriever.getDatasetFiles("MTBLS21", DB.METABOLIGHTS.getDBName());
        Assert.assertEquals(91, files.size());
    }

    @Test
    public void testEVA() throws IOException {
        Set<String> files = retriever.getDatasetFiles("PRJEB4019", DB.EVA.getDBName());
        Assert.assertEquals(25, files.size());
    }

    @Test
    public void testMetaWB() throws Exception {
        Set<String> files = retriever.getDatasetFiles("ST000814", DB.METABOLOMICSWORKBENCH.getDBName());
        Assert.assertEquals(3, files.size());
    }

    @Test
    public void testENA() throws Exception {
        Set<String> files = retriever.getDatasetFiles("PRJNA215355", DB.ENA.getDBName());
        Assert.assertTrue(files.size() > 0);
    }
}
