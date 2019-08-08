package uk.ac.ebi.ddi.task.ddidatasetfileretriever;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ddi.service.db.model.dataset.Dataset;
import uk.ac.ebi.ddi.service.db.service.dataset.DatasetFileService;
import uk.ac.ebi.ddi.service.db.service.dataset.IDatasetService;
import uk.ac.ebi.ddi.service.db.utils.DatasetCategory;
import uk.ac.ebi.ddi.task.ddidatasetfileretriever.configuration.DatasetFileRetrieveTaskProperties;

import java.util.List;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = DdiDatasetFileRetrieverApplication.class,
		initializers = ConfigFileApplicationContextInitializer.class)
@TestPropertySource(properties = {
		"file-retriever.database_name=PeptideAtlas"
})
public class ITDatasetFileRetrieverTest {

	@Autowired
	private IDatasetService datasetService;

	@Autowired
	private DatasetFileRetrieveTaskProperties taskProperties;

	@Autowired
	private DatasetFileService datasetFileService;

	@Autowired
	private DdiDatasetFileRetrieverApplication application;

	@Before
	public void setUp() throws Exception {
		Dataset dataset = new Dataset();
		dataset.setAccession("PAe000289");
		dataset.setCurrentStatus(DatasetCategory.ENRICHED.getType());
		dataset.setDatabase(taskProperties.getDatabaseName());
		datasetService.save(dataset);
	}

	@Test
	public void contextLoads() throws Exception {
		application.run();
		List<String> fileUrls = datasetFileService.getFiles("PAe000289", taskProperties.getDatabaseName());
		Assert.assertTrue(fileUrls.size() > 0);
	}

}
