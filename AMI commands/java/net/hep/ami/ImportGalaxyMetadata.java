package net.hep.ami.command.misc;

import java.util.*;
import java.util.stream.Collectors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.hep.ami.data.Row;
import net.hep.ami.data.RowSet;
import net.hep.ami.jdbc.Querier;
import net.hep.ami.jdbc.RouterQuerier;
import net.hep.ami.command.*;
import net.hep.ami.customInsertionProjects.*;
import net.hep.ami.utility.*;

import org.json.*;
import org.jetbrains.annotations.*;

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.hep.ami.QuerySingleton;
import net.hep.ami.Utils;

import net.hep.ami.CommandSingleton;


@CommandMetadata(role = "AMI_GUEST", visible = true)
public class ImportGalaxyMetadata extends AbstractCommand
{
	/*------------------------------------------------------------------------------------------------------------*/

	public ImportGalaxyMetadata(@NotNull Set<String> userRoles, @NotNull Map<String, String> arguments, long transactionId)
	{
		super(userRoles, arguments, transactionId);
	}

	/*------------------------------------------------------------------------------------------------------------*/

	@NotNull
	@Override
	public StringBuilder main(@NotNull Map<String, String> arguments) throws Exception
	{
		QuerySingleton querySingleton = QuerySingleton.INSTANCE;
		querySingleton.setQuerier(getQuerier("self"), false);
		Querier querier = null;

		String b64Metadata = arguments.get("b64Metadata");
		String encryptedMetadata = arguments.get("encryptedMetadata");
		Boolean onlyDataset = !Empty.is(arguments.get("onlyDataset"), Empty.STRING_NULL_EMPTY_BLANK) && arguments.get("onlyDataset").toLowerCase().equals("true");
		JSONObject metadata = null;

		JSONObject workflow = null;
		JSONObject invocation = null;
		JSONObject tools = null;
		JSONArray datasets = null;


		Boolean hasb64Metadata = !Empty.is(b64Metadata, Empty.STRING_NULL_EMPTY_BLANK);
		Boolean hasencryptedMetadata = !Empty.is(encryptedMetadata, Empty.STRING_NULL_EMPTY_BLANK);
		String projectName = arguments.get("project");
		String decodedStringMetadata = null;

		if(!hasb64Metadata && !hasencryptedMetadata) {
			throw new IllegalArgumentException("There is no metadata in the query.");
		}else{
			if(hasencryptedMetadata) {
				decodedStringMetadata = Utils.decodeJWT(encryptedMetadata);
			}else{
				decodedStringMetadata = Utils.decodeB64(b64Metadata);
			}
			metadata = new JSONObject(decodedStringMetadata);

			if(Empty.is(projectName, Empty.STRING_NULL_EMPTY_BLANK) && metadata.has("importParameters") && metadata.getJSONObject("importParameters").has("project")) {
				projectName = metadata.getJSONObject("importParameters").getString("project");
			}

			try {
				querier = getQuerier(projectName);
			}catch(Exception error) {
				throw new IllegalArgumentException(String.format("The project database %s doesn't exist", projectName));
			}
			querySingleton.setQuerier(querier);
			
			
			datasets = metadata.getJSONArray("datasets");
		}

		/*------------------------------------------------------------------------------------------------------------*/

		System.out.println("Starting importation...");

		if(!onlyDataset) {
			workflow = metadata.getJSONObject("workflow");
			invocation = metadata.getJSONObject("invocation");
			tools = metadata.getJSONObject("tools");

			//First of all, we need to check if the invocation doesn't exist yet.
			if(querySingleton.exists("workflowInvocation", Arrays.asList("galaxyInvocationID"), Arrays.asList(invocation.getString("ID")))) {
				throw new UnsupportedOperationException("This workflow invocation already exists in the database.");
			}

			// Insert the workflow if it doesn't exist
			String workflowID = querySingleton.insert(
				"workflow",
				Arrays.asList("galaxyWorkflowID", "name", "description", "groupID"),
				Arrays.asList(
					workflow.getString("ID"), 
					workflow.getString("name"), 
					workflow.getString("description"), 
					"0"
				),
				true
			);

			String workflowVersionID = querySingleton.insert(
				"workflowVersion",
				Arrays.asList("workflowID", "version"),
				Arrays.asList(workflowID, Integer.toString(workflow.getInt("version"))),
				true
			);

			String userID = Integer.toString(invocation.getInt("userID"));
			// Insert the user if needed
			querySingleton.insert(
				"user", 
				Arrays.asList("galaxyUserID"), 
				Arrays.asList(userID),
				true
			);

			// Insert the workflow invocation
			querySingleton.insert(
				"workflowInvocation",
				Arrays.asList("galaxyInvocationID", "executionDatetime", "workflowVersionID", "galaxyUserID"),
				Arrays.asList(
					invocation.getString("ID"), 
					invocation.getString("datetime"), 
					workflowVersionID, 
					userID
				),
				false //Don't need the ID
			);


			// Insert the tools
			Iterator<String> toolsKeys = tools.keys();
			while(toolsKeys.hasNext()) { // For each key in JSONObject tools
				String stepID = toolsKeys.next();
				JSONObject tool = tools.getJSONObject(stepID);
				// We need to insert the tables linked to toolExecution in the first place before inserting it

				// Insert software if there is one and not already added
				boolean hasSoftware = tool.has("tool_url_git_repository") && tool.has("tool_git_commit_ID");
				String softwareID = null;

				if(hasSoftware) {
					String urlGitRepo = tool.getString("tool_url_git_repository");
					String gitCommitID = tool.getString("tool_git_commit_ID");
					String gitTag = tool.getString("tool_git_tag");

					softwareID = querySingleton.insert(
						"software",
						Arrays.asList("repoURL", "commitID", "tag"),
						Arrays.asList(urlGitRepo, gitCommitID, gitTag),
						true
					);
					
				} 

				// tool
				String toolID = querySingleton.insert(
					"tool",
					Arrays.asList("toolID", "toolName", "description", "version"),
					Arrays.asList(
						tool.getString("ID"),
						"", 
						tool.getString("description"), 
						tool.getString("version")
					),
					true
				);
				

				// ClusterParams
				String clusterParamsID = querySingleton.insert(
					"clusterParams",
					Arrays.asList("parametersDict"),
					Arrays.asList(tool.getJSONObject("job_resource").toString()),
					true
				);

				// toolExecution
				String toolExecutionID = querySingleton.insert(
					"toolExecution", 
					Arrays.asList("workflowInvocationID", "toolID", "clusterParamsID", "softwareID"),
					Arrays.asList(
						invocation.getString("ID"), 
						toolID, 
						clusterParamsID,
						softwareID
					)	
				);

				/* Parameters */
				JSONObject parameters = tool.getJSONObject("parameters");

				Iterator<String> parametersKeys = parameters.keys();
				while(parametersKeys.hasNext()) { // For each key in JSONObject parameters
					String parameterKey = parametersKeys.next();
					JSONObject parameter = parameters.getJSONObject(parameterKey); // We got the current input

					String paramType = parameter.getString("type");
					String paramValue = null;
					if(paramType.equals("integer")) {
						paramValue = Integer.toString(parameter.getInt("value"));
					}else if(paramType.equals("float")) {
						paramValue = Float.toString(parameter.getFloat("value"));
					}else { // String, select & boolean
						paramValue = parameter.getString("value");
					}

					if(paramValue != null) {
						querySingleton.insert(
							"parameter",
							Arrays.asList("name", "type", "intValue", "floatValue", "stringValue", "toolExecutionID"),
							Arrays.asList(
								parameterKey, 
								paramType, 
								paramType.equals("integer") 	? paramValue : null,
								paramType.equals("float")   	? paramValue : null,
								!paramType.equals("integer") || !paramType.equals("float") ? paramValue : null,
								toolExecutionID
							)
						);
					}
				}

				/* Inputs */
				JSONObject inputs = tool.getJSONObject("inputs");
				Iterator<String> inputsKeys = inputs.keys();
				while(inputsKeys.hasNext()) { // For each key in JSONObject parameters
					String inputName = inputsKeys.next();
					JSONObject input = inputs.getJSONObject(inputName); // We got the current input (data name or collection name)

					String inputType = input.getString("type");

					//Data collection
					if(inputType.equals("data_collection")) {
						String galaxyCollectionID = input.getString("HDCA_id");
						JSONArray items = input.getJSONArray("items");
						for(int i=0; i < items.length(); i++) {
							JSONObject item = items.getJSONObject(i);
							String galaxyDatasetID = item.getString("HDA_id");
							String path = item.getString("path");
							String fileSize = item.getString("size");

							String collectionID = querySingleton.insert(
								"collection", 
								Arrays.asList("galaxyDatasetCollectionID", "name"), 
								Arrays.asList(galaxyCollectionID, inputName)
							);

							// Get or insertion
							String fileID = querySingleton.addFile(path, inputName, fileSize, galaxyDatasetID, galaxyCollectionID);

							querySingleton.insert(
								"input",
								Arrays.asList("toolExecutionID", "fileID"),
								Arrays.asList(
									toolExecutionID,
									fileID
								)
							);
						}
					}else{ //Single Data
						String inputPath = input.getString("path");
						String galaxyDatasetID = input.getString("HDA_id");
						String fileSize = input.getString("size");
						
						String fileID = querySingleton.addFile(inputPath, inputName, fileSize, galaxyDatasetID, null);					

						querySingleton.insert(
							"input",
							Arrays.asList("toolExecutionID", "fileID"),
							Arrays.asList(
								toolExecutionID,
								fileID
							)
						);
					}
				}

				JSONObject outputs = tool.getJSONObject("outputs");

				Iterator<String> outputsNames = outputs.keys();
				while(outputsNames.hasNext()) { // For each key in JSONObject ouputs
					String outputName = outputsNames.next();
					JSONObject output = outputs.getJSONObject(outputName); // We got the current output (data name or collection name)
					String outputType = output.getString("type");

					//Data collection
					if(outputType.equals("data_collection")) {
						String galaxyCollectionID = output.getString("HDCA_id");
						JSONArray items = output.getJSONArray("items");
						for(int i=0; i < items.length(); i++) {
							JSONObject item = items.getJSONObject(i);
							String galaxyDatasetID = item.getString("HDA_id");
							String path = item.getString("path");
							String size = item.getString("size");

							String collectionID = querySingleton.insert(
								"collection", 
								Arrays.asList("galaxyDatasetCollectionID", "name"), 
								Arrays.asList(galaxyCollectionID, outputName)
							);

							// Get or insertion
							String fileID = querySingleton.addFile(path, outputName, size, galaxyDatasetID, galaxyCollectionID);

							querySingleton.insert(
								"output",
								Arrays.asList("toolExecutionID", "fileID"),
								Arrays.asList(
									toolExecutionID,
									fileID
								)
							);
						}
					}else{ //Single Data
						String outputPath = output.getString("path");
						String galaxyDatasetID = output.getString("HDA_id");
						String size = output.getString("size");
						
						String fileID = querySingleton.addFile(outputPath, outputName, size, galaxyDatasetID, null);					

						querySingleton.insert(
							"output",
							Arrays.asList("toolExecutionID", "fileID"),
							Arrays.asList(
								toolExecutionID,
								fileID
							)
						);
					}
				}

			}
		}

		/* ---							 --- */
		/* --- Taking care of dataset(s) --- */
		/* ---							 --- */

		for(int d=0; d < datasets.length(); d++) {
			JSONObject dataset = datasets.getJSONObject(d);

			// Check that there is no dataset with the same name and version (= duplicate)
			String datasetName = dataset.getString("name");
			String datasetDescription = dataset.has("description")?dataset.getString("description"):null;
			String datasetVersion = dataset.getString("version");
			
			Boolean isDuplicate = querySingleton.exists("dataset", Arrays.asList("name", "version"), Arrays.asList(datasetName, datasetVersion));
			if(isDuplicate) {
				return new StringBuilder("<error><![CDATA[There is already a dataset with the same name and version]]></error>");
			}

			String newDatasetID = null;
			String invocationID = invocation!=null&&invocation.has("ID")?invocation.getString("ID"):null; //No invocation if only a dataset is inserted

			if(dataset.has("_specializedMetadata_")) { // => "métadonnées métiers"
				JSONArray allSpecializedMetadata = dataset.getJSONArray("_specializedMetadata_");
				newDatasetID = querySingleton.insertSpecializedMetadata(allSpecializedMetadata, invocationID);
			}else if(dataset.has("_project_")) { // If the project has a special class for insertion....
				try {
					String className = String.format("net.hep.ami.customInsertionProjects.%s", dataset.getString("_project_"));
					Class<?> insertionClass = (Class<?>) java.lang.Class.forName(String.format(className));
					Object insertionClassInstance = insertionClass
						.getDeclaredConstructor(JSONObject.class, String.class)
						.newInstance(dataset, invocationID);
					
					Method insertionMethod = insertionClass.getDeclaredMethod("insertion");
					newDatasetID = (String) insertionMethod.invoke(insertionClassInstance);
				}catch(Exception error) {
					throw error;
				}
			}

			if(newDatasetID == null || newDatasetID.strip().equals("")) {
				throw new UnsupportedOperationException("Can't insert dataset in the DB : the returned ID is null.");
			}

			if(dataset.has("_customMetadata_")) { // Metadata stocked in a table not implemented in the base architecture
				JSONObject customMetadata = dataset.getJSONObject("_customMetadata_");
				querySingleton.insertCustomMetadata(customMetadata, newDatasetID);
			}

			// Has parent(s) ?
			if(dataset.has("parents")) {
				JSONArray datasetParents = dataset.getJSONArray("parents");

				// We need to get the right parent datasetID. Get it from each path the user defined in the workflow.
				// For each input or parameter (with a path) with a step ID (= user defined value), get the dataset name = ID if existing
				HashMap<String, String> datasetsUsedByIP = new HashMap<>(); // IP = input or parameter

				Iterator<String> toolsKeys = tools.keys();
				while(toolsKeys.hasNext()) { // For each key in JSONObject tools
					String stepID = toolsKeys.next();
					JSONObject tool = tools.getJSONObject(stepID);
					
					/* Parameters */
					JSONObject parameters = tool.getJSONObject("parameters");

					Iterator<String> parametersKeys = parameters.keys();
					while(parametersKeys.hasNext()) { // For each key in JSONObject parameters
						String parameterKey = parametersKeys.next();
						JSONObject parameter = parameters.getJSONObject(parameterKey); // We got the current input

						// Is a string and is a path to a file...
						if(parameter.getString("type").equals("text") && Utils.isAFilePath(parameter.getString("value"))) {
							String path = parameter.getString("value");
							String SQLGetDatasetParentID = String.format(
								"SELECT dataset.ID AS datasetID, dataset.name AS datasetName " +
								"FROM dataset " +
								"INNER JOIN dataset_file ON dataset_file.datasetID = dataset.ID " +
								"INNER JOIN physicalFile ON physicalFile.fileID = dataset_file.fileID " +
								"WHERE physicalFile.path = '%s' LIMIT 1;", 
							path);
							RowSet rowSet = querier.executeSQLQuery("dataset", SQLGetDatasetParentID);
							List<Row> rows = rowSet.getAll();
							if(rows.size() > 0) {
								Row row = rows.get(0);
								String parentName = row.getValue("datasetName");
								String parentID = row.getValue("datasetID");
								datasetsUsedByIP.put(parentName, parentID);
							}
						}
					}

					/* Inputs */
					JSONObject inputs = tool.getJSONObject("inputs");
					Iterator<String> inputsKeys = inputs.keys();
					while(inputsKeys.hasNext()) { // For each key in JSONObject parameters
						String inputName = inputsKeys.next();
						JSONObject input = inputs.getJSONObject(inputName); // We got the current input (data name or collection name)

						// Is a single data
						if(input.getString("type").equals("data")) {
							String path = input.getString("path");
							String SQLGetDatasetParentID = String.format(
								"SELECT dataset.ID AS datasetID, dataset.name AS datasetName " +
								"FROM dataset " +
								"INNER JOIN dataset_file ON dataset_file.datasetID = dataset.ID " +
								"INNER JOIN physicalFile ON physicalFile.fileID = dataset_file.fileID " +
								"WHERE physicalFile.path = '%s' LIMIT 1;", 
							path);
							RowSet rowSet = querier.executeSQLQuery("dataset", SQLGetDatasetParentID);
							List<Row> rows = rowSet.getAll();
							if(rows.size() > 0) {
								Row row = rows.get(0); // Only get the first file <-- Bad idea ?
								String parentName = row.getValue("datasetName");
								String parentID = row.getValue("datasetID");
								datasetsUsedByIP.put(parentName, parentID);
							}
						}
					}
				}

				for(int p=0; p < datasetParents.length(); p++) {
					// In the user JSON, we only have the dataset name
					String parentName = datasetParents.getString(p);
					
					// But thanks to the files path in the inputs/parameters, we can retrieve the exact dataset ID associated with
					String parentID = datasetsUsedByIP.get(parentName);

					if(parentID != null) {
						querySingleton.setDatasetParent(newDatasetID, parentID);
					}else {
						throw new UnsupportedOperationException(String.format("The parent dataset '%s' doesn't exist.", parentName));
					}
				}
			}

			// Files in dataset
			JSONArray files = dataset.getJSONArray("files");
			for(int f=0; f < files.length(); f++) {
				JSONObject file = files.getJSONObject(f);

				//File & PhysicalFile
				String filePath = file.getString("path");
				String filename = file.has("name")?file.getString("name"):null;
				String fileSize = file.has("size")?file.getString("size"):null;
				String fileID = querySingleton.addFile(filePath, filename, fileSize); // Should find a file if it's a previous output. If not, no Galaxy Dataset ID

				//Site & SiteParam
				String fileSiteName = file.has("siteName")?file.getString("siteName"):null;

				JSONArray fileSiteParams = file.has("siteParams")?file.getJSONArray("siteParams"):null;

				if(fileSiteName != null && fileSiteParams != null) {
					querySingleton.setFileSite(fileID, fileSiteName, fileSiteParams);
				}

				//Tag
				String tag = file.has("tag")?file.getString("tag"):null;
				String tagID = null;
				if(tag != null) {
					tagID = querySingleton.insert(
						"tag",
						Arrays.asList("name"),
						Arrays.asList(file.getString("tag")),
						true
					);
				}

				querySingleton.insert(
					"dataset_file",
					Arrays.asList("datasetID", "fileID", "tagID"),
					Arrays.asList(newDatasetID, fileID, tagID),
					true
				);
			}

			// Commit the new rows !
			querySingleton.commit();
			
			/* Update its children */
			String SQLGetChildrenDatasets = String.format(
				"SELECT DSChild.ID " +
				"FROM dataset " +
				"INNER JOIN provenance ON provenance.sourceDatasetID = dataset.ID " +
				"INNER JOIN dataset DSChild ON DSChild.ID = provenance.destinationDatasetID " +
				"WHERE dataset.name = '%1$s' AND DSChild.status = 'canBeUpdated';",
				datasetName
			);

			RowSet childrenRowSet = querier.executeSQLQuery("dataset", SQLGetChildrenDatasets);
			List<Row> childrenRows = childrenRowSet.getAll();
			String galaxyAPIKey = null;
			if(childrenRows.size() > 0) {
				// Get user API key
				String currentUserName = m_AMIUser;

				RouterQuerier routerQuerier = new RouterQuerier();
				List<Row> rowsUserJSON = routerQuerier.executeSQLQuery("router_user", String.format("SELECT json FROM router_user WHERE router_user.AMIUser = '%s' LIMIT 1", currentUserName)).getAll();
				if(rowsUserJSON.size() == 0) {
					throw new Exception("Cannot update the children dataset, there is an error while getting the user info.");
				}
				Row rowUserJSON = rowsUserJSON.get(0);
				JSONObject userJSON = new JSONObject(rowUserJSON.getValue("json"));
				if(!userJSON.has("galaxyAPIKey")) {
					throw new Exception("Cannot update the children dataset, there is an error while getting the user Galaxy API key.");
				}
				galaxyAPIKey = userJSON.getString("galaxyAPIKey");

				if(galaxyAPIKey != null && galaxyAPIKey.strip().equals("")) {
					throw new IllegalArgumentException(String.format("No API key was found for the user (%s)", currentUserName));
				}
			
				for(Row rowChild : childrenRows) {
					String childID = rowChild.getValue(0);

					// No need to check if auto update is activated, the AutoUpdateDataset cmd does already that
					System.out.println(CommandSingleton.executeCommand(String.format(
							"AutoUpdateDataset -datasetID=\"%1$s\" -userAPIKey=\"%2$s\"",
							childID,
							galaxyAPIKey
						), false // Check roles
					));
				}
			}
		}

		/*------------------------------------------------------------------------------------------------------------*/

		return new StringBuilder("<info><![CDATA[Metadata inserted with success]]></info>");
	}

	/*----------------------------------------------------------------------------------------------------------------*/

	@NotNull
	@Contract(pure = true)
	public static String help()
	{
		return "Import Galaxy Metadata after a workflow invocation.";
	}

	/*----------------------------------------------------------------------------------------------------------------*/

	@NotNull
	@Contract(pure = true)
	public static String usage()
	{
		return "-project=\"\" -encryptedMetadata=\"\" | -b64Metadata=\"\"";
	}

	/*----------------------------------------------------------------------------------------------------------------*/
}
