package net.hep.ami;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import net.hep.ami.data.Row;
import net.hep.ami.data.RowSet;
import net.hep.ami.data.Update;
import net.hep.ami.jdbc.Querier;
import net.hep.ami.jdbc.reflexion.SchemaSingleton;

import org.json.*;

public enum QuerySingleton {
	INSTANCE;
	
	private Querier querier;

	public QuerySingleton getInstance() {
        return INSTANCE;
    }

	/* Set the querier for this singleton. If isProject is true, the catalog used by the querier must be one of the projects (AMI_Project...) */
	public void setQuerier(Querier querier, boolean isProject) throws Exception {
		String databaseName = querier.getInternalCatalog();
		if(!isProject || getProjectsDatabaseInternalName().contains(databaseName)) {
			this.querier = querier;
		}else {
			throw new IllegalArgumentException(String.format("The project database %s doesn't exist", databaseName));
		}
	}

	// isProject is true by default
	public void setQuerier(Querier querier) throws Exception {
		setQuerier(querier, true);
	}

	// Generate a SQL insertion from the 3 first arguments. If uniqueRow is true, check if a row with identical data exists. If it does, directly return the row ID
    public String insert(String tableName, List<String> columnsName, List<String> columnsValue, boolean uniqueRow) throws Exception {
		int columnsNameSize = columnsName.size();
		int columnsValueSize = columnsValue.size();
		if(columnsNameSize != columnsValueSize) {
			throw new IllegalArgumentException(String.format("Number of cols (%s) doesn't match the number of values (%s)", columnsNameSize, columnsValueSize));
		}

		if(uniqueRow) { // Are the values already inserted ?
			List<Row> rows = getColsValue(tableName, columnsName, columnsValue);
			if(rows != null) {
				return rows.get(0).getValue(0); //Should be the primary key but maybe we need something more robust.
			}
		}

		// Transform columns name into a string
		String columnsNameStr = columnsName.stream()
			.map(s -> '`' + s + '`')
			.collect(Collectors.joining(", "));


		// Same for the values
		String columnsValueStr = columnsValue.stream()
			.map(val -> {
				if(val == null) return "null";
				if(val instanceof String) return String.format("'%1$s'", val.replace("'", "''")); // (Escape the single quotes)
				return val.toString();
			})
			.collect(Collectors.joining(", ")); 


		// Format the SQL string for insertion
		String sqlInsertion = String.format("INSERT INTO `%1$s` (%2$s) VALUES (%3$s)", tableName, columnsNameStr, columnsValueStr);

		// Execute the statement
		try(
			PreparedStatement statement = querier.sqlPreparedStatement(tableName, sqlInsertion, true, null, true);
		) {
			int nbOfUpdatedRows = statement.executeUpdate();

			if(nbOfUpdatedRows == 0) {
				throw new SQLException("Insertion failed");
			}

			try(ResultSet resultSet = statement.getGeneratedKeys()) {
				if(resultSet.next()) {
					return resultSet.getString(1);
				}else {
					return columnsValue.get(0); //Should be the primary key but maybe we need something more robust.
				}
			} catch(Exception error) {
				throw new SQLException(String.format("Can't retrieve the generated key for the insertion command (%1$s) : %2$s", sqlInsertion, error));
			}
		
		} catch(Exception error) {
			throw new SQLException(String.format("Can't execute the insertion (%1$s) : %2$s", sqlInsertion, error));
		}
	}


	public String insert(String tableName, List<String> columnsName, List<String> columnsValue) throws Exception {
		return insert(tableName, columnsName, columnsValue, true);
	}


	// Get the value of a specific col, with a simple where condition
	public String getColValue(String tableName, String selectedCol, String getByCol, String valueCol) throws Exception {
		RowSet query = querier.executeSQLQuery(tableName, String.format("SELECT %2$s FROM `%1$s` WHERE UPPER(%1$s.%3$s) = '%4$S' LIMIT 1", tableName, selectedCol, getByCol, valueCol));
		List<Row> rows = query.getAll();
		return rows.size() > 0 ? rows.get(0).getValue(0) : null;
	}


	/* TODO : remove mode once getGeneratedKey() is working */
	// Get the values of several cols, with multiple where conditions
	public List<Row> getColsValue(String tableName, List<String> whereColsName, List<String> whereColsValue, List<String> selectedColsName, String mode) throws Exception {
		//Compare size
		int whereColsNameSize = whereColsName.size();
		int whereColsValueSize = whereColsValue.size();

		if(whereColsName.size() != whereColsValue.size()) {
			throw new IllegalArgumentException(String.format("Number of cols (%s) doesn't match the number of values (%s)", whereColsNameSize, whereColsValueSize));
		}

		String whereClause = "";
		for(int i=0; i<whereColsName.size(); i++) { // To optimize...
			String colValue = whereColsValue.get(i);
			if(colValue != null) {
				if(colValue.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{1,}$")) { //It's a datetime
					colValue = String.format("CAST('%s' AS datetime)", colValue);
				}else{ //A string (works also with number)
					colValue = String.format("'%s'", colValue.replace("'", "''")); //Escape the single quotes
				}
			}

			whereClause += String.format("`%1$s` = %2$s", whereColsName.get(i), colValue);
			if(i<whereColsNameSize-1) {
				whereClause += " and ";
			}
		}

		String selectedCols = selectedColsName!=null?String.join(", ", selectedColsName):"*";

		String sqlSelect = null;

		RowSet rowSet = null;
		if(mode.equals("sql")) {
			sqlSelect = String.format("SELECT %1$s FROM %2$s WHERE %3$s LIMIT 1", selectedCols, tableName, whereClause);
			rowSet = querier.executeSQLQuery(tableName, sqlSelect);
		}else if(mode.equals("mql")) {
			sqlSelect = String.format("SELECT %1$s WHERE %2$s LIMIT 1", selectedCols.replace("`", ""), whereClause.replace("`", ""));
			rowSet = querier.executeMQLQuery(tableName, sqlSelect);
		}
		List<Row> rows = rowSet.getAll(); //GetFirst ?
		return rows.size() > 0 ? rows : null;
	}

		public List<Row> getColsValue(String tableName, List<String> whereColsName, List<String> whereColsValue, List<String> selectedColsName) throws Exception {
		return getColsValue(tableName, whereColsName, whereColsValue, selectedColsName, "sql");
	}

	public List<Row> getColsValue(String tableName, List<String> whereColsName, List<String> whereColsValue) throws Exception {
		return getColsValue(tableName, whereColsName, whereColsValue, null, "sql");
	}


	// Check if a row with some values exists
	public boolean exists(String tableName, List<String> whereColsName, List<String> whereColsValue) throws Exception {
		List<Row> rows = getColsValue(tableName, whereColsName, whereColsValue);
		return rows != null;
	}

	// Help inserting a file in the right database tables
	public String addFile(String filePath, String filename, String fileSize, String galaxyDatasetID, String galaxyCollectionID) throws Exception {
		String fileID = getColValue("physicalFile", "fileID", "path", filePath);
		String siteID = null;
		if(fileID == null) {
			//File
			fileID = insert(
				"file",
				Arrays.asList("GUID", "name", "size", "galaxyDatasetID", "galaxyCollectionID"),
				Arrays.asList(
					java.util.UUID.randomUUID().toString(),
					filename,
					fileSize,
					galaxyDatasetID,
					galaxyCollectionID
				)
			);

			//PhysicalFile
			insert(
				"physicalFile",
				Arrays.asList("path", "fileID"),
				Arrays.asList(
					filePath,
					fileID
				)
			);
							
		}else if(galaxyDatasetID != null){ // There is already a physicalFile & galaxyDatasetID is not null
			// But maybe the file inserted in the BDD has no galaxyDatasetID. (In the case the file insertion was in a DS but didn't come from an output)
			Boolean rowHasNoGalaxyDatasetID = getColValue("file", "galaxyDatasetID", "ID", fileID) == null;

			if(rowHasNoGalaxyDatasetID) {
				String sqlUpdateFileRow = String.format("UPDATE file SET name='%1$s', size='%2$s', galaxyDatasetID='%3$s', galaxyCollectionID='%4$s' WHERE ID='%5$s'", 
				filename, fileSize, galaxyDatasetID, galaxyCollectionID, fileID);
				querier.executeSQLQuery("file", sqlUpdateFileRow);
			}
		}

		return fileID;
	}

	public String addFile(String filePath, String filename, String fileSize) throws Exception {
		return addFile(filePath, filename, fileSize, null, null);
	}

	// Set information about a file
	public void setFileSite(String fileID, String siteName, JSONArray siteParams) throws Exception {
		//Get site ID
		String siteID = getColValue("site", "ID", "name", siteName);
		if(siteID == null) {
			throw new IllegalArgumentException(String.format("The site %s doesn't exist in the database. Please insert it first.", siteName));
		}

		//Insert site params
		for(int p=0; p < siteParams.length(); p++) {
			JSONObject siteParam = siteParams.getJSONObject(p);

			String paramType = "string";
			String paramValue = null;
			String paramName = siteParam.getString("name");
			String paramValueColName = "stringValue";

			if(siteParam.has("integerValue")) {
				paramType = "integer";
				paramValueColName = "integerValue";
			}else if(siteParam.has("floatValue")) {
				paramType = "float";
				paramValueColName = "floatValue";
			}else if(siteParam.has("timeValue")) {
				paramType = "time";
				paramValueColName = "timeValue";
			}else if(siteParam.has("blobValue")) {
				paramType = "blob";
				paramValueColName = "blobValue";
			}
			paramValue = siteParam.getString(paramValueColName);


			siteID = insert(
				"siteParam",
				Arrays.asList("siteID", "name", "type", paramValueColName),
				Arrays.asList(siteID, paramName, paramType, paramValue),
				true
			);
			
			// Format the SQL string for insertion
			String sqlUpdate = String.format("UPDATE physicalFile SET siteID='%1$s' WHERE fileID='%2$s'", siteID, fileID);

			// Execute the statement
			try(
				PreparedStatement statement = querier.sqlPreparedStatement("physicalFile", sqlUpdate, true, null, true);
			) {
				int nbOfUpdatedRows = statement.executeUpdate();

				if(nbOfUpdatedRows == 0) {
					throw new SQLException("Update failed");
				}
			}
		}
		
	}

	// SpecializedMetadata = "métadonnées métiers", make the right insertion.
	// The user can also use SQL or MQL (SELECT, UPDATE or INSERT) directly in its JSON, for a specific col. The ID returned will be used to make the insertion
	public String insertSpecializedMetadata(JSONArray allSpecializedMetadata, String invocationID) throws Exception { // Contains the dataset
		String datasetID = null;
		
		Pattern patternQuery = Pattern.compile("(MQL:|SQL:)(.+)", Pattern.CASE_INSENSITIVE);
		Pattern patternTableMQL = Pattern.compile("(SELECT|UPDATE|INSERT)(\\s*\\(?)(\\w+)\\.", Pattern.CASE_INSENSITIVE);
		Pattern patternTableSQL = Pattern.compile("(SELECT|UPDATE|INSERT).+FROM\\s(\\w+)", Pattern.CASE_INSENSITIVE); 

		for(int sm=0; sm < allSpecializedMetadata.length(); sm++) { // For each key/table in JSONObject specializedMetadata (process in order)
			JSONObject specializedMetadata = allSpecializedMetadata.getJSONObject(sm); // We got the current metadata
			String specializedMetadataTable = specializedMetadata.getString("table");
			JSONObject specializedMetadataCols = specializedMetadata.getJSONObject("cols");
			boolean mustBeUnique = specializedMetadata.has("_uniq_") && specializedMetadata.getString("_uniq_").equals("true"); // Insert only if the values doesn't already exist in the DB

			Iterator<String> params = specializedMetadataCols.keys();

			List<String> columnsName = new ArrayList<String>();
			List<String> columnsValue = new ArrayList<String>();

			while(params.hasNext()) { // For each param in the table
				String col = params.next();
				String value = specializedMetadataCols.getString(col);

				Matcher DBCommandMatcher = patternQuery.matcher(value);

				/* It's a query/update and not a classical primitive value */
				if(DBCommandMatcher.find()) { 
					String DBCommand = DBCommandMatcher.group(2).strip(); // Get the cmd

					boolean isQuery = DBCommand.toUpperCase().startsWith("SELECT"); // Is a SELECT cmd ?
					boolean isUpdate = DBCommand.toUpperCase().startsWith("INSERT") || DBCommand.toUpperCase().startsWith("UPDATE");

					if(!isQuery && !isUpdate) {
						throw new IllegalArgumentException(String.format("The database command must be a SELECT, an INSERT or an UPDATE : %s.", DBCommand));
					}

					boolean isSQL = value.startsWith("SQL:");
					boolean isMQL = value.startsWith("MQL:");

					if(!isSQL && !isMQL) { // Should not be triggered since we use a regex to check that before...
						throw new IllegalArgumentException(String.format("Can't recognize if it's MQL or SQL: %s. (The parameter value in the JSON should starts by either 'MQL:' or 'SQL:')", DBCommand));
					}

					Matcher tableMatcher;
					String table = null;
					RowSet rowSet = null;
					Update update = null;

					if(isSQL) {
						tableMatcher = patternTableSQL.matcher(DBCommand);
						if(!tableMatcher.find()) {
							throw new IllegalArgumentException(
								String.format("No table found in the SQL: %s", DBCommand)
							);
						}
						table = tableMatcher.group(2).strip(); // Get the table name from the SQL

						if(isQuery) {
							rowSet = querier.executeSQLQuery(table, DBCommand);
						}else if(isUpdate){
							update = querier.executeSQLUpdate(table, DBCommand);
						}
					/* ----  */
					}else if(isMQL) {
						tableMatcher = patternTableMQL.matcher(DBCommand);
						if(!tableMatcher.find()) {
							throw new IllegalArgumentException(
								String.format("No table found in the MQL: %s", DBCommand)
							);
						}
						table = tableMatcher.group(3).strip(); // Get the table name from the MQL

						if(isQuery) {
							rowSet = querier.executeMQLQuery(table, DBCommand);
						}else if(isUpdate) {
							update = querier.executeMQLUpdate(table, DBCommand);
						}

					}

					if(rowSet != null) { // It was a query and we have a result
						List<Row> rows = rowSet.getAll();
						value = rows.isEmpty()?null:rows.get(0).getValue(0); // We suppose that the ID is the first col returned by the query

					}else if(update != null && table != null) { //It was an update/insertion
						// Workaround while waiting for getGeneratedKey() to work
						if(DBCommand.startsWith("INSERT")) {
							Pattern patternInsert = Pattern.compile("INSERT\\s*\\((.+)\\)\\s*VALUES\\s*\\((.+)\\)", Pattern.CASE_INSENSITIVE);
							Matcher matcher = patternInsert.matcher(DBCommand);

							if(matcher.find()) {
								List<String> cols = Arrays.stream(matcher.group(1).split("\\s*,\\s*"))
									.toList();

								List<String> values = Arrays.stream(matcher.group(2).split("\\s*,\\s*"))
									.map(v -> v.replaceAll("^'|'$", ""))
									.toList();

								value = getColsValue(table, cols, values, null, isMQL?"mql":"sql").get(0).getValue(0); // We suppose that the ID is the first col of the first row
							}

						}else if(DBCommand.startsWith("UPDATE")) {
							Pattern patternUpdate = Pattern.compile("WHERE\\s*(.+)");
							Matcher matcher = patternUpdate.matcher(DBCommand);

							if(matcher.find()) {
								if(isSQL) {
									String query = String.format("SELECT * FROM %1$s WHERE %2$s", table, matcher.group(1).strip());
									value = querier.executeSQLQuery(table, query).getFirst().getValue(0); // We suppose that the ID is the first col of the first row
								}else if(isMQL) {
									String query = String.format("SELECT * WHERE %1$s", matcher.group(1).strip());
									value = querier.executeMQLQuery(table, query).getFirst().getValue(0);
								}
							}
						}
					}
				}

				columnsName.add(col);
				columnsValue.add(value);
			}

			if(specializedMetadataTable.equals("dataset")) {
				columnsName.add("workflowInvocationID");
				columnsValue.add(invocationID);
			}
									
			String ID = insert(specializedMetadataTable, columnsName, columnsValue, mustBeUnique);
			if(specializedMetadataTable.equals("dataset")) { // We need to keep the new datasetID for later
				datasetID = ID;
			}
		}

		if(datasetID == null) {
			throw new Exception("No dataset inserted");
		}else {
			return datasetID;
		}
	}

	// Custom Metadata = tables attached to "dataset_customMetadata"
	public void insertCustomMetadata(JSONObject customMetadata, String datasetID) throws Exception {
		JSONArray allGenericCustomMetadata = customMetadata.getJSONArray("_generic_");
		JSONArray allNotGenericCustomMetadata = customMetadata.getJSONArray("_notGeneric_");

		for(int gcm=0; gcm < allGenericCustomMetadata.length(); gcm++) { // Generic table
			JSONObject genericCustomMetadata = allGenericCustomMetadata.getJSONObject(gcm);
			String name = genericCustomMetadata.getString("name");
			String description = genericCustomMetadata.has("description")?genericCustomMetadata.getString("description"):null;

			// The metadata is either a int/float/string
			String value = genericCustomMetadata.getString("value");
			String type = genericCustomMetadata.getString("type");

			String intValue = null;
			String floatValue = null;
			String stringValue = null;

			if(type == "integer") {
				intValue = value;
			}else if(type == "float") {
				floatValue = value;
			}else {
				stringValue = value;
			}

			String genericMetadataID = insert(
				"genericMetadata",
				Arrays.asList("type", "intValue", "floatValue", "stringValue"),
				Arrays.asList(
					type,
					intValue,
					floatValue,
					stringValue
				)
			);

			insert(
				"dataset_customMetadata", 
				Arrays.asList("datasetID", "name", "description", "genericMetadataID"), 
				Arrays.asList(datasetID, name, description, genericMetadataID)
			);
		}

		for(int ngcm=0; ngcm < allNotGenericCustomMetadata.length(); ngcm++) { // A table created after the initial database creation
			JSONObject notGenericCustomMetadata = allNotGenericCustomMetadata.getJSONObject(ngcm);

			List<String> cols = new ArrayList<String>(); // Columns name
			List<String> values = new ArrayList<String>(); // Columns value

			String tableName = notGenericCustomMetadata.getString("table");
			JSONArray rows = notGenericCustomMetadata.getJSONArray("rows");

			for(int c = 0; c < rows.length(); c++) { //for each col
				JSONObject col = rows.getJSONObject(c);
				String key = col.keys().next();
				cols.add(key);

				Object rawValue = col.get(key);
				String value;

				if(rawValue == null || rawValue.equals(JSONObject.NULL)) {
					value = null;
				}else if(rawValue instanceof Boolean) {
					value = (Boolean) rawValue ? "1" : "0";
				}else {
					value = rawValue.toString();
				}

				values.add(value);
			}

			// Insert in the "special" table
			String notGenericCustomMetadataID = insert(
				tableName,
				cols,
				values
			);

			insert(
				"dataset_customMetadata",
				Arrays.asList("datasetID", "name", "description", String.format("%sID", tableName)),
				Arrays.asList(
					datasetID,
					notGenericCustomMetadata.getString("name"),
					notGenericCustomMetadata.has("description")?notGenericCustomMetadata.getString("description"):null,
					notGenericCustomMetadataID
				)
			);
		}
	}


	// To link two datasets together (a dataset made another one)
	public void setDatasetParent(String childID, String parentID) throws Exception {

		if(childID.equals(parentID)) {
			throw new IllegalArgumentException("The parent cannot also be the child.");
		}

		boolean isParentExisting = exists("dataset", Arrays.asList("ID"), Arrays.asList(parentID)); 
		boolean isChildExisting = exists("dataset", Arrays.asList("ID"), Arrays.asList(childID)); 

		if(!isParentExisting) {
			throw new IllegalArgumentException("The parent doesn't exist.");
		}

		if(!isChildExisting) {
			throw new IllegalArgumentException("The child doesn't exist.");
		}

		insert("provenance", Arrays.asList("sourceDatasetID", "destinationDatasetID"), Arrays.asList(parentID, childID));

	}

	// Update datasets status after an insertion (if a similar and older dataset exists, it's outdated. The children of the outdated ones are marked as "canBeUpdated")
	public void updateDatasetStatus(String datasetID, String datasetName, String datasetVersion) throws Exception {
		//1) Outdate the previous datasets
		List<String> oldDatasetsID = new ArrayList<String>();
		String SQLGetOldDatasets = String.format("SELECT ID FROM dataset WHERE dataset.name = '%1$s' AND dataset.version < '%2$s';", datasetName, datasetVersion); //Use getCol()
		RowSet query = querier.executeSQLQuery("dataset", SQLGetOldDatasets);
		List<Row> rows = query.getAll();

		if(rows.size() > 0) {
			for(int i=0; i<rows.size(); i++) {
				oldDatasetsID.add(rows.get(i).getValue("ID"));
			}

			String SQLSetOutdatedDS = String.format("UPDATE dataset SET status='outdated' WHERE ID IN (%s)", String.join(", ", oldDatasetsID)); 
			querier.executeSQLUpdate("dataset", SQLSetOutdatedDS);

			//2) Put its children status as "needUpdate"	
			String SQLSetNeedUpdateDS = String.format(
				"UPDATE dataset " +
				"SET dataset.status = 'canBeUpdated' " + 
				"WHERE dataset.ID IN (SELECT provenance.destinationDatasetID FROM provenance WHERE provenance.sourceDatasetID IN (%s));",
				String.join(", ", oldDatasetsID)
			);

			querier.executeSQLUpdate("dataset", SQLSetNeedUpdateDS);
		}
	}

	// Get the Galaxy URL from the config. Used to make API REST requests.
	public String getGalaxyURL() throws Exception {
		String getConfigResult = CommandSingleton.executeCommand(String.format(
				"GetConfig"
			), false // Check roles
		);

		Pattern regex = Pattern.compile("<field name=\"galaxy_url\"><!\\[CDATA\\[([\\w.:\\/]+)]]>");
		Matcher matcher = regex.matcher(getConfigResult);
		if(matcher.find()) {
			return matcher.group(1);
		}
		throw new NoSuchElementException("Galaxy URL value not found in the AMI config.");
	}

	// Get the external name => shorter name for the database, defined in the AMI interface
	public List<String> getProjectsDatabaseExternalName() {
		List<String> nonFilteredDatabaseNames = SchemaSingleton.getExternalCatalogNames();
		List<String> projectDatabaseNames = nonFilteredDatabaseNames.stream()
			.filter(databaseName -> !databaseName.equals("self"))
			.collect(Collectors.toList());

		return projectDatabaseNames;
	}

	// Get the internal name => complete database name
	public List<String> getProjectsDatabaseInternalName() {
		List<String> nonFilteredDatabaseNames = SchemaSingleton.getInternalCatalogNames();
		List<String> projectDatabaseNames = nonFilteredDatabaseNames.stream()
			.filter(databaseName -> !databaseName.equals("AMI_internal"))
			.collect(Collectors.toList());

		return projectDatabaseNames;
	}

	// Validate the current transaction -> Effectively make the updates
	public void commit() throws Exception {
		querier.getConnection().commit();
	}

}
