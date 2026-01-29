package net.hep.ami;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import net.hep.ami.data.Row;
import net.hep.ami.data.RowSet;
import net.hep.ami.jdbc.Querier;

import org.json.*;

public enum QuerySingleton {
	INSTANCE;
	
	Querier querier;

	public void setQuerier(Querier querier) {
		this.querier = querier;
	}

	public QuerySingleton getInstance() {
        return INSTANCE;
    }

	//insert only if not existing
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
					//throw new SQLException("Can't obtain the new row ID");
				}
			}
		
		}
	}


	public String insert(String tableName, List<String> columnsName, List<String> columnsValue) throws Exception {
		return insert(tableName, columnsName, columnsValue, true);
	}


	/*public String update(String tableName, List<String> columnsNames, List<String> columnsValue [...]) throws Exception {

	}*/


	//TODO : add tries & catches


	public String getColValue(String tableName, String selectedCol, String getByCol, String valueCol) throws Exception {
		RowSet query = querier.executeSQLQuery(tableName, String.format("SELECT %2$s FROM `%1$s` WHERE UPPER(%1$s.%3$s) = '%4$S' LIMIT 1", tableName, selectedCol, getByCol, valueCol));
		List<Row> rows = query.getAll();
		return rows.size() > 0 ? rows.get(0).getValue(0) : null;
	}


	public List<Row> getColsValue(String tableName, List<String> whereColsName, List<String> whereColsValue, List<String> selectedColsName) throws Exception {
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

		String sqlSelect = String.format("SELECT %1$s FROM %2$s WHERE %3$s LIMIT 1", selectedCols, tableName, whereClause);

		RowSet rowSet = querier.executeSQLQuery(tableName, sqlSelect);
		List<Row> rows = rowSet.getAll(); //GetFirst ?
		return rows.size() > 0 ? rows : null;
	}

	public List<Row> getColsValue(String tableName, List<String> whereColsName, List<String> whereColsValue) throws Exception {
		return getColsValue(tableName, whereColsName, whereColsValue, null);
	}


	public boolean exists(String tableName, List<String> whereColsName, List<String> whereColsValue) throws Exception {
		List<Row> rows = getColsValue(tableName, whereColsName, whereColsValue);
		return rows != null;
	}

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


}
