package net.hep.ami.command.misc;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.time.Duration;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpHeaders;

import org.json.*;
import org.jetbrains.annotations.*;

import net.hep.ami.*;
import net.hep.ami.role.*;
import net.hep.ami.command.*;
import net.hep.ami.utility.*;
import net.hep.ami.data.Row;
import net.hep.ami.data.RowSet;
import net.hep.ami.jdbc.Querier;


import net.hep.ami.QuerySingleton;

import net.hep.ami.CommandSingleton;

@CommandMetadata(role = "AMI_GUEST", visible = true)
public class AutoUpdateDataset extends AbstractCommand {

    public AutoUpdateDataset(@NotNull Set<String> userRoles, @NotNull Map<String, String> arguments, long transactionId) {
		super(userRoles, arguments, transactionId);
    }

    @NotNull
	@Override
	public StringBuilder main(@NotNull Map<String, String> arguments) throws Exception {
        QuerySingleton querySingleton = QuerySingleton.INSTANCE;
		querySingleton.setQuerier(getQuerier("ds_db"));

        Querier querier = getQuerier("ds_db");

        String galaxyURL = querySingleton.getGalaxyURL();

        String datasetID = arguments.get("datasetID");
        String userAPIKey = arguments.get("userAPIKey");

        if(Empty.is(datasetID, Empty.STRING_NULL_EMPTY_BLANK) || Empty.is(userAPIKey, Empty.STRING_NULL_EMPTY_BLANK) || userAPIKey == null) {
            throw new IllegalArgumentException("Missing argument(s): datasetID & userAPIKey.");
        }

        /* Check dataset has needUpdate status */
        boolean needUpdate = querySingleton.getColValue("dataset", "status", "status", "canBeUpdated") != null;

        /* Get dataset's workflow ID and version*/
        RowSet queryDataset = querier.executeSQLQuery("dataset", String.format(
            "SELECT workflow.galaxyWorkflowID AS workflowID, workflowVersion.version AS version, workflowVersion.ID AS workflowVersionID \n" +
            "FROM dataset \n" +
            "INNER JOIN workflowInvocation ON workflowInvocation.galaxyInvocationID = dataset.workflowInvocationID \n" +
            "INNER JOIN workflowVersion ON workflowVersion.ID = workflowInvocation.workflowVersionID \n" +
            "INNER JOIN workflow ON workflow.galaxyWorkflowID = workflowVersion.workflowID \n" +
            "WHERE dataset.ID = '%s' LIMIT 1;", datasetID));
        
        List<Row> rowsWorklow = queryDataset.getAll();
        if(rowsWorklow.size() != 1) {
            return new StringBuilder("<error><![CDATA[No information about the workflow found for this dataset]]</error>");
        }
        Row rowWorkflow = rowsWorklow.get(0);
		String workflowID = rowWorkflow.getValue("workflowID");
        String workflowVersion = rowWorkflow.getValue("version");
        String workflowVersionID = rowWorkflow.getValue("workflowVersionID");

        /* Check auto update activation */
        Row rowWorkflowVersion = querySingleton.getColsValue("workflowVersion", Arrays.asList("ID"), Arrays.asList(workflowVersionID), Arrays.asList("autoUpdate", "historyID")).get(0);
        boolean hasAutoUpdate = rowWorkflowVersion.getValue("autoUpdate", 0) == 1;
        String historyID = rowWorkflowVersion.getValue("historyID");

        if(needUpdate && hasAutoUpdate) {
            /* Get stepID used by the workflow to specify files */
            HashMap<String, String> stepsValues = new HashMap<String, String>();
            List<Row> rowsTags = querySingleton.getColsValue("workflowVersion_tag", Arrays.asList("workflowVersionID"), Arrays.asList(workflowVersionID), Arrays.asList("stepID"));
            for(Row rowTag : rowsTags) {
                String stepID = rowTag.getValue("stepID");

                /* Get latest DS file associated with this step ID */
                RowSet queryFileLatestDS = querier.executeSQLQuery("workflowVersion_tag", String.format(
                    "SELECT physicalFile.path AS path \n" +
                    "FROM workflowVersion_tag \n" +
                    "INNER JOIN tag ON tag.ID = workflowVersion_tag.tagID \n" +
                    "INNER JOIN dataset_file ON dataset_file.tagID = workflowVersion_tag.tagID \n" +
                    "INNER JOIN dataset ds1 ON ds1.ID = dataset_file.datasetID \n" +
                    "INNER JOIN physicalFile ON physicalFile.fileID = dataset_file.fileID \n" +
                    "WHERE workflowVersion_tag.stepID = '%s' AND ds1.version = ( \n" +
                    "SELECT MAX(ds2.version) \n" +
                    "FROM dataset ds2 \n" +
                    "WHERE ds2.name = ds1.name ) LIMIT 1;", stepID)
                );
                Row rowFile = queryFileLatestDS.getAll().get(0);
                String filePath = rowFile.getValue("path");
                stepsValues.put(stepID, filePath);
            }

            StringBuilder inputsStringBuilder = new StringBuilder("{");
            for(String stepID : stepsValues.keySet()) {
                inputsStringBuilder.append(String.format("\"%1$s\":\"%2$s\",", stepID, stepsValues.get(stepID)));
            }
            inputsStringBuilder.delete(inputsStringBuilder.length()-1, inputsStringBuilder.length()).append("}");
            String inputsString = inputsStringBuilder.toString();

            /* Workflow invocation */
            JSONObject invocationPayload = new JSONObject(String.format(
                "{\"workflow_id\"   : \"%1$s\"," +   
                "\"history\"        : \"hist_id=%2$s\"," + 
                "\"version\"        : \"%3$s\"," +
                "\"inputs_by\"      : \"step_id\"," +
                "\"inputs\"         : %4$s," + 
                "\"use_cached_job\" : \"false\"}",
                workflowID,
                historyID,
                workflowVersion,
                inputsString
            ));

            String invocationURL = String.format(galaxyURL+"/api/workflows/%s/invocations", workflowID); //GET URL from config

            // Send the request to Galaxy
            try {
                HttpClient client = HttpClient.newHttpClient();

                HttpRequest request = HttpRequest.newBuilder()
                    .header("x-api-key", userAPIKey)
                    .header("Content-Type", "application/json")
                    .version(HttpClient.Version.HTTP_1_1) // Weird timeout error without it...
                    .uri(URI.create(invocationURL))
                    .POST(HttpRequest.BodyPublishers.ofString(invocationPayload.toString()))
                    .timeout(Duration.ofSeconds(10))
                    .build();


                    HttpResponse<String> response;
                    try {
                        System.out.println("Sending invocation request now: " + invocationURL);
                        System.out.println(invocationPayload.toString());
                        response = client.send(request, HttpResponse.BodyHandlers.ofString());

                    }catch (IOException | InterruptedException error) {
                        Thread.currentThread().interrupt();
                        return new StringBuilder(String.format(
                            "<error>Request failed: %s</error>", // CDATA doesn't work here... I don't know why
                            error.getMessage()
                        ));
                    }

                    JSONObject json = new JSONObject(response.body());

                    if(!json.has("workflow_id")) {
                        return new StringBuilder(
                            "<error>Request failed: Can't get the new workflow instance ID.</error>"
                        );
                    }

                    String invocationID = json.getString("workflow_id");

                    return new StringBuilder(String.format(
                        "<info>Workflow has been restarted. %1$s/workflows/invocations/%2$s</info>",
                        galaxyURL,
                        invocationID
                    ));

            }catch(Exception error) {
                return new StringBuilder(String.format("<error>Request failed: %s</error>", error.getMessage()));
            }

        }else {
            return new StringBuilder("<info>Nothing to update</info>");
        }
    }

    @NotNull
	@Contract(pure = true)
	public static String help()
	{
		return "Auto update the dataset.";
	}

	/*----------------------------------------------------------------------------------------------------------------*/

	@NotNull
	@Contract(pure = true)
	public static String usage()
	{
		return "-datasetID=\"\" -userAPIKey=\"\"";
	}
}
