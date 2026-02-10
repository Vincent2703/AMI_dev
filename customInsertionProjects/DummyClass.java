package net.hep.ami.customInsertionProjects;

import java.util.Arrays;

import org.json.JSONObject;

import net.hep.ami.QuerySingleton;
import net.hep.ami.Utils;

import org.jetbrains.annotations.*;

public class DummyClass {
    //Singleton helper
    QuerySingleton querySingleton = QuerySingleton.INSTANCE;
    
    //private final JSONObject dataset;

    private String JSONSchemaPath = "/SpaceRemoteSensing.json";

    //Generic...? -> Abstract class ?
    public String name;
    private String description;
    public String version;
    private String startTime; //?
    private String stopTime; //?
    private String invocationID;

    //Specialized metadata
    private String levelID;
    private String sourceID;

    private JSONObject observation;
    private String objectiveID;
    private String instrumentID;
    private String phaseID;
    
    private JSONObject observationZone;
    private String targetID;

    public String datasetID;

    public DummyClass(JSONObject dataset, String _invocationID) throws Exception {
        if(!Utils.validateJSON(dataset, JSONSchemaPath)) {
            throw new IllegalArgumentException("Your JSON is not valid. Please check its structure.");
        }

        invocationID = _invocationID;

        name = dataset.getString("name");
        description = dataset.has("description")?dataset.getString("description"):null;
        version = dataset.getString("version");
        startTime = dataset.has("startTime")?dataset.getString("startTime"):null; //Is it generic ?
        stopTime = dataset.has("stopTime")?dataset.getString("stopTime"):null; //?

        if(dataset.has("level")) {
            String levelName = dataset.getString("level");
            levelID = _getMetadataIDByName("level", levelName); //Must have the metadata in the DB. Throws an exception if it's not the case
        }

        if(dataset.has("source")) {
            String sourceName = dataset.getString("source");
            sourceID = _getMetadataIDByName("source", sourceName);
        }

        if(dataset.has("observation")) {
            observation = dataset.getJSONObject("observation");

            if(observation.has("objective")) {
                String objectiveName = observation.getString("objective");
                objectiveID = _getMetadataIDByName("objective", objectiveName);
            }

            if(observation.has("instrument")) {
                String instrumentName = observation.getString("instrument");
                instrumentID = _getMetadataIDByName("instrument", instrumentName);
            }

            if(observation.has("phase")) {
                String phaseName = observation.getString("phase");
                phaseID = _getMetadataIDByName("phase", phaseName);
            }

            if(observation.has("observationZone")) {
                observationZone = observation.getJSONObject("observationZone");
            }

            if(observationZone.has("target")) {
                String targetName = observationZone.getString("target");
                targetID = _getMetadataIDByName("target", targetName);
                System.out.println("targetID");
                System.out.println(targetID);
            }
        }

    }

    /** 
     * Insert the metadata defined in the constructor, in the database
     * @param null
     * @throws ExceptionString observationZoneID = null;if(observationZone != null)
     */
    @NotNull
    public void insertion() throws Exception { //Returns true/false. Need the ID of the inserted rows later ?
        /* First of all, we need to insert observationZone and observation, if needed */

        //ObservationZone
        String observationZoneID = null;
        if(observationZone != null) {
            observationZoneID = _getMetadataIDByName("observationZone", observationZone.getString("name"), false);

            if(observationZoneID == null) { 
                observationZoneID = querySingleton.insert(
                    "observationZone",
                    Arrays.asList("name", "description", "coordSys", "minLon", "maxLon", "minLat", "maxLat", "minX", "maxX", "minY", "maxY", "minZ", "maxZ", "targetID"),
                    Arrays.asList(
                        observationZone.getString("name"),
                        observationZone.has("description")?observationZone.getString("description"):null,
                        observationZone.getString("coordSys"),
                        observationZone.getString("minLon"),
                        observationZone.getString("maxLon"),
                        observationZone.getString("minLat"),
                        observationZone.getString("maxLat"),
                        observationZone.getString("minX"),
                        observationZone.getString("maxX"),
                        observationZone.getString("minY"),
                        observationZone.getString("maxY"),
                        observationZone.getString("minZ"),
                        observationZone.getString("maxZ"),
                        targetID
                    )
                );
            }
        }

        //Observation
        String observationID = null;
        if(observation != null) {
            observationID = _getMetadataIDByName("observation", observation.getString("name"), false);

            if(observationID == null) {
                observationID = querySingleton.insert(
                    "observation",
                    Arrays.asList("name", "description", "startTime", "stopTime", "minFreq", "maxFreq", "objectiveID", "observationZoneID", "instrumentID", "phaseID"),
                    Arrays.asList(
                        observation.getString("name"),
                        observation.has("description")?observation.getString("description"):null,
                        observation.has("startTime")?observation.getString("startTime"):null,
                        observation.has("stopTime")?observation.getString("stopTime"):null,
                        observation.getString("minFreq"),
                        observation.getString("maxFreq"),
                        objectiveID,
                        observationZoneID,
                        instrumentID,
                        phaseID
                    )
                );
            }
        }

        /* Then the dataset */
        datasetID = querySingleton.insert(
            "dataset",
            Arrays.asList("name", "description", "version", "startTime", "stopTime", "workflowInvocationID", "levelID", "sourceID", "observationID", "workflowTypeID"),
            Arrays.asList(
                name,
                description,
                version,
                startTime,
                stopTime,
                invocationID,
                levelID,
                sourceID,
                observationID,
                null //Temp
            ),
            true
        );

    }
    

    /** 
     * Returns the metadata ID from its name
     * @param metadataName
     * @param metadataValue
     * @param throwException
     * @return String
     * @throws Exception
     */
    private String _getMetadataIDByName(String metadataName, String metadataValue, boolean throwException) throws Exception {
        String metadataID = querySingleton.getColValue(metadataName, "ID", "name", metadataValue);
        if(metadataID != null) {
            return metadataID;
        }else{
            if(throwException) {
                throw new NullPointerException(String.format("There is no '%s.name' metadata with the value '%s'.", metadataName, metadataValue));
            }else {
                return null;
            }
        }
    }

    /** 
     * @param metadataName
     * @param metadataValue
     * @return String
     * @throws Exception
     */
    private String _getMetadataIDByName(String metadataName, String metadataValue) throws Exception {
        return _getMetadataIDByName(metadataName, metadataValue, true);
    }

}
