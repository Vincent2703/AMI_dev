(() => {
    "use strict";
    $AMIClass("RestartWorkflowApp", {
        $extends: ami.SubApp,
        $init: async function() {
            this.$super.$init();
            this.galaxyURL = null;
            this.userGalaxyTokenAPI = null;
            this.isUpdate = false; // Fetch the last DS version if relevant
            this.workflowID = null;
            this.workflowVersionsRootInputs = {};
        },
        onReady: function() {
        },
        onLogin: async function() {
            // To make the requests work...
            this.galaxyURL = await this.getGalaxyURL();
            this.userGalaxyTokenAPI = await this.getUserGalaxyAPIToken();

            const userData = new URLSearchParams(document.location.search).get("userdata"); //Get URL params
            const URLParams = JSON.parse(userData);

            let selectedWorkflowVersion = null;

            if(URLParams.hasOwnProperty("workflowID") && URLParams.hasOwnProperty("workflowVersion")) {
                this.workflowID = URLParams.workflowID;
                selectedWorkflowVersion = URLParams.workflowVersion;
            }else {
                throw new Error("Missing workflowID and/or workflowVersion in the URL params.");
            }

            this.isUpdate = URLParams.hasOwnProperty("isUpdate") && URLParams.isUpdate == "true";

            const workflow = await this.getWorkflowInfo(selectedWorkflowVersion); // Only get the name for now


            amiWebApp.replaceHTML("#ami_main_content",`
                <div class="px-3 pb-2" id="A2944C0A_9249_E4D2_3679_494C1A3AAAF0">
                    <form id="formWorkflowRestart">
                        <span class="d-flex align-items-center mt-3 mb-3">
                            <h2 class="me-3">Workflow restart - ${workflow.name}</h2>
                            <label for="workflowVersions">Version:&nbsp;</label><select id="workflowVersions" ${this.isUpdate?"disabled":''}></select>
                        </span>
                        <div class="d-flex mt-4 mb-4" id="tabsAndBtn">
                            <ul id="formTabs" class="nav nav-tabs w-75">
                                <li id="rootInputsTab" class="nav-item">
                                    <button class="nav-link active">Workflow inputs</button>
                                </li>
                                <li id="settingsTab" class="nav-item">
                                    <button class="nav-link">Settings</button>
                                </li>
                            </ul>
                            <ul class="nav nav-tabs w-25 justify-content-end">
                                <li style="display: none;">
                                    <button id="galaxyWFInvocationBtn" class="nav-link" data-invocation-url="" onclick="event.preventDefault();window.open($(event.target).attr('data-invocation-url'), '_blank');">Open invocation in Galaxy</button>
                                </li>
                                <li>
                                    <button id="restartWorkflowBtn" class="btn btn-primary">Restart workflow</button>
                                </li>
                            </ul>
                        </div>
                        <div id="formTabsContent" class="tab-content">
                            <div id="rootInputs" class="tab-pane container-fluid row" style="display: flex;">
                                <div id="rootInputsParameters" class="col-4">
                                    <p class="text-center"><b>Parameters</b></p>
                                    <div class="inputs d-flex flex-column"></div>
                                </div>
                                <div id="rootInputsDatasets" class="col-4">
                                    <p class="text-center"><b>Datasets</b></p>
                                    <div class="inputs"></div>
                                </div>
                                <div id="rootInputsDatasetsCollections" class="col-4">
                                    <p class="text-center"><b>Datasets collections</b></p>
                                    <div class="inputs"></div>
                                </div>
                            </div>
                            <div id="settings" class="tab-pane active container row" style="display: none;">
                                <p><label for="histories">History:&nbsp;</label><select id="histories"></select></p>
                                <p><label for="useCachedJob">Use Cached Job:&nbsp;</label><input id="useCachedJob" type="checkbox"></p>
                            </div>
                        </div>
                    </form>
                </div>\n
            `);

            // Callback for the tabs (change the active button + show/hide the corresponding content)
            const tabs = $("#settingsTab button, #rootInputsTab button");
            tabs.click((event) => {
                event.preventDefault();
                const tab = $(event.target);
                let contentTabElement = tab.parent().attr("id") == "settingsTab" ? $("#settings") : $("#rootInputs");
                $("#formTabs button").removeClass("active");
                tab.toggleClass("active");
                $("#formTabsContent>div").hide();
                contentTabElement.css("display", "flex");
            });

            const workflowVersions = await this.getWorkflowVersions();
            let promises = workflowVersions.map(async wfVersion => { // Promises array (we must wait for each await). wfVersion is an object here
                const currentWorkflowVersion = wfVersion.version;
                $("#workflowVersions").append(`<option value="${currentWorkflowVersion}">${currentWorkflowVersion}</option>`);
                this.workflowVersionsRootInputs[currentWorkflowVersion] = await this.getWorkflowRootInputs(currentWorkflowVersion);
            });
            await Promise.all(promises); // Wait until all promises are resolved*/
            $(`#workflowVersions option[value='${selectedWorkflowVersion}']`).attr("selected", "selected");

            // Callback for workflowVersions select
            const workflowVersionsSelect = $("#workflowVersions");
            workflowVersionsSelect.change((event) => {
                selectedWorkflowVersion = event.target.value;
                this.buildForm(selectedWorkflowVersion);
            });


            const userHistories = await this.getUserHistories(); // Galaxy user histories
            const userLatestHistory = await this.getUserLatestUsedHistory();

            userHistories.forEach((history) => { // For each history, we add it to the select
                const isLatestHistory = history.id == userLatestHistory.id;
                $("#histories").append(`<option value="${history.id}" ${isLatestHistory?"selected":''}>${history.name}</option>`);
            });

            // Callback when the user click on the restart button
            $("#restartWorkflowBtn").click((event) => {
                const invocationPromise = this.restartWorkflow(selectedWorkflowVersion);
                invocationPromise.then((invocation) => {
                    if(typeof invocation !== "undefined") {
                        const galaxyInvocationURL = `${this.galaxyURL}/workflows/invocations/${invocation.id}`;
                        $("#galaxyWFInvocationBtn").parent().css("display", "block");
                        $("#galaxyWFInvocationBtn").attr("data-invocation-url", galaxyInvocationURL); // Link to the new invocation in Galaxy
                    } //TODO : error
                });
                event.preventDefault();
            });

            // Display the form
            await this.buildForm(selectedWorkflowVersion);


        },
        onLogout: function() {
            $("#A2944C0A_9249_E4D2_3679_494C1A3AAAF0").html("Please sign-in.");
        },

        /* From the config, get the Galaxy URL */
        getGalaxyURL: function() {
            return new Promise((resolve, reject) => {
                amiCommand.execute("GetConfig").done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length > 0) {
                        const rowGalaxyURL = rows.find(param => param.field[0]["@name"] === "galaxy_url");
                        const galaxyURL = rowGalaxyURL.field[0]["$"];
                        resolve(galaxyURL);
                    }
                    reject("An error occured while retrieving the Galaxy URL from the config.")
               });
            });
        },

        /* Get the current user Galaxy API token */
        getUserGalaxyAPIToken: function() {
            return new Promise((resolve, reject) => {
                amiCommand.execute("GetSessionInfo").done(async (queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length > 0) {
                        const AMIUserName = amiWebApp.jspath('..field{.@name==="AMIUser"}.$', rows[0])[0];
                        if(AMIUserName) {
                            const getUserGalaxyAPITokenCmd = `SearchQuery -catalog="self" -entity="router_user" -sql="` +
                            `SELECT json ` +
                            `FROM router_user ` +
                            `WHERE AMIUser='${AMIUserName}'"`;
                            await amiCommand.execute(getUserGalaxyAPITokenCmd).done((queryResult) => {
                                const rows = amiWebApp.jspath("..row", queryResult);
                                if(rows.length == 1) {
                                    const userJSON = amiWebApp.jspath('..field{.@name==="json"}.$', rows[0])[0];
                                    resolve(JSON.parse(userJSON).galaxyAPIKey);
                                }
                            }); 
                        }
                    }
                    reject("An error occured while retrieving the user Galaxy API token.");
                });
            });
        },

        /* Get all versions for this workflow */
        getWorkflowVersions: async function() {
            const getselectedWorkflowVersionsURL = `${this.galaxyURL}/api/workflows/${this.workflowID}/versions`;

            try {
                const response = await fetch(getselectedWorkflowVersionsURL, {headers: {"x-api-key": this.userGalaxyTokenAPI}});
                if(!response.ok) {
                    throw new Error(`An error occured while retrieving the workflow versions. ${response.status} error.`);
                }
                   
                return response.json();
            }catch(error) {
                throw error;
            }
        },

        // Get workflow name
        getWorkflowInfo: async function(selectedWorkflowVersion) {
            const getWorkflowInfoURL = `${this.galaxyURL}/api/workflows/${this.workflowID}?version=${selectedWorkflowVersion}`; 

            try {
                const response = await fetch(getWorkflowInfoURL, {headers: {"x-api-key": this.userGalaxyTokenAPI}});
                if(!response.ok) {
                    throw new Error(`An error occured while retrieving the workflow info. ${response.status} error.`);
                }

                const workflowResponse = await response.json();
                const workflowName = workflowResponse.name

                return {"name": workflowName};
                
            }catch(error) {
                throw error;
            }
        },

        // Fetch the "root inputs" (the inputs to fill in, in order to start a workflow)
        getWorkflowRootInputs: async function(selectedWorkflowVersion) {
            const getWorkflowVersionURL = `${this.galaxyURL}/api/workflows/${this.workflowID}?version=${selectedWorkflowVersion}&legacy=true`; // Legacy to be able to get the step_ID
            console.log(getWorkflowVersionURL);

            let rootInputs = {"parameter_input":{}, "data_input":{}, "data_collection_input":{}};

            try {
                const response = await fetch(getWorkflowVersionURL, {headers: {"x-api-key": this.userGalaxyTokenAPI}});
                if(!response.ok) {
                    throw new Error(`An error occured while retrieving the parameters for this workflow version. ${response.status} error.`);
                }

                const workflowResponse = await response.json();

                const inputs = workflowResponse.inputs;
                const steps = workflowResponse.steps;

                Object.entries(inputs).forEach(([stepID, input]) => {
                    const step = steps[stepID];
                    const type = step.type; // Type of the input (parameter/dataset/datasets collection)
                    const rootInput = {"name": input.label, "optional": step.tool_inputs.optional};
                    if(type == "parameter_input") {
                        rootInput["defaultValue"] = step.tool_inputs.default ?? '';
                        rootInput["type"] = step.tool_inputs.parameter_type; // Type of the parameter (text/integer...)
                    }
                    rootInputs[type][step.id] = rootInput;
                });
                
                console.log(rootInputs);
                return rootInputs;
                
            }catch(error) {
                throw error;
            }
        },

        /* Get all user histories (workflow outputs are attached to a history in Galaxy) */
        getUserHistories: async function() {
            const getUserHistoriesURL = `${this.galaxyURL}/api/histories`;

            try {
                const response = await fetch(getUserHistoriesURL, {headers: {"x-api-key": this.userGalaxyTokenAPI}});
                
                if(!response.ok) {
                    throw new Error(`An error occured while retrieving the user histories. ${response.status} error.`);
                }
                    
                return response.json();
            }catch(error) {
                throw error;
            }
        },

        /* Get the most recently used history for the current user */
        getUserLatestUsedHistory: async function() {
            const getUserLatestUsedHistory = `${this.galaxyURL}/api/histories/most_recently_used`;

            try {
                const response = await fetch(getUserLatestUsedHistory, {headers: {"x-api-key": this.userGalaxyTokenAPI}});

                if(!response.ok) {
                    throw new Error(`An error occured while retrieving the latest user history. ${response.status} error.`);
                }
                    
                return response.json();
            }catch(error) {
                throw error;
            }
        },

        buildForm: async function(selectedWorkflowVersion) {
            // Clear the inputs
            $("#rootInputsParameters .inputs, #rootInputsDatasets .inputs, #rootInputsDatasetsCollections .inputs").empty();

            rootInputs = this.workflowVersionsRootInputs[selectedWorkflowVersion];

            Object.entries(rootInputs).forEach(([rootInputType, rootInputsOfType]) => {
                Object.entries(rootInputsOfType).forEach(async ([stepID, rootInput]) => {
                    if(rootInputType == "parameter_input") {
                        //TODO
                        let htmlInputType = "text";
                        //
                        let value = null;
                        if(this.isUpdate) {
                            const latestDSFile = await this.getLatestDatasetFile(stepID);
                            value = latestDSFile.path;
                        }else {
                            value = rootInput.defaultValue;
                        }


                        $("#rootInputsParameters .inputs").append(`
                            <span class="mb-3">
                                <label for="${stepID}">${rootInput.name}:</label>
                                <input class="input w-100" id="${stepID}" type="${htmlInputType}" value="${value}">
                            </span>
                        `);
                    }else if(rootInputType == "data_input") {
                        $("#rootInputsDatasets .inputs").append(`
                            <span class="mb-3">
                                <label for="${stepID}">${rootInput.name}:</label>
                                <input class="input w-100" id="${stepID}" type="text">
                            </span>
                        `);
                    }else if(rootInputType == "data_collection_input") {
                        $("#rootInputsDatasetsCollections .inputs").append(`
                            <span class="mb-3">
                                <label for="${stepID}">${rootInput.name}:</label>
                                <input class="input w-100" id="${stepID}" type="text">
                            </span>
                        `);
                    }
                });
            });
        },

        /* From the stepID, get the latest dataset file that can be used for the corresponding input */
        getLatestDatasetFile: async function(stepID) {
            const getLatestDatasetFileCmd = `SearchQuery -catalog="ds_db" -entity="input" -raw="` +
            `SELECT tag.name AS tagName, ds1.name AS datasetName, ds1.version AS datasetVersion, physicalFile.path AS path ` +
            `FROM workflowVersion_tag ` +
            `INNER JOIN tag ON tag.ID = workflowVersion_tag.tagID ` +
            `INNER JOIN dataset_file ON dataset_file.tagID = workflowVersion_tag.tagID ` +
            `INNER JOIN dataset ds1 ON ds1.ID = dataset_file.datasetID ` +
            `INNER JOIN physicalFile ON physicalFile.fileID = dataset_file.fileID ` +
            `WHERE workflowVersion_tag.stepID = ${stepID} AND ds1.version = ( ` +
                `SELECT MAX(ds2.version) ` +
                `FROM dataset ds2 ` +
                `WHERE ds2.name = ds1.name  ` +
            `) LIMIT 1;"`;

            return new Promise(async (resolve, reject) => { 
                await amiCommand.execute(getLatestDatasetFileCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length > 0) {
                        const row = rows[0];
                        resolve({
                            "datasetName": amiWebApp.jspath('..field{.@name==="datasetName"}.$', row)[0],
                            "datasetVersion": amiWebApp.jspath('..field{.@name==="datasetVersion"}.$', row)[0],
                            "tagName": amiWebApp.jspath('..field{.@name==="tagName"}.$', row)[0],
                            "path": amiWebApp.jspath('..field{.@name==="path"}.$', row)[0],
                        });
                    }
                });
                reject("No parent dataset found for this workflow. Maybe you haven't inserted one or you forgot to associate the workflow input(s) to (a) tag(s).");
            });
        },


        /* From a file path, get the corresponding HDAID if it exists */
        getHDAIDFromPath: async function(path) {
            return new Promise(async (resolve, reject) => { 
                const getInputsValuesCmd = `SearchQuery -catalog="ds_db" -entity="file" -raw="` +
                `SELECT file.galaxyDatasetID AS HDAID ` +
                `FROM file ` +
                `INNER JOIN physicalFile ON physicalFile.fileID = file.ID ` +
                `WHERE physicalFile.path = '${path}' LIMIT 1;"`;

                await amiCommand.execute(getInputsValuesCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length > 0) {
                        resolve(amiWebApp.jspath('..field{.@name==="HDAID"}.$', rows[0])[0]);
                    }else{
                        resolve(null);
                    }
                });
                reject("An error occured. 1");
            });
        },

        /* For each tool, get the latest cluster params used */
        getDefaultClusterParams: async function(workflowVersion) {
            let parameters = {};

            const getLatestClusterParamsUsedCmd = `SearchQuery -catalog="ds_db" -entity="workflowInvocation" -raw="` +
            `SELECT tool.toolID AS toolID, clusterParams.parametersDict AS parametersDict ` +
            `FROM workflowInvocation ` +
            `INNER JOIN workflowVersion wfv1 ON wfv1.ID = workflowInvocation.workflowVersionID ` +
            `INNER JOIN toolExecution ON toolExecution.workflowInvocationID = workflowInvocation.galaxyInvocationID ` +
            `INNER JOIN tool ON tool.toolID = toolExecution.toolID ` +
            `INNER JOIN clusterParams ON clusterParams.ID = toolExecution.clusterParamsID ` +
            `WHERE wfv1.workflowID='${this.workflowID}' AND wfv1.version='${workflowVersion}' ` +
            `AND workflowInvocation.executionDatetime = ( ` +
            `SELECT MAX(wfi2.executionDatetime) ` +
                `FROM workflowInvocation wfi2 ` +
                `INNER JOIN workflowVersion wfv2 ON wfv2.ID = workflowInvocation.workflowVersionID ` +
                `WHERE wfi2.workflowVersionID = wfv1.ID AND wfv2.version=wfv1.version ` +
            `);"`;

            return new Promise(async (resolve, reject) => { 
                await amiCommand.execute(getLatestClusterParamsUsedCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length > 0) {
                        rows.forEach((row) => {
                            const toolID = amiWebApp.jspath('..field{.@name==="toolID"}.$', row)[0];
                            const parametersDict = amiWebApp.jspath('..field{.@name==="parametersDict"}.$', row)[0];

                            // To optimize : (set default ?)
                            parameters[toolID] = {};
                            parameters[toolID]["__job_resource"] = parametersDict;
                        });

                        resolve(parameters);
                    }
                    resolve(null);
                });
            });
        },

        /* Restart the workflow using the values the user filled in the form */
        restartWorkflow: async function(workflowVersion) {
            const historyID = $("#histories").val();

            const rootInputs = this.workflowVersionsRootInputs[workflowVersion]; // List of root inputs

            //Payload
            let payloadRootInputs = {};

            let promises = null;

            Object.entries(rootInputs).forEach(([rootInputType, rootInputsOfType]) => {
                promises = Object.keys(rootInputsOfType).map(async (stepID) => {
                    const value  = $(`#formWorkflowRestart #${stepID}`).val();

                    if(rootInputType == "parameter_input") {
                        payloadRootInputs[stepID] = value;
                    }else if(rootInputType == "data_input") { // Is a dataset
                        const HDAID = await this.getHDAIDFromPath(value);
                        if(HDAID != null) { // Found a corresponding HDA ID
                            payloadRootInputs[stepID] = {"id": HDAID, "src": "hda"};
                        }else { // TODO We should check with a regex if it's an url
                            payloadRootInputs[stepID] = {"url": value, "src": "url"};
                        }
                    }else { // Is a collection
                        payloadRootInputs[stepID] = {"id": value, "src": "hdca"};
                    }
                });
            });
                       
            await Promise.all(promises); // Wait until all promises are resolved

            const use_cached_job = $("#useCachedJob").is(":checked");

            const latestClusterParamsUsed = await this.getDefaultClusterParams(workflowVersion);

            let payload = {
                "workflow_id"   : this.workflowID,
                "history"       : `hist_id=${historyID}`,
                "version"       : workflowVersion,
                "inputs_by"     : "step_id", // Inputs are indexed by step ID
                "inputs"        : payloadRootInputs,
                "use_cached_job": use_cached_job
            };

            if(latestClusterParamsUsed != null && await this.getGalaxyURL() != "http://localhost:8081") {
                payload["parameters"] = latestClusterParamsUsed;
            }

            const restartWorkflowURL = `${this.galaxyURL}/api/workflows/${this.workflowID}/invocations`;
            try {
                const response = await fetch(restartWorkflowURL, {
                    method: "POST",
                    headers: {"Content-type": "application/json; charset=UTF-8", "x-api-key": this.userGalaxyTokenAPI},
                    body: JSON.stringify(payload)
                });
                if(!response.ok) {
                    throw new Error(`Error: ${response.statusText}`);
                }else {
                    return response.json();
                }
            }catch(error) {
                console.error(error.message);
            }
                
        }

    }), window.restartWorkflowApp = new RestartWorkflowApp                                                                                                                                                             
})();