(() => {
    "use strict";
    $AMIClass("AutoUpdateWorkflowApp", {
        $extends: ami.SubApp,
        $init: async function() {
            this.$super.$init();
            this.galaxyURL = null;
            this.userGalaxyTokenAPI = null;
            this.workflowID = null;
        },
        onReady: function() {
        },
        onLogin: async function() {
            this.galaxyURL = await this.getGalaxyURL();
            this.userGalaxyTokenAPI = await this.getUserGalaxyAPIToken();

            const userData = new URLSearchParams(document.location.search).get("userdata"); //Get URL params
            const URLParams = JSON.parse(userData);

            let workflowVersion = null;

            if(URLParams.hasOwnProperty("workflowID") && URLParams.hasOwnProperty("workflowVersion")) {
                this.workflowID = URLParams.workflowID;
                workflowVersion = URLParams.workflowVersion;
            }else {
                throw new Error("Missing workflowID and workflowVersion in the URL params.");
            }

            const workflow = await this.getWorkflowInfo(workflowVersion);

            const tagsInputs = await this.getTagsInputs(workflow.version);

            amiWebApp.replaceHTML("#ami_main_content", `
                <div class="px-3 pb-2" id="A2944C0A_9249_E4D2_3679_494C1A3AAAF0">
                    <span class="d-flex align-items-baseline"><h3>Auto Update Configuration <b>${workflow.name}</b></h3>&nbsp;-&nbsp;<span>version ${workflow.version}</span></span>
                    <div id="inputs_tags" class="row d-flex justify-content-center"></div>

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
                            <li id="successMessage" class="d-none align-items-center me-3">
                                <span>Configuration saved</span>
                            </li>
                            <li id="errorMessage" class="d-none align-items-center me-3">
                                <span>An error occured.</span>
                            </li>
                            <li>
                                <button id="saveBtn" class="btn btn-primary">Save</button>
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
                        <div id="settings" class="tab-pane container row" style="display: none;">
                            <p><label for="histories">Default history:&nbsp;</label><select id="histories"></select></p>
                            <p><label for="autoUpdateActivation">Auto update: </label>&nbsp;<input id="autoUpdateActivation" type="checkbox" ${workflow.autoUpdate?"checked":''}></p>
                        </div>
                    </div>
                </div>
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


            const histories = await this.getUserHistories();
            histories.forEach((history) => {
                $("#histories").append(`<option value="${history.id}" ${history.id == workflow.history?'selected':''}>${history.name}</option>`)
            });

            // Save button callback
            $("#saveBtn").click(async (event) => {
                const workflowVersionID = await this.getWorkflowVersionID(workflow.version); // Get (and create if needed) the workflow version ID in the AMI DB
                const saveResult = await this.save(workflowVersionID);
                if(saveResult) {
                    $("#successMessage").toggleClass("d-none d-flex");
                }else {
                    $("#errorMessage").toggleClass("d-none d-flex");
                    throw new Error(saveResult);
                }
            });

            // Build the form
            const allTags = await this.getTags();
            Object.entries(workflow.rootInputs).forEach(([rootInputType, rootInputsOfType]) => {
                Object.entries(rootInputsOfType).forEach(async ([stepID, rootInput]) => {
                    if(rootInputType == "parameter_input") {
                        let tagSaved = tagsInputs.hasOwnProperty(stepID) ? tagsInputs[stepID] : '';

                        $("#rootInputsParameters .inputs").append(`
                            <span class="mb-3">
                                <label for="${stepID}">${rootInput.name}:</label>
                                <select class="input tag w-100" id="${stepID}"><option value="" selected>None</option></select>
                            </span>
                        `);
                        allTags.forEach((tag) => {
                            $(`.tag#${stepID}`).append(`<option value="${tag}">${tag}</option>`);
                            const isSelected = tagSaved == tag;
                            if(isSelected) {
                                $(".tag option:first-of-type").removeAttr("selected");
                                $(`.tag#${stepID} option:last-of-type`).attr("selected", "selected");
                            }
                        });
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

        /* Get workflow info : such as name & inputs */
        getWorkflowInfo: async function(workflowVersion) {
            const getWorkflowVersionURL = `${this.galaxyURL}/api/workflows/${this.workflowID}?version=${workflowVersion}&legacy=true`; // Legacy to be able to get the step_ID

            let rootInputs = {"parameter_input":{}, "data_input":{}, "data_collection_input":{}};

            try {
                const response = await fetch(getWorkflowVersionURL, {headers: {"x-api-key": this.userGalaxyTokenAPI}});
                if(!response.ok) {
                    throw new Error(`An error occured while retrieving the parameters for this workflow version. ${response.status} error.`);
                }

                const workflowResponse = await response.json();
                const workflowName = workflowResponse.name;

                const inputs = workflowResponse.inputs;
                const steps = workflowResponse.steps;

                Object.entries(inputs).forEach(([stepID, input]) => {
                    const step = steps[stepID];
                    const type = step.type; // Type of the input (parameter/dataset/datasets collection)
                    const rootInput = {"name": input.label, "optional": step.tool_inputs.optional};
                    if(type == "parameter_input") {
                        rootInput["defaultValue"] = input.value;
                        rootInput["type"] = step.tool_inputs.parameter_type; // Type of the parameter (text/integer...)
                    }
                    rootInputs[type][step.id] = rootInput;
                });

                // Get specific details about the version saved in the AMI DB (default history and is auto update activated)
                const workflowVersionInfo = await this.getWorkflowVersionInfo(workflowVersion);
                
                return {
                    "ID": this.workflowID,
                    "version": workflowVersion,
                    "autoUpdate": workflowVersionInfo.autoUpdate,
                    "history": workflowVersionInfo.historyID,
                    "name": workflowName,
                    "rootInputs": rootInputs
                }
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

        /* Get (and create it if needed) the workflow version ID in the AMI DB */
        getWorkflowVersionID: async function(workflowVersion) {
            return new Promise((resolve, reject) => { 
                const getWFVersIDCmd = `SearchQuery -catalog="ds_db" -entity="workflowVersion" -sql="` +
                `SELECT ID ` +
                `FROM workflowVersion ` +
                `WHERE workflowID='${this.workflowID}' AND version='${workflowVersion}';"`;

                amiCommand.execute(getWFVersIDCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length == 1) {
                        const row = rows[0];
                        resolve(amiWebApp.jspath('..field{.@name==="ID"}.$', row)[0])
                    }else{
                        if(rows.length > 1) {
                            reject("Several workflows exist with the same ID and version ?!");
                        }else {
                            const insertWFVersCmd = `UpdateQuery -catalog="ds_db" -entity="workflowVersion" -sql="` +
                            `INSERT INTO workflowVersion ` +
                            `(workflowID, version) ` +
                            `VALUES ('${this.workflowID}', '${workflowVersion}')"`;

                            amiCommand.execute(insertWFVersCmd).done(async (queryResult) => {
                                const rows = amiWebApp.jspath("..row", queryResult);
                                if(rows.length == 1) {
                                    resolve(await this.getWorkflowVersionID(workflowVersion)); // Can't directly get the new ID...
                                }else {
                                    reject("An error occured.");
                                }
                            });
                        }
                    }
                });
            });
        },

        /* Get the tags associated with this workflow */
        getTags: async function() {
            let tags = [];
            return new Promise((resolve, reject) => { 
                const getWFVersIDCmd = `SearchQuery -catalog="ds_db" -entity="workflowVersion_tag" -sql="` +
                `SELECT tag.name AS tagName ` +
                `FROM dataset_file ` +
                `INNER JOIN tag ON tag.ID = dataset_file.tagID ` +
                `INNER JOIN dataset ON dataset.ID = dataset_file.datasetID ` +
                `INNER JOIN workflowInvocation ON workflowInvocation.galaxyInvocationID = dataset.workflowInvocationID ` +
                `INNER JOIN workflowVersion ON workflowVersion.workflowID = '${this.workflowID}';"`;
            
                amiCommand.execute(getWFVersIDCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length > 0) {
                        rows.forEach((row) => {
                            tags.push(amiWebApp.jspath('..field{.@name==="tagName"}.$', row)[0]);
                        });
                    }       
                    resolve(tags);             
                });
            });
        },

        /* Get the tags associated with the inputs for this workflowVersion */
        getTagsInputs: async function(workflowVersion) {
            return new Promise((resolve, reject) => { 
                let tagsInputs = {};

                const getWFVersIDCmd = `SearchQuery -catalog="ds_db" -entity="workflowVersion_tag" -sql="` +
                `SELECT workflowVersion_tag.stepID AS stepID, tag.name AS tagName ` +
                `FROM workflowVersion_tag ` +
                `INNER JOIN tag ON tag.ID = workflowVersion_tag.tagID ` +
                `INNER JOIN workflowVersion ON workflowVersion.ID = workflowVersion_tag.workflowVersionID ` +
                `INNER JOIN workflow ON workflow.galaxyWorkflowID = workflowVersion.workflowID ` +
                `WHERE workflow.galaxyWorkflowID = '${this.workflowID}' AND workflowVersion.version = '${workflowVersion}';"`;
            
                amiCommand.execute(getWFVersIDCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length > 0) {
                        rows.forEach((row) => {
                            const stepID = amiWebApp.jspath('..field{.@name==="stepID"}.$', row)[0];
                            const tagName = amiWebApp.jspath('..field{.@name==="tagName"}.$', row)[0];
                            tagsInputs[stepID] = tagName;
                        });
                    }
                    resolve(tagsInputs)
                });
            });
        },

        /* Save form data */
        save: async function(workflowVersionID) {
            let newRelations = [];

            const inputs = $(".input").toArray();
            let promises = inputs.map(async input => {
                const inputValue = $(input).val();
                if(inputValue !== "") {
                    await this.getTagIDfromName(inputValue).then((tagID) => {
                        newRelations.push({"stepID": input.id, "tag": tagID});
                    }).catch(function(error) {
                        $("#errorMessage span").text(error);
                        $("#errorMessage").toggleClass("d-none d-flex");                                               
                        throw new Error(error); // Doesn't display the error message in a modal...
                    });
                }

            });
            await Promise.all(promises);

            return new Promise(async (resolve, reject) => { 
                // We delete all previous rows for this wf version... (Easiest way to avoid duplicates and non wanted values)
                const deleteTagsInputsRelationsCmd = `UpdateQuery -catalog="ds_db" -entity="workflowVersion_tag" -sql="` +
                `DELETE FROM workflowVersion_tag ` +
                `WHERE workflowVersionID=${workflowVersionID};"`;

                // Insert the relations...
                amiCommand.execute(deleteTagsInputsRelationsCmd).done((deleteResult) => {
                    let insertTagsInputsRelationsCmd = `UpdateQuery -catalog="ds_db" -entity="workflowVersion_tag" -sql="` +
                    `INSERT INTO workflowVersion_tag ` +
                    `(workflowVersionID, stepID, tagID) ` +
                    `VALUES`;

                    newRelations.forEach((relation, i) => {
                        if(i<newRelations.length-1) {
                            insertTagsInputsRelationsCmd += `(${workflowVersionID}, ${relation.stepID}, ${relation.tag})`;
                        }else {
                            insertTagsInputsRelationsCmd += `(${workflowVersionID}, ${relation.stepID}, ${relation.tag});"`;
                        }
                    });

                    // Activate the auto update and set the default history
                    amiCommand.execute(insertTagsInputsRelationsCmd).done((insertResult) => {
                        const rows = amiWebApp.jspath("..row", insertResult);
                        if(rows.length == 1) {
                            const isAutoUpdateActivated = $("#autoUpdateActivation").is(":checked");
                            const historyID = $("#histories").val();
                            const activateAutoUpdateCmd = `UpdateQuery -catalog="ds_db" -entity="workflowVersion" -sql="` +
                            `UPDATE workflowVersion ` +
                            `SET autoUpdate = '${isAutoUpdateActivated?1:0}', historyID = '${historyID}' ` +
                            `WHERE ID = '${workflowVersionID}';"`;

                            amiCommand.execute(activateAutoUpdateCmd).done((insertResult) => {
                                const rows = amiWebApp.jspath("..row", insertResult);
                                if(rows.length != 1) { // Has not been updated
                                    reject("Error !");
                                }
                            });
                            resolve(true); // Everything's fine
                        }else{
                            reject("Error !");
                        }
                    });
                });
            });
        },

        /* From a tag name, get its ID */
        getTagIDfromName: async function(tagName) {
            return new Promise((resolve, reject) => { 
                const getWorkflowIDCmd = `SearchQuery -catalog="ds_db" -entity="tag" -sql="` +
                `SELECT ID ` +
                `FROM tag ` +
                `WHERE name='${tagName}';"`;

                amiCommand.execute(getWorkflowIDCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length == 1) {
                        const row = rows[0];
                        resolve(amiWebApp.jspath('..field{.@name==="ID"}.$', row)[0]);
                    }else{
                        const errorMsg = `No '${tagName}' tag was found in the DB.`;
                        reject(errorMsg);
                    }
                });
            });
        },

        /* Auto update activated ? + history ID */
        getWorkflowVersionInfo: async function(workflowVersion) {
             return new Promise((resolve, _) => { 
                const getIsAutoUpdateActivatedCmd = `SearchQuery -catalog="ds_db" -entity="workflowVersion" -sql="` +
                `SELECT autoUpdate, historyID ` +
                `FROM workflowVersion ` +
                `WHERE workflowID='${this.workflowID}' AND version='${workflowVersion}';"`;

                amiCommand.execute(getIsAutoUpdateActivatedCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length == 1) {
                        const row = rows[0];
                        resolve({
                            "autoUpdate": amiWebApp.jspath('..field{.@name==="autoUpdate"}.$', row)[0] == 1,
                            "historyID": amiWebApp.jspath('..field{.@name==="historyID"}.$', row)[0]
                        });
                    }else {
                        resolve(false); // If there is no entry, no auto update activated
                    }
                });
            });
        }

    }), window.autoUpdateWorkflowApp = new AutoUpdateWorkflowApp;                                                                                                                                                             
})();