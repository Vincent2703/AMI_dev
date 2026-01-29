(() => {
    "use strict";
    $AMIClass("WorkflowsListApp", {
        $extends: ami.SubApp,
        $init: async function() {
            this.$super.$init();
            this.galaxyURL = null;
            this.userGalaxyTokenAPI = null;
        },
        onReady: function() {
        },
        onLogin: async function() {
            this.galaxyURL = await this.getGalaxyURL();
            this.userGalaxyTokenAPI = await this.getUserGalaxyAPIToken();
            
            amiWebApp.replaceHTML("#ami_main_content", `
                <table id="workflows" class="table">
                    <thead class="thead-dark">
                        <th>Name</th>
                        <th>Version</th>
                        <th>Auto update</th>
                        <th>Configure</th>
                    </thead>
                    <tbody>
                    </tbody>
                </table>
            `);

            const workflows = await this.getWorkflows(); // Get all workflows inserted in the AMI DB
            
            const workflowsVersionsAutoUpdate = await this.getWorkflowsVersionsWithAutoUpdate(); // From the AMI DB

            workflows.forEach(async (workflow) => {
                $("#workflows tbody").append(`
                    <tr id="${workflow.ID}">
                        <td>${workflow.name} <p class="showPrevVersions" style="font-size:0.8em; cursor:pointer; color:dodgerblue;">Show previous versions</p></td>
                        <td class="versions"></td>
                        <td class="autoUpdate"></td>
                        <td class="configure"></td>
                    </tr>
                `);

                $(`#${workflow.ID} .showPrevVersions`).click((event) => {
                    $(event.target).hide(); // Hide the link
                    $(`#${workflow.ID} td p.d-none`).toggleClass("d-none d-block"); // Display previous versions
                });

                const workflowVersions = await this.getWorfklowVersions(workflow.ID); // Every worfklow version (from galaxy endpoint)
                workflowVersions.forEach((version) => {
                    // Versions
                    $(`#${workflow.ID} .versions`).append(`<p class="d-none mb-1">${version}</p>`);
                    // Auto update
                    let checked = false;
                    if(workflowsVersionsAutoUpdate.hasOwnProperty(workflow.ID) && workflowsVersionsAutoUpdate[workflow.ID].hasOwnProperty(version)) {
                        checked = workflowsVersionsAutoUpdate[workflow.ID][version];
                    }
                    $(`#${workflow.ID} .autoUpdate`).append(`<p class="d-none mb-1"><input class="form-check-input" type="checkbox" disabled ${checked?"checked":''}></p>`);
                    // Configure
                    $(`#${workflow.ID} .configure`).append(`<p class="d-none mb-1"><a href='?subapp=AutoUpdateWorkflow&userdata={"workflowID":"${workflow.ID}","workflowVersion":"${version}"}'>Edit</a></p>`);
                })

                $(`#${workflow.ID} td p.d-none:last-child`).toggleClass("d-none d-block");
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

        /* Get all workflows ID. TODO later : filter by group id */
        getWorkflows: async function() {
            let workflows = [];
            return new Promise((resolve, reject) => { 
                const getWorkflowIDCmd = `SearchQuery -catalog="ds_db" -entity="workflow" -raw="` +
                `SELECT galaxyWorkflowID AS ID, name ` +
                `FROM workflow ` +
                `ORDER BY workflow.name;"`;
                amiCommand.execute(getWorkflowIDCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if(rows.length > 0) {
                        rows.forEach((row) => {
                            workflows.push({
                                    "ID": amiWebApp.jspath('..field{.@name==="ID"}.$', row)[0],
                                    "name": amiWebApp.jspath('..field{.@name==="name"}.$', row)[0],
                                });
                        });
                        resolve(workflows);
                    }else {
                        reject("No workflow found");
                    }
                });
            });
        },

        /* Get workflow versions */
        getWorfklowVersions: async function(workflowID) {
            const getWorkflowVersionsURL = `${this.galaxyURL}/api/workflows/${workflowID}/versions`;

            try {
                const response = await fetch(getWorkflowVersionsURL, {headers: {"x-api-key": this.userGalaxyTokenAPI}});
                if(!response.ok) {
                    throw new Error(`An error occured while retrieving the workflow versions. ${response.status} error.`);
                }

                const workflowVersions = await response.json();
                let versions = workflowVersions.map(workflowVersion => workflowVersion.version);
                return versions;
            }catch(error) {
                throw error;
            }
        },

        /* Get all workflow versions with auto update */
        getWorkflowsVersionsWithAutoUpdate: async function() {
            let workflows = {};
            return new Promise((resolve, reject) => { 
                const getWorkflowIDCmd = `SearchQuery -catalog="ds_db" -entity="workflow" -sql="` +
                `SELECT workflow.galaxyWorkflowID AS ID, workflowVersion.version AS version, workflowVersion.autoUpdate AS autoUpdate ` +
                `FROM workflowVersion ` +
                `INNER JOIN workflow ON workflow.galaxyWorkflowID = workflowVersion.workflowID ` +
                `WHERE autoUpdate = 1;"`;

                amiCommand.execute(getWorkflowIDCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    rows.forEach((row) => {
                        const ID = amiWebApp.jspath('..field{.@name==="ID"}.$', row)[0];
                        const version = amiWebApp.jspath('..field{.@name==="version"}.$', row)[0];
                        const autoUpdate = amiWebApp.jspath('..field{.@name==="autoUpdate"}.$', row)[0] == "1";

                        workflows[ID] ??= {}; // We can use this syntax instead of hasOwnProperty()
                        workflows[ID][version] ??= autoUpdate;
                    });

                    resolve(workflows);

                });
            });
        }
    }), window.WorkflowsListApp = new WorkflowsListApp;
})();