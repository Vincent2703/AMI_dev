getSelectedProject = async function(searchInDB=false) {
    // Try to get the selected database from a cookie (that expires at the end of the session)
    let project = null;
    const databaseCookieMatch = document.cookie.match(new RegExp('(^| )database=([^;]+)'));

    if(databaseCookieMatch) {
        project = databaseCookieMatch[2].replace("AMI_Project_", "");
    } 

    if(project == null && searchInDB) {
        // If not found, find it in the DB
        // Get the user name
        const getUserInfoCmd = "GetUserInfo";
        let username = null;
        await amiCommand.execute(getUserInfoCmd).done((queryResult) => {
            const row = amiWebApp.jspath("..row", queryResult)[0];
            username = amiWebApp.jspath('..field{.@name==="AMIUser"}.$', row)[0];
        });

        // Get value from user json
        const getUserJSONCmd = `BrowseQuery -catalog="self" -entity="router_user" -sql="SELECT json FROM router_user WHERE AMIUser = '${username}' LIMIT 1"`;
        await amiCommand.execute(getUserJSONCmd).done((queryResult) => {
            const row = amiWebApp.jspath("..row", queryResult)[0];
            project = JSON.parse(amiWebApp.jspath('..field{.@name==="json"}.$', row)[0]).selectedDatabase; // null if not found
        });

        if(project != null) { //We fetched the value in the DB but there is no cookie
            this.setSelectedProject(project, false); //Save in a cookie
        }
    }

    return project;
}

setSelectedProject = async function(selectedDatabase, saveInDB=true) {
    document.cookie = `database=${selectedDatabase};expires=0;path=/;`; // Will expire at the end of the session
    
    if(saveInDB) {
        // Get the user name
        const getUserInfoCmd = "GetUserInfo";
        let username = null;
        await amiCommand.execute(getUserInfoCmd).done((queryResult) => {
            const row = amiWebApp.jspath("..row", queryResult)[0];
            username = amiWebApp.jspath('..field{.@name==="AMIUser"}.$', row)[0];
        });

        // Get value from user json
        let userJSON = null;
        let newUserJSON = null;
        const getUserJSONCmd = `BrowseQuery -catalog="self" -entity="router_user" -sql="SELECT json FROM router_user WHERE AMIUser = '${username}' LIMIT 1;"`;
        await amiCommand.execute(getUserJSONCmd).done((queryResult) => {
            const row = amiWebApp.jspath("..row", queryResult)[0];
            userJSON = JSON.parse(amiWebApp.jspath('..field{.@name==="json"}.$', row)[0]);
            userJSON.selectedDatabase = selectedDatabase;
            newUserJSON = JSON.stringify(userJSON).replace(/"/g, '\\"');
        });

        const setUserJSONCmd = `UpdateQuery -catalog="self" -entity="router_user" -sql="UPDATE router_user SET json='${newUserJSON}' WHERE AMIUser='${username}';"`;
        await amiCommand.execute(setUserJSONCmd).done((queryResult) => {
            //TODO
            return;
        });
    }
}

/* From the config, get the Galaxy URL */
getGalaxyURL = function() {
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
getUserGalaxyAPIToken = function() {
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
}