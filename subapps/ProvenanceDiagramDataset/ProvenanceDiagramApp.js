(() => {
    "use strict";
    $AMIClass("ProvenanceDiagramApp", {
        $extends: ami.SubApp,
        $init: function() {
            this.$super.$init();
        },
        onReady: function() {

        },
        onLogin: function() { //Only for logged users
            amiWebApp.replaceHTML("#ami_main_content",'<div class="px-3 pb-2 text-center" id="A2944C0A_9249_E4D2_3679_494C1A3AAAF0"></div>\n');
            const userData = new URLSearchParams(document.location.search).get("userdata"); //Get URL params
            const URLParams = JSON.parse(userData); //Should contain a json object with the datasetID
            this.makeDot(URLParams.datasetID).then((dot) => {
                this.displayDot(dot);
            }).catch((error) => {
                console.error("Erreur lors de la génération du dot:", error);
                $("#A2944C0A_9249_E4D2_3679_494C1A3AAAF0").html(`<h2>Erreur : ${error}</h2>`);
            });
        },
        onLogout: function() {
            $("#A2944C0A_9249_E4D2_3679_494C1A3AAAF0").html("Please sign-in.")
        },

        /*
            Main function, construct the provenance diagram
            Take a dataset ID as an argument
            return the dot diagram code
        */
        makeDot: function(datasetID) {
            // Get the datasets related to the selected dataset -> all its parent, children and "cousins" (other datasets used by the children as inputs)
            const getProvenanceDatasetsCmd = `SearchQuery -catalog="ds_db" -entity="provenance" -raw="CALL getDatasetRelations(${datasetID}, 4)"`;
        
            return new Promise((resolve, reject) => {
                let nodes = new Map(); // All nodes
                let nodesOptionsStr = []; // List of strings that contains the options (style and link to DS) for each node
                let nodesRelationsStr = []; // List of strings that contains 2 nodes related between them (n1 -> n2)
        
                amiCommand.execute(getProvenanceDatasetsCmd).done((queryResult) => {
                    const rows = amiWebApp.jspath("..row", queryResult);
                    if (rows.length > 0) {
                        rows.forEach((queryResult) => {
                            const parentDatasetID = amiWebApp.jspath('..field{.@name==="parentDatasetID"}.$', queryResult)[0] || '',
                                parentDatasetName = amiWebApp.jspath('..field{.@name==="parentDatasetName"}.$', queryResult)[0] || '',
                                parentDatasetstate = amiWebApp.jspath('..field{.@name==="parentDatasetState"}.$', queryResult)[0] || "latest",
                                childDatasetID = amiWebApp.jspath('..field{.@name==="childDatasetID"}.$', queryResult)[0] || '',
                                childDatasetName = amiWebApp.jspath('..field{.@name==="childDatasetName"}.$', queryResult)[0] || '',
                                childDatasetstate = amiWebApp.jspath('..field{.@name==="childDatasetState"}.$', queryResult)[0] || "latest";
        
                            nodesRelationsStr.push(`${parentDatasetName} -> ${childDatasetName}`);

                            const childDataset = this._createNodeDataset(childDatasetID, childDatasetName, childDatasetstate, datasetID==childDatasetID);
                            const parentDataset = this._createNodeDataset(parentDatasetID, parentDatasetName, parentDatasetstate, datasetID==parentDatasetID);
                                               
                            nodes.set(parentDatasetID, parentDataset);
                            nodes.set(childDatasetID, childDataset);        
                        });        

                        nodes.forEach(function(node) {
                            if(node.selected) {
                                nodesOptionsStr.push(`${node.name}[color=black, fillcolor=${node.color}, URL="${node.URL}", tooltip="${node.tooltip}"]`);
                            }else{
                                nodesOptionsStr.push(`${node.name}[color=${node.color}, URL="${node.URL}", tooltip="${node.tooltip}"]`);
                            }
                        });

        
                        const separator = ";\n\t\t\t";
                        const dot = `
                        digraph G {
                            nodesep=0.7;
                            ranksep=.75;
                            tooltip="Provenance Diagram";
                        subgraph cluster_0 {
                            style=invis;
                            node [shape=rect, style="filled, rounded", color=white]
                            ${separator}${nodesOptionsStr.join(separator)}${separator}${nodesRelationsStr.join(separator)}
                        }
                        }`;
        
                        resolve(dot); // Résolution de la promesse avec le code Dot
                    } else {
                        $("#A2944C0A_9249_E4D2_3679_494C1A3AAAF0").html(`<h2>No parent dataset with the ID ${datasetID}</h2>`);
                        reject("Aucune donnée trouvée pour l'ID du dataset"); // Rejeter la promesse si pas de données
                    }
                }).fail(function (jqXHR, txtError) {
                    console.log(`Une erreur est survenue: ${txtError}`, jqXHR);
                    reject(txtError); // Rejeter la promesse en cas d'erreur
                });
            });
        },
    
        /*
            Take the dot code in argument and display the diagram
        */
        displayDot: function(dot) {
            Viz.instance().then(function(viz) { //Put in another function
                var svg = viz.renderSVGElement(dot);
                $("#A2944C0A_9249_E4D2_3679_494C1A3AAAF0").append(svg);
            });
        },

        _createNodeDataset: function(ID, name, state, selected) {
            let tooltip = "Latest"; //Based on state
            let color = "lightgreen"; //Normal
            if(state == "error") {
                tooltip = "Error";
                color = "red";
            }else if(state == "outdated") {
                tooltip = "Outdated";
                color = "orange";
            }else if(state == "canBeUpdated") {
                tooltip = "Can be updated";
                color = "Gold";
            }

            return {
                "name": name, 
                "state": state,
                "tooltip": tooltip,
                "color": color,
                "URL": `http://localhost:8000/?subapp=TableViewer&userdata=%7B%22command%22%3A%22BrowseQuery+-catalog%3D%5C%22ds_db%5C%22+-entity%3D%5C%22dataset%5C%22+-mql%3D%5C%22SELECT+*+WHERE+dataset.ID+%3D%27${ID}%27%5C%22+-limit%3D%5C%2210%5C%22%22%2C%22catalog%22%3A%22ds_db%22%2C%22entity%22%3A%22dataset%22%2C%22primaryField%22%3A%22ID%22%7D`,
                "selected": selected
            };
        },

        getDepthDatasetsTree: function() {
            //TODO (user scope)
        },

    }), window.provenanceDiagramApp = new ProvenanceDiagramApp
})();
