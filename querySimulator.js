const cosmosDBConnectionSettings = require("./Json_Files/cosmosDBConnectionSettings.json");
const appSettings = require('./Json_Files/appSettings.json');
const prompt = require('prompt');
const dataMultipleRowSelection = require('./modules/dataMultipleRowSelection').dataMultipleRowSelection;


async function main() {
    prompt.start();
    console.log("\n1. Execute query from console\n2. Execute query from appSettings file\nEnter your choice...");
    prompt.get(['choice'], function (err, result) {
        if(err) { return onErr(err); }
        switch(result.choice) {
            case '1':   var connectionSettings, connectionMode, collectionName, querySpec, numOfIterations;
                        console.log("\n1. Without Dedicated Gateway\n2. With dedicated gateway\n3. Both");
                        prompt.get(['gatewayChoice'], function (err, gatewayResult) {
                            if (err) { return onErr(err); }
                            console.log("\nEnter the Collection Name...");
                            prompt.get(['collName'], function (err, collNameResult) {
                                if(err) { return onErr(err); }
                                console.log("\nEnter the query to execute(given the collection name as simply c)...");
                                prompt.get(['query'], function (err, queryResult) {
                                    if(err) { return onErr(err); }
                                    console.log("\nEnter the number of iterations...");
                                    prompt.get(['numOfIterations'], function (err, numOfIterationsResult) {
                                        if(err) { return onErr(err); }
                                        if (gatewayResult.gatewayChoice == '3') {
                                            collectionName = collNameResult.collName;

                                            var queryString = queryResult.query;
                                            // console.log("!!!!!!!!!!!!!!!", queryResult.query.query, queryResult.query.parameters);
                                            
                                            if(queryString.includes("query") && queryString.includes("parameters")) {
                                                querySpec = JSON.parse(queryString);
                                            }
                                            else {
                                                querySpec = {
                                                    query: queryString
                                                };
                                            }
                                            numOfIterations = parseInt(numOfIterationsResult.numOfIterations);
                                            connectionSettings = cosmosDBConnectionSettings.cosmosDBDirectConnection;
                                            connectionMode = "Direct Connection";
                                            dataMultipleRowSelection.dataSelection(connectionSettings, connectionMode, collectionName, querySpec, numOfIterations, function() {
                                                connectionSettings = cosmosDBConnectionSettings.cosmosDBDedicatedGateway;
                                                connectionMode = "Dedicated Gateway";
                                                dataMultipleRowSelection.dataSelection(connectionSettings, connectionMode, collectionName, querySpec, numOfIterations, function() { 
                                                });
                                            });
                                        }
                                        else {
                                            if (gatewayResult.gatewayChoice == '1') { connectionSettings = cosmosDBConnectionSettings.cosmosDBDirectConnection; connectionMode = "Direct Connection"; }
                                            if (gatewayResult.gatewayChoice == '2') { connectionSettings = cosmosDBConnectionSettings.cosmosDBDedicatedGateway; connectionMode = "Dedicated Gateway"; }
                                            collectionName = collNameResult.collName;

                                            var queryString = queryResult.query;
                                            // console.log("!!!!!!!!!!!!!!!", queryString, queryString.query, queryString.parameters);

                                            if(queryString.includes("query") && queryString.includes("parameters")) {
                                                querySpec = JSON.parse(queryString);
                                            }
                                            else {
                                                querySpec = {
                                                    query: queryString
                                                };
                                            }
                                            numOfIterations = parseInt(numOfIterationsResult.numOfIterations);
                                            dataMultipleRowSelection.dataSelection(connectionSettings, connectionMode, collectionName, querySpec, numOfIterations, function() {});
                                        }
                                    });
                                });
                            });
                        });
                        function onErr(err) {
                            console.log(err);
                            return 1;
                        }
                        break;
            case '2':   var connectionSettings, connectionMode, collectionName, querySpec, numOfIterations;
                        var gatewayChoice = appSettings.gatewayUsed;
                            if (gatewayChoice == 'Both') {
                                collectionName = appSettings.collectionName;

                                var queryString = appSettings.queryString;
                                if(queryString.includes("query") && queryString.includes("parameters")) {
                                    querySpec = JSON.parse(queryString.replace(/'/g,"\""));
                                }
                                else {
                                    querySpec = {
                                        query: queryString
                                    };
                                }
                                numOfIterations = appSettings.numofIterations;
                                connectionSettings = cosmosDBConnectionSettings.cosmosDBDirectConnection;
                                connectionMode = "Direct Connection";
                                dataMultipleRowSelection.dataSelection(connectionSettings, connectionMode, collectionName, querySpec, numOfIterations, function() {
                                    connectionSettings = cosmosDBConnectionSettings.cosmosDBDedicatedGateway;
                                    connectionMode = "Dedicated Gateway";
                                    dataMultipleRowSelection.dataSelection(connectionSettings, connectionMode, collectionName, querySpec, numOfIterations, function() { 
                                    });
                                });
                            }
                            else {
                                if (gatewayChoice == 'No') { connectionSettings = cosmosDBConnectionSettings.cosmosDBDirectConnection; connectionMode = "Direct Connection"; }
                                else if (gatewayChoice == 'Yes') { connectionSettings = cosmosDBConnectionSettings.cosmosDBDedicatedGateway; connectionMode = "Dedicated Gateway"; }
                                collectionName = appSettings.collectionName;

                                var queryString = appSettings.queryString;
                                if(queryString.includes("query") && queryString.includes("parameters")) {
                                    querySpec = JSON.parse(queryString.replace(/'/g,"\""));
                                }
                                else {
                                    querySpec = {
                                        query: queryString
                                    };
                                }
                                numOfIterations = appSettings.numofIterations;
                                dataMultipleRowSelection.dataSelection(connectionSettings, connectionMode, collectionName, querySpec, numOfIterations, function() {});
                            }
                            break;
            default:    console.log("\nWrong Entry!!!");
                        break;
        } 
    });
}

main();