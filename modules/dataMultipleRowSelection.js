const CosmosClient = require("@azure/cosmos").CosmosClient;
const { performance } = require('perf_hooks');
const appSettings = require('../Json_Files/appSettings.json');
const minMaxValue = require('./utilities');
const logger = require('../utils/logger').logger;
const DQLQueryStatsLogger = require('../utils/DQLQueryStatsLogger').logger;
const RawDataStatsLogger = require('../utils/RawDataStatsLogger').logger;
const headerLogger = require('../utils/headerLogger').logger;

var selectMetrics = {
    loopCounter : 0,
    avgRoundtripTime: 0.0,
    minSelectRoundtripTime: 100000.0,
    maxSelectRoundtripTime: 0.0,
    totalRoundtripTime: 0.0,
    avgRequestDuration: 0.0,
    minRequestDuration: 100000.0,
    maxRequestDuration: 0.0,
    totalRequestDuration: 0.0,
    avgRequestCharge: 0.0,
    minRequestUnit: 100000.0,
    maxRequestUnit: 0.0,
    totalRequestUnit: 0.0,
    cacheHitCount: 0,
    pragmaNoCache: 0,
    pragmaElse: 0,
    cacheControlElse: 0,
    numberOfReadRegions: 0,
    avgThrottleRetryCount: 0,
    avgThrottleRetryWaitTime: 0.0,
};
var metaDataLog;

async function dataSelection(connectionSettings, connectionMode, collectionName, querySpec, numOfIterations, callback) {
    var roundtripStartTime = performance.now();
    const endpoint = connectionSettings[0].endpoint;
    const key = connectionSettings[1].key;
    const databaseId = connectionSettings[2].databaseId;
    const partitionKey = connectionSettings[3].partitionKey;
    const consistencyLevel = connectionSettings[4].clientOptions.consistencyLevel;
    const preferredLocations = connectionSettings[5].preferredLocations;
    var client;

    if (appSettings.INCLUDE_CLIENT_OPTIONS) {
        if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
            client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations }, consistencyLevel });
        }
        else {
            client = new CosmosClient({ endpoint, key, consistencyLevel });
        }   
    }
    else {
        if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
            client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations } });
        }
        else {
            client = new CosmosClient({ endpoint, key });
        }  
    }
    

    const database = client.database(databaseId);
    const { container } = await client.database(databaseId).containers.createIfNotExists( {   
        id: collectionName, partitionKey 
    });

    try {
        selectMetrics = { loopCounter : 0,avgRoundtripTime: 0.0,minSelectRoundtripTime: 10000,maxSelectRoundtripTime: 0,totalRoundtripTime: 0,avgRequestDuration: 0,minRequestDuration: 10000.0,maxRequestDuration: 0.0,totalRequestDuration: 0.0,avgRequestCharge: 0,minRequestUnit: 1000.0,maxRequestUnit: 0.0,totalRequestUnit: 0.0,cacheHitCount: 0, pragmaNoCache: 0, pragmaElse: 0, cacheControlElse: 0, numberOfReadRegions: 0, avgThrottleRetryCount: 0, avgThrottleRetryWaitTime: 0.0};
        var failedQueries = 0, debugLogEntry = 0, selectRoundtripTime;
        for (i = 0; i < numOfIterations; i++) {
            selectMetrics.loopCounter++;
            var selectRoundtripStartTime = performance.now();
            try {
                const feedOptions = {
                    populateQueryMetrics: true
                  };
                const { resources: records, headers: headerInformation } = await container.items.query(querySpec, feedOptions).fetchNext();

                if (debugLogEntry < 5) {
                    headerLogger.info(JSON.stringify(headerInformation));
                    logger.debug("**********************************************");
                    debugLogEntry++;
                }
                if (i == 0) {
                    RawDataStatsLogger.info(JSON.stringify(records));
                    if (appSettings.PRINT_RECORDS_ON_CONSOLE) { console.log(records); }
                    if (appSettings.PRINT_HEADERS_ON_CONSOLE) { console.log(headerInformation); }
                }
                var selectRoundtripEndTime = performance.now();
                selectRoundtripTime = selectRoundtripEndTime - selectRoundtripStartTime;
                calculateQueryMetrics(selectRoundtripTime, headerInformation);
            }catch(err) {
                failedQueries++;
                console.log(err.message);
            }
        }
        logSelectProcessDesc(connectionSettings, connectionMode, collectionName, failedQueries, querySpec);
    }catch(err) {
        console.log(err.message);
    }
    var roundtripEndTime = performance.now();

    selectMetrics.totalRoundtripTime = (roundtripEndTime - roundtripStartTime).toFixed(2);
    selectMetrics.avgRoundtripTime = (selectMetrics.totalRoundtripTime/selectMetrics.loopCounter).toFixed(3);
    
    selectMetrics.avgRequestDuration = (selectMetrics.totalRequestDuration/selectMetrics.loopCounter).toFixed(3);
    
    selectMetrics.avgRequestCharge = (selectMetrics.totalRequestUnit/selectMetrics.loopCounter).toFixed(3);

    selectMetrics.avgThrottleRetryCount = (selectMetrics.avgThrottleRetryCount/selectMetrics.loopCounter).toFixed(3);

    selectMetrics.avgThrottleRetryWaitTime = (selectMetrics.avgThrottleRetryWaitTime/selectMetrics.loopCounter).toFixed(3);

    logSelectResponseStats();

    callback();
}



function calculateQueryMetrics(selectRoundtripTime, headerInformation) {
    var { newMinValue, newMaxValue } = minMaxValue.calculateMinMax(selectRoundtripTime, selectMetrics.minSelectRoundtripTime, selectMetrics.maxSelectRoundtripTime);
        selectMetrics.minSelectRoundtripTime = newMinValue;
        selectMetrics.maxSelectRoundtripTime = newMaxValue;

    //Request Duration 
    var requestDuration = parseFloat(headerInformation['x-ms-request-duration-ms']);
    if (requestDuration) {
        var { newMinValue, newMaxValue } = minMaxValue.calculateMinMax(requestDuration, selectMetrics.minRequestDuration, selectMetrics.maxRequestDuration);
        selectMetrics.minRequestDuration = newMinValue;
        selectMetrics.maxRequestDuration = newMaxValue;
        selectMetrics.totalRequestDuration = selectMetrics.totalRequestDuration + requestDuration;
    }       

    // Request Charge
    var requestCharge = parseFloat(headerInformation['x-ms-request-charge']);
    var { newMinValue, newMaxValue } = minMaxValue.calculateMinMax(requestCharge, selectMetrics.minRequestUnit, selectMetrics.maxRequestUnit);
    selectMetrics.minRequestUnit = newMinValue;
    selectMetrics.maxRequestUnit = newMaxValue;
    selectMetrics.totalRequestUnit = selectMetrics.totalRequestUnit + requestCharge;

    // Cache Hit
    var cacheHit = headerInformation['x-ms-cosmos-cachehit'];
    if(cacheHit) {
		if (cacheHit.toUpperCase() == "TRUE") {
            	selectMetrics.cacheHitCount++;
                // selectMetrics.totalRecordFetchedWithCache += queryMetrics.retrievedDocumentCount;
	    	}
        }

    if (headerInformation.pragma == 'no-cache') { selectMetrics.pragmaNoCache++; }
    else { selectMetrics.pragmaElse++; }
    if (headerInformation['cache-control'] != 'no-store, no-cache') { selectMetrics.cacheControlElse++;  }
    if(headerInformation['x-ms-number-of-read-regions']) {
        selectMetrics.numberOfReadRegions += parseInt(headerInformation['x-ms-number-of-read-regions']);
    }
    if(headerInformation['x-ms-throttle-retry-count']) {
        selectMetrics.avgThrottleRetryCount += headerInformation['x-ms-throttle-retry-count']; // Average will be calculated at the end of main function
    }
    if(headerInformation['x-ms-throttle-retry-wait-time-ms']) {
        selectMetrics.avgThrottleRetryWaitTime += headerInformation['x-ms-throttle-retry-wait-time-ms']; // Average will be calculated at the end of main function
    }
}


function logSelectProcessDesc(connectionSettings, connectionMode, collectionName, failedQueries, queryString) {

    var scenarioType = `MSEL-DGW_${connectionMode == "Dedicated Gateway" ? "Yes" : connectionMode == "Direct Connection" ? "No" : null}-Run_Num#${appSettings.RunNum}`;
    console.log("\n==============================================");
    console.log("\nProcess Desc: \tMulti Row Selecting Data Records");
    console.log(`\nScenario Type: \t${scenarioType}`);
    console.log(`\nDedicated GW Used: \t${connectionMode == "Dedicated Gateway" ? "Yes" : connectionMode == "Direct Connection" ? "No" : null}`);
    console.log(`\nCosmos DB Endpoint: \t${connectionSettings[0].endpoint}`);
    console.log(`\nDB Name: \t${connectionSettings[2].databaseId}`);
    console.log(`\nCollection Name: \t${collectionName}`);
    console.log(`\nPartition Key Name: \t${connectionSettings[3].partitionKey.paths[0]}`);
    console.log(`\nSuccessful queries: \t${selectMetrics.loopCounter-failedQueries}`);
    console.log(`\nQueries Failed: \t${failedQueries}`);
    console.log(`\nTotal Queries Processed: \t${selectMetrics.loopCounter}`);
    console.log(`\nSample query: \n\n${JSON.stringify(queryString)}`);

    // _______________________________________________________________________________
    // Logging into file
    logger.info("====================PROCESS START==========================");
    logger.info("Process Desc: \tMulti Row Selecting Data Records");
    logger.info(`Scenario Type: \t${scenarioType}`);
    logger.info(`Dedicated GW Used: \t${connectionMode == "Dedicated Gateway" ? "Yes" : connectionMode == "Direct Connection" ? "No" : null}`);
    logger.info(`Cosmos DB Endpoint: \t${connectionSettings[0].endpoint}`);
    logger.info(`DB Name: \t${connectionSettings[2].databaseId}`);
    logger.info(`Collection Name: \t${collectionName}`);
    logger.info(`Partition Key Name: \t${connectionSettings[3].partitionKey.paths[0]}`);
    logger.info(`Successful queries: \t${selectMetrics.loopCounter-failedQueries}`);
    logger.info(`Queries Failed: \t${failedQueries}`);
    logger.info(`Total Queries Processed: \t${selectMetrics.loopCounter}`);
    logger.info(`Sample query: `);
    logger.info(JSON.stringify(queryString));

    var dedicatedGatewayUsed = connectionMode == "Dedicated Gateway" ? 'Yes' : connectionMode == "Direct Connection" ? 'No' : 'null';
    // DMLQueryStats.csv file log
   metaDataLog = 'Multi Row Selecting Data Records,' + scenarioType + ',' + dedicatedGatewayUsed + ',' + connectionSettings[0].endpoint + ',' + connectionSettings[2].databaseId + ','+ collectionName + ',' + connectionSettings[3].partitionKey.paths[0] + ',' + String(selectMetrics.loopCounter-failedQueries) + ',' + String(failedQueries) + ',' + String(selectMetrics.loopCounter) + ',' + queryString;
}


function logSelectResponseStats() {

    console.log(`\nQuery Response Stats\n---------------------`);
    console.log(`\nTotal Cache Hit('x-ms-cosmos-cachehit'): \t${selectMetrics.cacheHitCount}`);
    console.log(`\nNumber of times pragma is no-cache: \t${selectMetrics.pragmaNoCache}`);
    console.log(`\nNumber of times pragma is not no-cache: \t${selectMetrics.pragmaElse}`);
    console.log(`\nNumber of times cache-control is not no-store and no-cache: \t${selectMetrics.cacheControlElse}`);
    console.log(`\nTotal Number of read regions: \t${selectMetrics.numberOfReadRegions}`);
    console.log(`\nAverage of Throttle Retry Count: \t${selectMetrics.avgThrottleRetryCount}`);
    console.log(`\nAverage of Throttle Retry Wait Time: \t${selectMetrics.avgThrottleRetryWaitTime}`);
    console.log("\n**********************************************");
    console.log(`\nProcess Round Trip : Avg Time to process a single Record: \t${selectMetrics.avgRoundtripTime} Millis`);
    console.log(`\nProcess Round Trip : Min Time taken to select: \t${selectMetrics.minSelectRoundtripTime.toFixed(3)} Millis`);
    console.log(`\nProcess Round Trip : Max Time taken to select: \t${selectMetrics.maxSelectRoundtripTime.toFixed(3)} Millis`);
    console.log(`\nProcess Round Trip : Total Time taken to select: \t${selectMetrics.totalRoundtripTime} Millis`);
    console.log("\n**********************************************");
    console.log(`\nCosmos DB: Avg Time to process a single Record: \t${selectMetrics.avgRequestDuration} Millis`);
    console.log(`\nCosmos DB: Min Time taken to select: \t${selectMetrics.minRequestDuration} Millis`);
    console.log(`\nCosmos DB: Max Time taken to select: \t${selectMetrics.maxRequestDuration} Millis`);
    console.log(`\nCosmos DB: Total Time taken to select: \t${selectMetrics.totalRequestDuration.toFixed(3)} Millis`);
    console.log("\n**********************************************");
    console.log(`\nDifference in Avg Time to Execute Query ( NW + Compute Delay): \t${(selectMetrics.avgRoundtripTime - selectMetrics.avgRequestDuration).toFixed(3)} Millis`);
    console.log(`\nDifference in Total Time to Execute Query ( NW + Compute Delay): \t${(selectMetrics.totalRoundtripTime - selectMetrics.totalRequestDuration).toFixed(3)} Millis`);
    console.log("\n**********************************************");
    console.log(`\nAvg RU to process a single Record: \t${selectMetrics.avgRequestCharge}`);
    console.log(`\nMin RU taken to select: \t${selectMetrics.minRequestUnit}`);
    console.log(`\nMax RU taken to select: \t${selectMetrics.maxRequestUnit}`);
    console.log(`\nTotal RU taken to select: \t${selectMetrics.totalRequestUnit.toFixed(3)}`);
    console.log("\n==============================================");

    // _______________________________________________________________________________
    // Logging into file
    logger.info(`Query Response Stats`);
    logger.info(`---------------------`);
    logger.info(`Total Cache Hit('x-ms-cosmos-cachehit'): \t${selectMetrics.cacheHitCount}`);
    logger.info(`Number of times pragma is no-cache: \t${selectMetrics.pragmaNoCache}`);
    logger.info(`Number of times pragma is not no-cache: \t${selectMetrics.pragmaElse}`);
    logger.info(`Number of times cache-control is not no-store and no-cache: \t${selectMetrics.cacheControlElse}`);
    logger.info(`Total Number of read regions: \t${selectMetrics.numberOfReadRegions}`);
    logger.info(`Average of Throttle Retry Count: \t${selectMetrics.avgThrottleRetryCount}`);
    logger.info(`Average of Throttle Retry Wait Time: \t${selectMetrics.avgThrottleRetryWaitTime}`);
    logger.info("**********************************************");
    logger.info(`Process Round Trip : Avg Time to process a single Records: \t${selectMetrics.avgRoundtripTime} Millis`);
    logger.info(`Process Round Trip : Min Time taken to select: \t${selectMetrics.minSelectRoundtripTime.toFixed(3)} Millis`);
    logger.info(`Process Round Trip : Max Time taken to select: \t${selectMetrics.maxSelectRoundtripTime.toFixed(3)} Millis`);
    logger.info(`Process Round Trip : Total Time taken to select: \t${selectMetrics.totalRoundtripTime} Millis`);
    logger.info("**********************************************");
    logger.info(`Cosmos DB: Avg Time to process a single Records: \t${selectMetrics.avgRequestDuration} Millis`);
    logger.info(`Cosmos DB: Min Time taken to select: \t${selectMetrics.minRequestDuration} Millis`);
    logger.info(`Cosmos DB: Max Time taken to select: \t${selectMetrics.maxRequestDuration} Millis`);
    logger.info(`Cosmos DB: Total Time taken to select: \t${selectMetrics.totalRequestDuration.toFixed(3)} Millis`);
    logger.info("**********************************************");
    logger.info(`Difference in Avg Time to Execute Query ( NW + Compute Delay): \t${(selectMetrics.avgRoundtripTime - selectMetrics.avgRequestDuration).toFixed(3)} Millis`);
    logger.info(`Difference in Total Time to Execute Query ( NW + Compute Delay): \t${(selectMetrics.totalRoundtripTime - selectMetrics.totalRequestDuration).toFixed(3)} Millis`);
    logger.info("**********************************************");
    logger.info(`Avg RU to process a single Records: \t${selectMetrics.avgRequestCharge}`);
    logger.info(`Min RU taken to select: \t${selectMetrics.minRequestUnit}`);
    logger.info(`Max RU taken to select: \t${selectMetrics.maxRequestUnit}`);
    logger.info(`Total RU taken to select: \t${selectMetrics.totalRequestUnit.toFixed(3)}`);
    logger.info("========================PROCESS END======================");


    // DMLQueryStats.csv file log
    // DMLQueryStatsLogger.info('YYYY-MM-DD HH:MM:SS:MS,PROCESS_DESC,SCENARIO_TYPE,DGW USED?,DB_ENDPOINT,DB_NAME,COLL_NAME,PART_KEY_NAME,NUM_RECORDS,FAILED_RECORDS,PROCESSED_RECORDS,SAMPLE_QUERY,TOT_CACHE_HIT,PRAGMA=NO-CACHE,PRAGMA!=NO-CACHE,CACHE-CONTROL!="no-store,no-cache",NUM_READ_REGIONS,AVG_THROTTLE_RTRY_COUNT,AVG_RTRY_WAIT_TIME,PRT:PROCESS_AVG_TIME,PRT:PROCESS_MIN_TIME,PRT:PROCESS_MAX_TIME,PRT:PROCESS_TOTAL_TIME,CoDB:PROCESS_AVG_TIME,CoDB:PROCESS_MIN_TIME,CoDB:PROCESS_MAX_TIME,CoDB:PROCESS_TOTAL_TIME,DIFF_AVG_TIME,DIFF_TOTAL_TIME,PROCESS_AVG_RU,PROCESS_MIN_RU,PROCESS_MAX_RU,PROCESS_TOTAL_RU');
    DQLQueryStatsLogger.info(`${metaDataLog},${selectMetrics.cacheHitCount},${selectMetrics.pragmaNoCache},${selectMetrics.pragmaElse},${selectMetrics.cacheControlElse},${selectMetrics.numberOfReadRegions},${selectMetrics.avgThrottleRetryCount},${selectMetrics.avgThrottleRetryWaitTime},${selectMetrics.avgRoundtripTime},${selectMetrics.minSelectRoundtripTime.toFixed(3)},${selectMetrics.maxSelectRoundtripTime.toFixed(3)},${selectMetrics.totalRoundtripTime},${selectMetrics.avgRequestDuration},${selectMetrics.minRequestDuration},${selectMetrics.maxRequestDuration},${selectMetrics.totalRequestDuration.toFixed(3)},${(selectMetrics.avgRoundtripTime - selectMetrics.avgRequestDuration).toFixed(3)},${(selectMetrics.totalRoundtripTime - selectMetrics.totalRequestDuration).toFixed(3)},${selectMetrics.avgRequestCharge},${selectMetrics.minRequestUnit},${selectMetrics.maxRequestUnit},${selectMetrics.totalRequestUnit.toFixed(3)}`);
}


module.exports.dataMultipleRowSelection = { dataSelection };