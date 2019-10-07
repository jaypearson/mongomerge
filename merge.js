// Place holders for execution metrics
var numErrors = 0;
var numDocs = 0;
var numUpdated = 0;
var numUpserted = 0;

setVerboseShell(false);

// Load configuration - result should be in 'config' object
load("config.js");

try {
    // Connect to the MongoDB cluster
    cluster = Mongo(config.connStr);

    // Get a reference to the database
    db = cluster.getDB(config.databaseName);

    // Get a reference to the source collection
    var sourceColl = db.getCollection(config.source);

    // Get a reference to the target collection
    var targetColl = db.getCollection(config.target);

    // Force a collection scan with natural sort order
    var sourceCursor = sourceColl.find().hint({ $natural: 1 });

    // Loop through cursor and process each document
    while (sourceCursor.hasNext()) {
        mergeDoc(sourceCursor.next());
    }

    // Print out summary of results
    print(
        `${numDocs} docs ${numErrors} errors ${numUpdated} updated ${numUpserted} upserted`
    );
} catch (ex) {
    // Any exception that happens here is most likely fatal
    // Because this script is idempotent the correct action in every case should be to restart the script
    print(ex);
    // No way to return an error code as far as I'm aware
    exit;
}

function mergeDoc(doc) {
    // Match document by _id
    var match = { _id: doc["_id"] };

    // Upsert document, only update one, set write concern
    var options = {
        upsert: true,
        multi: false,
        writeConcern: config.writeConcern
    };

    // Execute update operation and save result
    result = targetColl.update(match, { $set: doc }, options);

    // Check for errors and update execution metrics
    if (printErrors(result)) {
        numErrors++;
    } else {
        numDocs++;
        numUpdated += result.nModified;
        numUpserted += result.nUpserted;
    }
}

// Print errors to the console
// There is no way to log directly to a local file from mongo shell
// The errors could be piped via standard output to a file
function printErrors(result) {
    errors = false;
    if (result.hasWriteError()) {
        var we = result.writeError;
        print(`WRITE ERROR: ${we.code} - ${we.errmsg}`);
        errors = true;
    }
    if (result.hasWriteConcernError()) {
        var wce = result.writeConcernError;
        print(
            `WRITE CONCERN ERROR: ${wce.code} - ${wce.errInfo} - ${wce.errmsg}`
        );
        errors = true;
    }
    return errors;
}
