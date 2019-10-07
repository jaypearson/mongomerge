config = {
    databaseName: "sample",
    target: "target",
    source: "tweets",
    writeConcern: { w: "majority", j: true, wtimeout: 100 },
    connStr: "mongodb://localhost:27017"
};

// Execute with: mongo.exe --nodb --quiet "merge.js"
