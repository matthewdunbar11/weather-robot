var MongoClient = require('mongodb').MongoClient;


var url = 'mongodb://weather-robot:mvl6uUXrZVM3ZiDwdkvMttODyP4Kv7wjc0setRQ3BljzH29Dp6uxn7tZWYBpza2DLqlrfOkmcuvo6jeEh2ff9w==@weather-robot.documents.azure.com:10255/weather?ssl=true&replicaSet=globaldb';

MongoClient.connect(url, function(err, db) {
    if(err === null) {
        console.log('Connected to mongo');

        var RTM = require("satori-sdk-js");

        var endpoint = "wss://open-data.api.satori.com";
        var appKey = "Ac4e9Db66FCBCF68B84fCd6aF9C36d36";
        var channel = "full-weather";

        var rtm = new RTM(endpoint, appKey);
        rtm.on("enter-connected", function() {
        console.log("Connected to RTM!");
        });

        var subscription = rtm.subscribe(channel, RTM.SubscriptionMode.SIMPLE);
        var collection = db.collection('weather');
        
        subscription.on('rtm/subscription/data', function (pdu) {
        pdu.body.messages.forEach(function (msg) {
            msg.time = new Date();            
            collection.insertOne(msg);
        });
        });

        rtm.start();


        var http = require('http');
        
        //step 2) start the server
        http.createServer(function (req, res) {
        
        //set an HTTP header of 200 and the meta type
        res.writeHead(200, {'Content-Type': 'text/plain'});
        
        //write something to the request and end it
        res.end('Your node.js server is running on localhost:80');
        
        }).listen(80);//step 3) listen for a request on port 3000



    }
});



