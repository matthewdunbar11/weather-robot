var MongoClient = require('mongodb').MongoClient;


var url = 'mongodb://mongo:27017/weather-import';

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



    }
});



