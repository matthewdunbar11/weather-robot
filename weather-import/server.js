var MongoClient = require('mongodb').MongoClient;


const express = require('express');
const app = express();

var currentOperation = 'starting';
app.get('/', function(req, res) {
    res.send('Hello world!');
});


app.get('/status', function(req, res) {
    res.send(currentOperation);
});

app.listen(process.env.PORT || 3000, function() {
    console.log('App listening on port ' + process.env.PORT || 3000);
})

//var url = 'mongodb://mongo:27017/weather-import';
var url = process.env.MONGODB_URI || 'mongodb://mongo:27017/weather-import';

currentOperation = 'beginning mongo connect';
MongoClient.connect(url, function(err, db) {

    if(err === null) {
        currentOperation = 'mongo connected';
        console.log('Connected to mongo');

        var RTM = require("satori-sdk-js");

        var endpoint = "wss://open-data.api.satori.com";
        var appKey = "Ac4e9Db66FCBCF68B84fCd6aF9C36d36";
        //var channel = "full-weather";
        var channel = "METAR-AWC-US";

        var rtm = new RTM(endpoint, appKey);
        currentOperation = 'beginning satori subscription';
        rtm.on("enter-connected", function() {
            currentOperation = 'connected to satori';
            console.log("Connected to RTM!");
        });

        var subscription = rtm.subscribe(channel, RTM.SubscriptionMode.SIMPLE);
        var collection = db.collection('weather');
        collection.createIndex({"station_id":1,"observation_time":1}, {unique: true}, function() {

            var messagesReceived = 0;
            subscription.on('rtm/subscription/data', function (pdu) {            
            pdu.body.messages.forEach(function (msg) {

                currentOperation = 'successfully received ' + ++messagesReceived + ' messages';
                msg.time = new Date();            
                collection.insertOne(msg);
            });
            });

            rtm.start();

        });
        



    }
    else {
        currentOperation = 'mongo connect error: ' + err;
    }
});



