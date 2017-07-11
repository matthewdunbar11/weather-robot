var MongoClient = require('mongodb').MongoClient;


const express = require('express');
const app = express();

app.get('/', function(req, res) {
    res.send('Hello world!');
});

var port = process.env.PORG || 8070;
app.listen(port, function() {
    console.log('App listening on port ' + port);
})

//var url = 'mongodb://mongo:27017/weather-import';
var url = process.env.MONGODB_URI || 'mongodb://mongo:27017/weather-import';

currentOperation = 'beginning mongo connect';
MongoClient.connect(url, function(err, db) {

    if(err === null) {
        currentOperation = 'mongo connected';
        console.log('Connected to mongo');

        var collection = db.collection('weather');
        var destinationCollection = db.collection('weather-transformed');
        collection.find({}, (function(err, cursor) {
            function processWeatherEntry(err, weatherEntry) {
                var station_id = weatherEntry.station_id;
                var observation_time = new Date(weatherEntry.observation_time);
                var observation_date = new Date(observation_time);
                observation_date.setHours(0,0,0,0);
                destinationCollection.findOne({observation_date: observation_date, station_id: station_id}).then(function(result) {
                    if(result == null) {
                        result = {};
                    }

                    result = create_max_property(weatherEntry, result, "temp_c", observation_time);
                    result = create_max_property(weatherEntry, result, "dewpoint_c", observation_time);
                    result = create_min_property(weatherEntry, result, "temp_c", observation_time);
                    result = create_min_property(weatherEntry, result, "dewpoint_c", observation_time);
                    result = create_min_property(weatherEntry, result, "sea_level_pressure_mb", observation_time);
                    result = create_max_property(weatherEntry, result, "sea_level_pressure_mb", observation_time);
                    result = create_earliest_property(weatherEntry, result, "sea_level_pressure_mb", observation_time);
                    result = create_latest_property(weatherEntry, result, "sea_level_pressure_mb", observation_time);
                    result = create_earliest_property(weatherEntry, result, "temp_c", observation_time);
                    result = create_earliest_property(weatherEntry, result, "dewpoint_c", observation_time);
                    result = create_latest_property(weatherEntry, result, "temp_c", observation_time);
                    result = create_latest_property(weatherEntry, result, "dewpoint_c", observation_time);
                    
                    if(typeof result.observations == 'undefined') {
                        result.observations = [];
                    }

                    if(!result.observations.some(function(e) { return e._id == weatherEntry._id })) {
                        result.observations.push(weatherEntry);
                    }

                    result.station_id = weatherEntry.station_id;
                    result.observation_date = observation_date;

                    // This needs to handle callbacks properly so that the loop we're in (collection.find({}).forEach...) doesn't progress
                    // until the insert/update is complete (what if we are in the middle of inserting a locations weather and then we try to update
                    // with another weather entry before the insert is done?)
                    if(typeof result._id == 'undefined') {
                        destinationCollection.insertOne(result, function() {
                            cursor.nextObject(processWeatherEntry);
                        });
                    }
                    else {
                        destinationCollection.updateOne({_id: result._id}, result, function() {
                            cursor.nextObject(processWeatherEntry);
                        });
                    }
                });

            }

            cursor.nextObject(processWeatherEntry);
        }));
        



    }
    else {
        currentOperation = 'mongo connect error: ' + err;
    }
});


function create_max_property(source, destination, propertyName, observation_time) {
    if(typeof destination["max_" + propertyName] != 'undefined') {
        var existing_value = destination["max_" + propertyName];
        var new_value = source[propertyName];
        if(new_value > existing_value) {
            destination["max_" + propertyName] = source[propertyName];
            destination["max_" + propertyName + "_time"] = observation_time;
        }
    }
    else {
        destination["max_" + propertyName] = source[propertyName];
        destination["max_" + propertyName + "_time"] = observation_time;
    }

    return destination;
}

function create_min_property(source, destination, propertyName, observation_time) {
    if(typeof destination["min_" + propertyName] != 'undefined') {
        var existing_value = destination["min_" + propertyName];
        var new_value = source[propertyName];
        if(new_value < existing_value) {
            destination["min_" + propertyName] = source[propertyName];
            destination["min_" + propertyName + "_time"] = observation_time;
        }
    }
    else {
        destination["min_" + propertyName] = source[propertyName];
        destination["min_" + propertyName + "_time"] = observation_time;
    }

    return destination;    
}

function create_earliest_property(source, destination, propertyName, observation_time) {
    if(typeof destination["earliest_" + propertyName + "_time"] != 'undefined') {
        var existing_value = destination["earliest_" + propertyName + "_time"];
        
        if(observation_time < existing_value) {
            destination["earliest_" + propertyName] = source[propertyName];
            destination["earliest_" + propertyName + "_time"] = observation_time;
        }
    }
    else {
        destination["earliest_" + propertyName] = source[propertyName];
        destination["earliest_" + propertyName + "_time"] = observation_time;
    }

    return destination;    
}

function create_latest_property(source, destination, propertyName, observation_time) {
    if(typeof destination["latest_" + propertyName + "_time"] != 'undefined') {
        var existing_value = destination["latest_" + propertyName + "_time"];
        
        if(observation_time > existing_value) {
            destination["latest_" + propertyName] = source[propertyName];
            destination["latest_" + propertyName + "_time"] = observation_time;
        }
    }
    else {
        destination["latest_" + propertyName] = source[propertyName];
        destination["latest_" + propertyName + "_time"] = observation_time;
    }

    return destination;    
}
