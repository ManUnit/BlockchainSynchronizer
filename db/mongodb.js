var MongoClient = require('mongodb').MongoClient,
    format = require('util').format;
var config = require('../etc/config');
var url = "mongodb://" + config.mongodb.user + ":" + config.mongodb.password + "@" + config.mongodb.host + ":" + config.mongodb.port + "/";

var obj = {}; // Main defind

obj.findPage = function(NoPagelow, NoPageHigh,callback) {
    // console.log(" HELLO COME TO  ");
    //userid = 11;
     if (!NoPagelow || !NoPageHigh ) return;
    MongoClient.connect(url, function(err, db) {
        var dbo = db.db("sync");
        //var query = { "number" : 1 }
        // var query = ({ "number": { $gte: NoPagelow, $lte: NoPageHigh }}) 
        var query = ({number: {$gte: NoPagelow , $lte : NoPageHigh } } ) 
        dbo.collection("blockers").find(query).sort({ number: 1 }).toArray(function(err, result) {
            callback(null,result);
        });
    });
}



exports.data = obj;

// obj.findIgnore ( 249990,290000 , function (err,result ){
//    console.log(result);
//    //console.log ( config.mongodb.user )
// }); 