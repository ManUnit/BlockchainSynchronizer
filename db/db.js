/*

Copyright : Anan Paenthongkham 
email : hs1gab@gmail.com
update : 2018-07-09
Blockchain synchronization version 1.0  

*/

var config   = require('../etc/config')
var mongoose = require( 'mongoose' );
mongoose.Promise = require('bluebird');
var options = {
    user: config.mongodb.user ,
    pass: config.mongodb.password ,
    auth: {
        authdb: config.mongodb.authenticationDatabase
    },
    useMongoClient: true,
    autoIndex: false, 
    reconnectTries: Number.MAX_VALUE, 
    reconnectInterval: 500, 
    poolSize: 10,   
    bufferMaxEntries: 0
}
mongoose.connect('mongodb://'+ config.mongodb.host +':' + config.mongodb.port +  '/' + config.mongodb.dbsync , options);
var Schema   = mongoose.Schema;
console.log ( " == Sync Database Started == " ) ; 

var Blocker = new Schema(
{
    "number": {type: Number, index: {unique: true}},
    "hash": String,
    "parentHash": String,
    "nonce": String,
    "sha3Uncles": String,
    "logsBloom": String,
    "transactionsRoot": String,
    "stateRoot": String,
    "receiptRoot": String,
    "miner": String,
    "difficulty": String,
    "totalDifficulty": String,
    "size": Number,
    "extraData": String,
    "gasLimit": Number,
    "gasUsed": Number,
    "timestamp": Number,
    "blockTime": Number,
    "uncles": [String]
});

var Contract = new Schema(
{
    "address": {type: String, index: {unique: true}},
    "creationTransaction": String,
    "contractName": String,
    "compilerVersion": String,
    "optimization": Boolean,
    "sourceCode": String,
    "abi": String,
    "byteCode": String
}, {collection: "Contract"});

var Transaction = new Schema(
{
    "hash": {type: String, index: {unique: true}},
    "nonce": Number,
    "blockHash": String,
    "blockNumber": Number,
    "transactionIndex": Number,
    "from": String,
    "to": String,
    "value": String,
    "gas": Number,
    "gasPrice": String,
    "timestamp": Number,
    "input": String
}, {collection: "Transaction"});

var arrDiff = function arr_diff (a1, a2) {
    var a = [], diff = [];
    for (var i = 0; i < a1.length; i++) {
        a[a1[i]] = true;
    }
    for (var i = 0; i < a2.length; i++) {
        if (a[a2[i]]) {
            delete a[a2[i]];
        } else {
            a[a2[i]] = true;
        }
    }
    for (var k in a) {
        diff.push(k);
    }
    return diff;
}


Transaction.index({blockNumber:-1});
Transaction.index({from:1, blockNumber:-1});
Transaction.index({to:1, blockNumber:-1});
Blocker.index({miner:1});

mongoose.model('Contract', Contract);
mongoose.model('Transaction', Transaction);
mongoose.model('Blocker', Blocker);
module.exports.Contract = mongoose.model('Contract');
module.exports.Transaction = mongoose.model('Transaction');
module.exports.Blocker = mongoose.model('Blocker');
module.exports.arrDiff  =  arrDiff  ; 



