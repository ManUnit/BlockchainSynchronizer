var mainConfig = require('./etc/config');
var dbm = require('./db/db.js');
var fs = require('fs');
var Web3 = require('web3');
var mongoose = require('mongoose');
var db = require('./db/mongodb');
var Blocker = mongoose.model('Blocker');
var Transaction = mongoose.model('Transaction');
var etherUnits = require("./lib/etherUnits.js");
var BigNumber = require('bignumber.js');
const util = require('util');
var web3 = new Web3(new Web3.providers.HttpProvider('http://' + mainConfig.web3js.nodeAddr + ':' + mainConfig.web3js.port.toString()));

var monitorNewBlock = function(config) {
    var newBlocks = web3.eth.filter("latest");
    newBlocks.watch(function(error, latestBlock) {
        if (error) {
            console.log('Error: ' + error);
        } else if (latestBlock == null) {
            console.log('Warning: null block hash');
        } else {
            // console.log('\n\n\nFound new block: ' + latestBlock);
            if (web3.isConnected()) {
                web3.eth.getBlock(latestBlock, true, function(error, blockData) {
                    if (error) {
                        console.log('Warning: error on getting block with hash/number: ' + latestBlock + ': ' + error);
                    } else if (blockData == null) {
                        console.log('Warning: null data received with hash/number: ' + latestBlock);
                    } else {
                        //  console.log("==>\n\n");
                        // console.log (blockData) ;
                        process.stdout.write("Newblocknumber " + blockData.number + " hash : " + latestBlock + " \n");
                        syncBlockToDB(config, blockData, true);
                        syncTransactionToDB(config, blockData, true);
                    }
                });
            } else {
                console.log('Error: Web3 connection time out trying to get block ' + latestBlock + ' retrying connection now ');
                monitorNewBlock(config); // Loop 
            }
        }
    });
}
/*


*/

var fullSyncChain = function(config, nextBlock) {
    //	console.log ("< calling to full sync >");
    if (web3.isConnected()) {
        if (typeof nextBlock === 'undefined') {
            findLowestBlock(config, function(error, startBlock) {
                if (error) {
                    console.log('ERROR: error: ' + error);
                    return;
                }
                fullSyncChain(config, startBlock);
            });
            return;
        }

        if (nextBlock == null) {
            console.log('nextBlock is null');
            return;
        } else if (nextBlock < config.startBlock) {
            syncBlockToDB(config, null, true);
            syncTransactionToDB(config, null, true);
            console.log('*** Sync Finsihed ***');
            config.syncAll = false;
            return;
        }

        var count = config.bulkSize;
        while (nextBlock >= config.startBlock && count > 0) {
            web3.eth.getBlock(nextBlock, true, function(error, blockData) {
                if (error) {
                    console.log('Warning: error on getting block with hash/number: ' + nextBlock + ': ' + error);
                } else if (blockData == null) {
                    console.log('Warning: null block data received from the block with hash/number: ' + nextBlock);
                } else {
                    syncBlockToDB(config, blockData);
                    syncTransactionToDB(config, blockData);
                }
            });
            nextBlock--;
            count--;
        }

        setTimeout(function() { fullSyncChain(config, nextBlock); }, 500);
    } else {
        console.log('Error: Web3 connection time out trying to get block ' + nextBlock + ' retrying connection now');
        fullSyncChain(config, nextBlock);
    }
}

/*

*/

var findLowestBlock = function(config, callback) {
    var blockNumber = null;
    var oldBlockFind = Blocker.find({}, "number").lean(true).sort('number').limit(1); // Find oldest blocknumber
    oldBlockFind.exec(function(err, docs) {
        if (err || !docs || docs.length < 1) {
            // not found in db. sync from config.endBlock or 'latest'
            if (web3.isConnected()) {
                var currentBlock = web3.eth.blockNumber;
                var latestBlock = config.endBlock || currentBlock || 'latest';
                console.log("latestBlock is : " + latestBlock);
                if (latestBlock === 'latest') {
                    web3.eth.getBlock(latestBlock, true, function(error, blockData) {
                        if (error) {
                            console.log('Warning: error on getting block with hash/number: ' + latestBlock + ': ' + error);
                        } else if (blockData == null) {
                            console.log('Warning: null block data received from the block with hash/number: ' + latestBlock);
                        } else {
                            console.log('Starting block number = ' + blockData.number);
                            blockNumber = blockData.number - 1;
                            callback(null, blockNumber);
                        }
                    });
                } else {
                    console.log('Starting block number = ' + latestBlock);
                    blockNumber = latestBlock - 1;
                    callback(null, blockNumber);
                }
            } else {
                console.log('Error: Web3 connection error');
                callback(err, null);
            }
        } else {
            blockNumber = docs[0].number - 1;
            console.log('Older block found. Starting block number = ' + blockNumber);
            callback(null, blockNumber);
        }
    });
}

/*

*/

var recoveryIgnoreBlockToDB = function(config, lowBlock, oldMaxPage, lastMaxBlock) {
    //	console.log ("< calling to full sync >");
    if (web3.isConnected()) {
        if (typeof oldMaxPage === 'undefined' || typeof lastMaxBlock === 'undefined' || typeof lowBlock === 'undefined') {
            var MaxBlockInDB = Blocker.find({}, "number").lean(true).sort('-number').limit(1);
            MaxBlockInDB.exec(function(maxerr, maxBlock) {
                findLowestBlock(config, function(error, lowestBlock) {
                    if (error || maxerr) {
                        console.log('ERROR: error: ' + error);
                        return;
                    }
                    var downPage = maxBlock[0].number - config.pageSize;
                    console.log("let  go find ignore block  with  total find : " + maxBlock[0].number + " to : " + (lowestBlock + 1))
                    recoveryIgnoreBlockToDB(config, lowestBlock, downPage, maxBlock[0].number);
                });
            });
            return;
        }

        if (oldMaxPage <= 0 || oldMaxPage < lowBlock) {
            syncBlockToDB(config, null, true);
            syncTransactionToDB(config, null, true);
            console.log('*** Recovery Ignore blocks Finished ! ***');
            config.syncAll = false;
            return;
        }

        var count = config.pageSize;
        //var pageSkip = lastMaxBlock - oldMaxPage;
        console.log("Search ignore block from :  " + oldMaxPage + " to : " + lastMaxBlock);
        var startPageNumber = oldMaxPage;

        //console.log("\n\");
        //  arrDiff 

        db.data.findPage(oldMaxPage, lastMaxBlock, function(err, result) {
            //console.log(result);
            console.log("Start Box : " + oldMaxPage);
            //var SearchLength = config.pageSize + 1;
            var lostNum = [];
            var storedNum = [];
            var ignoreDiff = [];
            for (var i = 0; i <= result.length - 1; i++) {
                storedNum.push(result[i].number);
            }
            for (var i = oldMaxPage; i <= lastMaxBlock; i++) {
                lostNum.push(i);
            }
            ignoreDiff = dbm.arrDiff(lostNum, storedNum);

            console.log("Found missing block " + ignoreDiff.length);
            if (ignoreDiff.length > 0) {
                addIgnoreBlock (  config ,ignoreDiff , true ) ; 
            }
            return true;
        });
        var newMaxpage = oldMaxPage - config.pageSize;
        setTimeout(function() { recoveryIgnoreBlockToDB(config, lowBlock, newMaxpage, oldMaxPage); }, 2000);
      
    } else {
        //   console.log('Error: Web3 connection time out trying to get block ' + nextBlock + ' retrying connection now');
        //   recoveryIgnoreBlockToDB(config);
    }
}

/*

*/

var syncBlockToDB = function(config, blockData, flush) {
    var self = syncBlockToDB;
    if (!self.bulkOps) { // BulkOperations API give bigger size  write to file .node-xmlhttprequest
        self.bulkOps = []; // store more than one of data   
    }
    if (blockData && blockData.number >= 0) {
        self.bulkOps.push(new Blocker(blockData));
        console.log('\t-block #' + blockData.number.toString() + ' added ' + blockData.hash);
    }

    if (flush && self.bulkOps.length > 0 || self.bulkOps.length >= config.bulkSize) {
        var bulk = self.bulkOps;
        self.bulkOps = [];
        if (bulk.length == 0) return;

        Blocker.collection.insert(bulk, function(err, blocks) {
            if (typeof err !== 'undefined' && err) {
                if (err.code == 11000) {
                    if (!('quiet' in config && config.quiet === true)) {
                        console.log('Skip: Duplicate DB key : ' + err);
                    }
                } else {
                    console.log('Error: Aborted due to error on DB: ' + err);
                    process.exit(9);
                }
            } else {
                console.log('* ' + blocks.insertedCount + ' blocks successfully written.');
            }
        });
    }
}
/*

*/
var addIgnoreBlock = async function(config ,ignoreNumber, flush) {
    if (flush && typeof ignoreNumber != 'undefined') {
        for (var i = 0; i <= ignoreNumber.length - 1; i++) {
            web3.eth.getBlock(ignoreNumber[i], true, function(error, blockData) {
                if (error) {
                    console.log('Warning: error on getting block with hash/number: ' + ignoreNumber[i] + ': ' + error);
                } else if (blockData == null) {
                    console.log('Warning: null block data received from the block with hash/number: ' + ignoreNumber[i]);
                } else {
                      syncBlockToDB(config, blockData);
                      syncTransactionToDB(config, blockData);
                }
            });
            setTimeout(function(){},200);
        }
    }
}

/*

*/

var syncTransactionToDB = function(config, blockData, flush) {
    var self = syncTransactionToDB;
    if (!self.bulkOps) {
        self.bulkOps = [];
        self.blocks = 0;
    }
    if (blockData && blockData.transactions.length > 0) {
        for (d in blockData.transactions) {
            var txData = blockData.transactions[d];
            txData.timestamp = blockData.timestamp;
            txData.value = etherUnits.toEther(new BigNumber(txData.value), 'wei');
            self.bulkOps.push(txData);
        }
        console.log('\t- block #' + blockData.number.toString() + ': ' + blockData.transactions.length.toString() + ' transactions recorded.');
    }
    self.blocks++;

    if (flush && self.blocks > 0 || self.blocks >= config.bulkSize) {
        var bulk = self.bulkOps;
        self.bulkOps = [];
        self.blocks = 0;
        if (bulk.length == 0) return;
        Transaction.collection.insert(bulk, function(err, tx) {
            if (typeof err !== 'undefined' && err) {
                if (err.code == 11000) {
                    if (!('quiet' in config && config.quiet === true)) {
                        console.log('Skip: Duplicate transaction key ' + err);
                    }
                } else {
                    console.log('Error: Aborted due to error on Transaction: ' + err);
                    process.exit(9);
                }
            } else {
                console.log('* ' + tx.insertedCount + ' transactions successfully recorded.');
            }
        });
    }
}

/*

*/

var checkBlockDBExistsThenWrite = function(config, patchData, flush) {
    Blocker.find({ number: patchData.number }, function(err, b) {
        if (!b.length) {
            syncBlockToDB(config, patchData, flush);
            syncTransactionToDB(config, patchData, flush);
        } else if (!('quiet' in config && config.quiet === true)) {
            console.log('Block number: ' + patchData.number.toString() + ' already exists in DB.');
        }
    });
};

/*

*/



var MissingBlockReEntry = function(config, startBlock, endBlock) {
    if (!web3 || !web3.isConnected()) {
        console.log('Error: Web3 is not connected. Retrying connection shortly...');
        setTimeout(function() { MissingBlockReEntry(SyncOptions); }, 3000);
        return;
    }

    if (typeof startBlock === 'undefined' || typeof endBlock === 'undefined') {
        // get the last saved block
        var MaxBlockInDB = Blocker.find({}, "number").lean(true).sort('-number').limit(1); // Find DB was wroted max number 

        //console.log(util.inspect(MaxBlockInDB, false, null)) ; 
        MaxBlockInDB.exec(function(err, docs) {
            console.log("====== Sort Blocknumber from DB ====");
            console.log(docs);
            if (err || !docs || docs.length < 1) {
                // no blocks found. terminate MissingBlockReEntry()
                console.log('No need to patch blocks.');
                return;
            }
            var latestBlockNumberInDB = docs[0].number + 1;
            var currentBlock = web3.eth.blockNumber;
            MissingBlockReEntry(config, latestBlockNumberInDB, currentBlock); // Current block write by other function
        });
        return;
    }

    var missingBlocks = endBlock - startBlock + 1;
    if (missingBlocks > 0) {
        console.log('Patching from #' + startBlock + ' to #' + endBlock + " Total  Missing : " + (missingBlocks));
        var patchBlock = startBlock;
        var count = 0;
        while (count < (missingBlocks + 10) && patchBlock <= endBlock) {
            if (!('quiet' in config && config.quiet === true)) {
                console.log('Patching Block: ' + patchBlock)
            }
            web3.eth.getBlock(patchBlock, true, function(error, patchData) {
                if (error) {
                    console.log('Warning: error on getting block with hash/number: ' + patchBlock + ': ' + error);
                } else if (patchData == null) {
                    console.log('Warning: null block data received from the block with hash/number: ' + patchBlock);
                } else {
                    checkBlockDBExistsThenWrite(config, patchData)
                }
            });
            patchBlock++;
            console.log("patching missing block count : " + count)
            count++;
        }
        // flush
        syncBlockToDB(config, null, true);
        syncTransactionToDB(config, null, true);
        setTimeout(function() { MissingBlockReEntry(config, patchBlock, endBlock); }, 1000);
    } else {
        // flush
        syncBlockToDB(SyncOptions, null, true);
        syncTransactionToDB(SyncOptions, null, true);
        console.log('*** Block return Missing block Completed ***');
    }
}

/*
  Loop 
*/
const SyncOptions = mainConfig.sync;
console.log(" == Blocksync starting  == ");
console.log("Sync Options :  recoveryMissingBlock { " + SyncOptions.recoveryMissingBlock + " }  syncAll { " + SyncOptions.syncAll + " }");

if (SyncOptions.monitor === true) {
    monitorNewBlock(SyncOptions); // loop 
}

if (SyncOptions.recoveryIgnoreBlock === true) {
    recoveryIgnoreBlockToDB(SyncOptions);
}

if (SyncOptions.recoveryMissingBlock === true) {
    console.log('Checking for missing blocks');
    MissingBlockReEntry(SyncOptions);
}

if (SyncOptions.syncAll === true) {
    console.log('[Starting Full Sync]');
    fullSyncChain(SyncOptions);
}

