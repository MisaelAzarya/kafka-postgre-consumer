const pg = require('pg');
const kafka = require('kafka-node');
const config = require('./config');

var flag = 0;

function ins(pgClient, table, data){
    return new Promise((resolve, reject) => {
        var qry = "INSERT INTO "+table+" VALUES "+data;
        pgClient.query(qry, function(err, results) {
            if (err) {
            console.error(err);
            return reject(err);
            }
            resolve(results);
        })
    })
}
  
function updt(pgClient, table, data, param_key, data_key){
    return new Promise((resolve, reject) => {
        pgClient.query("UPDATE "+table+" SET "+data+" WHERE "+param_key+" = '"+data_key+"'", function(err, results) {
            if (err) {
            console.error(err);
            return reject(err);
            }
            resolve(results);
        })
    })
}
  
function dlt(pgClient, table, param_key, data_key){
    return new Promise((resolve, reject) => {
        pgClient.query("DELETE FROM "+table+" WHERE "+param_key+" = '"+data_key+"'", function(err, results) {
            if (err) {
            console.error(err);
            return reject(err);
            }
            resolve(results);
        })
    })
}


function typeInsert(logs,table,clientpg){
    var full_data = [];
    var full_data2 = [];
    var new_id = [];
    for(var x=0;x<logs.affectedRows.length;x++){
        var data = [];
        var value = Object.values(logs.affectedRows[x].after);
        var keys = Object.keys(logs.affectedRows[x].after);
        // untuk yang superstore
        for(var i=0;i<value.length;i++){
        data.push("'" + value[i] + "'");
        }
        new_id.push(value[0]);
        full_data.push('(' + data.join(', ') + ')');
    }
    ins(clientpg,table,full_data);
    console.log('ID = '+new_id+' in superstore Inserted');
}

function typeUpdate(logs,table,clientpg){
    for(var x=0;x<logs.affectedRows.length;x++){
        var data = [];
        var value = Object.values(logs.affectedRows[x].after);
        var keys = Object.keys(logs.affectedRows[x].after);
        var param_key = keys[0];
        var data_key = value[0];
        // untuk yang superstore
        for(var i=1;i<value.length;i++){
            data.push(keys[i] + " = '" + value[i] + "'");
        }
        updt(clientpg,table,data,param_key,data_key);
        console.log('ID = '+data_key+' in superstore Updated');
    }
}

function typeDelete(logs,table,clientpg){
    for(var x=0;x<logs.affectedRows.length;x++){
        var value = Object.values(logs.affectedRows[x].before);
        var keys = Object.keys(logs.affectedRows[x].before);
        var param_key = keys[0];
        var data_key = value[0];
        // untuk yg superstore
        dlt(clientpg,table,param_key,data_key);
        console.log('ID = '+data_key+' in superstore Deleted');
    }
}

function kafkaListen(clientpg){
    console.log("kafka consumer is booting up");
    const Consumer = kafka.Consumer;
    const client = new kafka.KafkaClient(config.kafka_server);
    let consumer = new Consumer(
        client,
        [{ topic: config.kafka_topic, partition: 0 }],
        {
        autoCommit: true,
        fetchMaxWaitMs: 1000,
        fetchMaxBytes: 1024 * 1024,
        encoding: 'utf8',
        fromOffset: false
        }
    );
    consumer.on('message', async function(message) {
        if(flag==0)consumer.pause();
        if(flag==1){
            var logs = JSON.parse(message.value);
            console.log(logs);
            var table = "superstore";
            var table_log = "superstore_log";
      
            if(logs.type=='INSERT'){
                typeInsert(logs,table,clientpg);
            }
            else if(logs.type=='UPDATE'){ 
                typeUpdate(logs,table,clientpg);
            }else{ // untuk delete
                typeDelete(logs,table,clientpg);
            }
            // untuk insert log
            var data_for_log = "(DEFAULT,DEFAULT,"+logs.affectedRows.length+",'"+logs.type+"')";
            ins(clientpg, table_log,data_for_log);
            console.log('Superstore Log Inserted');
        }
    })
    consumer.on('error', function(err) {
        console.log('error', err);
    });
}

function handleDisconnect() {
    //syntaxnya username:password@server:port/database_name
    const pgConString = "postgres://postgres:1234@localhost:5432/staging_ingestion";
    var clientpg = new pg.Client(pgConString);

    
    clientpg.connect(function(err) {              
        if(err) { 
            flag=0;                                    
            console.log('error when connecting to db:', err);
            setTimeout(handleDisconnect, 2000); 
        } 
        else{
            console.log("connect");
            flag = 1;
            kafkaListen(clientpg);
        }                                    
    });
    
    clientpg.on('error', function(err) {
        flag = 0;
        if(err.code === "57P01"){
            console.log('db error', err);
        }
        else if(err == "Error: Connection terminated unexpectedly"){
            console.log('db error', err);
            flag = 1;
            handleDisconnect();
        }
        else if(err.code === 'PROTOCOL_CONNECTION_LOST' || err.code === "ECONNRESET") { 
            if(flag==1){
                handleDisconnect();
                flag = 0;
            }                    
        } else {                                     
            throw err;                                  
        }
    });                                     
}
handleDisconnect();