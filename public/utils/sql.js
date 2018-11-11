
const mysql = require('mysql');
const async = require('async');

const db_config={
    host:"120.77.173.91",
    user:"root",
    password:"crcsss2018",
    port:"3306",
    database:"users"
};

let sql = {

    init:()=>{
        let conn=mysql.createConnection(db_config);

        conn.connect(function(err){
            if(err){
                console.log(`mysql连接失败: ${err}!`);
            }else{
                console.log("mysql连接成功!");
            }
        });

        return conn;
    },
    init2:(id)=>{
        const db_config2={
            host:"120.77.173.91",
            user:"user"+id,
            password:"user"+id,
            port:"3306",
            database:"userdb"+id,
            multipleStatements: true
        };
        let conn2=mysql.createConnection(db_config2);

        conn2.connect(function(err){
            if(err){
                console.log(`mysql连接失败[init2]: ${err}!`);
            }else{
                console.log("mysql连接成功[init2]!");
            }
        });

        return conn2;
    },

    createdsql:(data)=>{
        let conn=mysql.createConnection(db_config);
        return new Promise((res,rej)=>{
            let tarTable = data.tarTable;

            let tsql = `Insert into ${tarTable}(devId,stateId,value,createAt) `;

            let stCount = parseInt((new Date().getTime()/1000/360/60));

            let json = JSON.parse(data.sqlContent);

            let devsJson=json.content;


            if(devsJson=="") return "";

            let Arr = [0,1,2,3];

            async.map(Arr, function(i, callback) {

                let sname = "seg"+(stCount-i);

                let isEixst = `SELECT count(table_name) isEixst FROM information_schema.TABLES WHERE table_name ='${sname}' and TABLE_SCHEMA='hstate2018';`;

                conn.query(isEixst,function (err,res) {
                    //if(err) throw err;
                     if(res[0]['isEixst']!=0){
                        let bsql = `select devId,stateId,value,createAt from hstate2018.${sname} where `;


                        let tarr = [];
                        for(let k in devsJson){
                            let v =devsJson[k];
                            if(!v.selstate || v.selstate.length==0){
                                continue;
                            }
                            let ifs = "";
                            for(let k1 in v.selstate){
                                let v1 =v.selstate[k1];
                                ifs+=`(hstate2018.${sname}.devId=${v.eid} and `;
                                if(k1==v.selstate.length-1)
                                    ifs+=`hstate2018.${sname}.stateId=${v1})`;
                                else
                                    ifs+=`hstate2018.${sname}.stateId=${v1}) or `;
                            }
                            tarr.push(ifs);
                        }

                        bsql+=tarr.join(' or ');
                        callback(err,bsql);

                    }
                });

            }, function(err,results) {

                if(err) rej(err);
                //console.log(results);
                tsql+=results.join(" UNION ");

                res(tsql)
            });


        });

    },

    getTimeStamp:(cycle,execTime)=>{
        let m = new Date(execTime).getMinutes();
        let s = new Date(execTime).getSeconds();
        let h = new Date(execTime).getHours();
        let d = new Date(execTime).getDay();
        if(cycle==3600){
            return `${s} ${m} * * * *`;
        }else if(cycle==86400){
            return `${s} ${m} ${h} * * *`;
        }else if(cycle==604800){
            return `${s} ${m} ${h} * * ${d}`;
        }

    },

    getSortArr:(nodeIds)=>{
        //查询节点
        let sortArr = [];
        let getSort = function (index)
        {

            if(nodeIds[index].nextId=="" || nodeIds[index].nextId==null || nodeIds[index].nextId==undefined){
                return
            }

            for(let i in nodeIds){
                if(nodeIds[index].nextId==nodeIds[i].nodeId){
                    sortArr.push(nodeIds[i]);
                    getSort(i);
                }
            }
        };
        for(let i=0;i<nodeIds.length;i++){
            let isStart = true;
            for(let k=i+1;k<nodeIds.length;k++){
                if(nodeIds[k].nextId==nodeIds[i].nodeId){
                    isStart=false;
                    break;
                }
            }
            if(isStart){
                //console.log("start:"+nodeIds[i].nodeId);
                sortArr.push(nodeIds[i]);
                getSort(i);
                break;
            }
        }
        return sortArr;

    },

    execSortTask:function(arr,uuid){
        let conn2=this.init2(uuid);
        return new Promise((res,rej)=>{

            async.mapSeries(arr, (item, callback)=> {

                if(item.type==0){
                    //同步

                    this.createdsql(item).then(sqlresult=>{

                        conn2.query(sqlresult,function (err,rs) {
                            console.log(2)
                            if(err){
                                //TODO 记录日志
                                callback(err,rs);
                            }else{
                                //TODO
                                callback(err,rs);
                            }


                        });
                    });


                }else{
                    //脚本
                    let sqlContent = JSON.parse(item.sqlContent).content;


                        conn2.query(sqlContent,function (err,rs) {
                            if(err){
                                //TODO 记录日志
                            }else{
                                //TODO

                            }
                            console.log(1)
                            callback(err,rs);
                        });

                }
            },
            function(err,results) {
                conn2.end();
                if(err) rej(err);
                console.log('DONE')
                //console.log(results);
                res(results)
            });
        });

    }


};

module.exports=sql;