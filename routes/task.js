const express = require('express');
const router = express.Router();
const reqs = require("../public/utils/req");
const schedule = require('node-schedule');

const sqls = require('../public/utils/sql');
const rdss = require('../public/utils/rds');

let taskArr = {};

let conn = sqls.init();
let rds = rdss.init();
//数据库初始化


/* GET users listing. */
router.use(function (req, res, next) {

    if((req.method=="GET" && !req.query.cookie) || (req.method=="POST" && !req.body.cookie)){
        return res.status(401).json(reqs.error(401,"token validate failed!"));
    }
        rds.hget("tooken2UsrIId",req.query.cookie,function (err,uuid) {

        if(uuid==null){
            return res.status(401).json(reqs.error(401,"token validate failed!"));
        }
        req.uuid = uuid;
        req.conn2 = sqls.init2(uuid);
        next();
    });


});

router.get('/', function(req, res, next) {
    return res.status(200).json(reqs.success(taskArr));
});



router.get('/add', function(req, res, next) {
    //创建一个任务

    let taskId = req.query.tid;

    if(!taskId){
        return res.status(500).json(reqs.error(500,"tid or cycle error!"))
    }

    if(taskArr[taskId] && taskArr[taskId].status=="running"){
        return res.status(500).json(reqs.error(500,"task is already running!"))
    }

        //mysql参数查询
        let sql  = `select * from datadb.worktask where id=${taskId};`;

            conn.query(sql,function(err,result){

            if(err){
                    req.conn2.end();
                    return res.status(500).json(reqs.error(500,`interval error:${err.message}`))
            }else{

                if(result.length==0){
                    req.conn2.end();
                    return res.status(500).json(reqs.error(500,`任务不存在!`))
                }

                let task = result[0];

                let nodeIds = JSON.parse(task.nodeIds);

                //查询脚本任务内容
                let sql = `select * from datadb.tasknodes where nodeId='${nodeIds[0]}'`;
                conn.query(sql,function (err,result2) {
                    if(result2.length==0){
                        req.conn2.end();
                        return res.status(500).json(reqs.error(500,`任务不存在!`))
                    }

                    let sqlContent = JSON.parse(result2[0].sqlContent).content;

                    //判断任务类型 0-同步 1-流任务 2-脚本任务
                    if(task.type==1){
                        //流任务 取出节点集合
                        let nodeIds = JSON.parse(task.nodeIds);

                            if(nodeIds.length!=0){
                                let sql  = `select * from datadb.tasknodes where nodeId in ('${nodeIds.join("','")}')`;

                                conn.query(sql,function (err,result) {

                                    if(err){
                                        req.conn2.end();
                                        return res.status(500).json(reqs.error(err.message));
                                    }

                                    let sortArr = sqls.getSortArr(result); //排序后的任务数组
                                    if(task.execTime==0){
                                        //立即执行
                                        sqls.execSortTask(sortArr,req.uuid).then(r=>{
                                            req.conn2.end();
                                            return res.status(200).json(reqs.success(r));
                                        });

                                    }else{
                                        //定时执行
                                        let cycle = task.cycle;
                                        let execTime = task.execTime;
                                        let r_c = sqls.getTimeStamp(cycle,execTime);

                                        if(taskArr[task.id] && taskArr[task.id].status=="running"){
                                            taskArr[task.id].t.cancel();
                                        }


                                        let t = schedule.scheduleJob(r_c, function(){

                                            sqls.execSortTask(sortArr,req.uuid).then(r=>{
                                                //TODO 日志写入

                                            });

                                            console.log("task  "+task.id+' is running:' + new Date());
                                        });

                                        taskArr[task.id] = {
                                            t:t,
                                            taskId:task.id,
                                            cycle:cycle,
                                            execTime:execTime,
                                            status:"running",
                                            uuid:req.uuid,
                                            type:task.type,
                                            nodeId:nodeIds[0],
                                            tname:task.tname
                                        };
                                        req.conn2.end();
                                        return res.status(200).json(reqs.success(""))

                                    }

                                })
                            }





                    }else if(task.type==2){

                            if(task.execTime==0){
                                req.conn2.query(sqlContent,function (err,rs) {
                                    if(err){
                                        req.conn2.end();
                                        return res.status(500).json(reqs.error(500,`执行错误:${err.message}`))
                                    }else{
                                        //TODO 记录日志
                                        req.conn2.end();
                                        return res.status(200).json(reqs.success(""))
                                    }
                                });
                            }else{
                                let cycle = task.cycle;
                                let execTime = task.execTime;
                                let r_c = sqls.getTimeStamp(cycle,execTime);

                                if(taskArr[task.id] && taskArr[task.id].status=="running"){
                                    taskArr[task.id].t.cancel();
                                }

                                let t = schedule.scheduleJob(r_c, function(){
                                    let tmpConn = sqls.init2(req.uuid);
                                    tmpConn.query(sqlContent,function (err,result) {
                                        if(err){
                                            //TODO 记录日志
                                            tmpConn.end();
                                        }else if(result){
                                            //TODO 记录日志
                                            tmpConn.end();
                                        }else{

                                        }
                                    });

                                    console.log("task  "+task.id+' is running:' + new Date());
                                });

                                //添加到队列

                                taskArr[task.id] = {
                                    t:t,
                                    taskId:task.id,
                                    cycle:cycle,
                                    execTime:execTime,
                                    status:"running",
                                    uuid:req.uuid,
                                    type:task.type,
                                    nodeId:nodeIds,
                                    tname:task.tname
                                };
                                req.conn2.end();
                                return res.status(200).json(reqs.success(""))

                            }

                            req.conn2.end();
                            return res.status(200).json(reqs.success(result2))


                    }else{

                        //同步任务 --生成sql语句

                        if(task.execTime==0){
                            //立即执行
                            sqls.createdsql(result2[0]).then(sqlresult=>{
                                //console.log(res);
                                req.conn2.query(sqlresult,function (err,result) {
                                    if(err){
                                        //TODO 记录日志

                                        req.conn2.end();
                                        return res.status(500).json(reqs.success("sync task exec fail"));

                                    }else if(result){
                                        //TODO 记录日志
                                        req.conn2.end();
                                        return res.status(200).json(reqs.success("sync task exec success"));
                                    }
                                });
                            });
                        }else{
                            //获取SQL
                            sqls.createdsql(result2[0]).then(sqlresult=>{

                                let cycle = task.cycle;
                                let execTime = task.execTime;
                                let r_c = sqls.getTimeStamp(cycle,execTime);

                                if(taskArr[task.id] && taskArr[task.id].status=="running"){
                                    taskArr[task.id].t.cancel();
                                }

                                let t = schedule.scheduleJob(r_c, function(){
                                    let tmpConn = sqls.init2(req.uuid);
                                    tmpConn.query(sqlresult,function (err,result) {
                                        if(err){
                                            //TODO 记录日志
                                            tmpConn.end();
                                            return res.status(500).json(reqs.success("sync task exec fail"));
                                        }else if(result){
                                            //TODO 记录日志
                                            tmpConn.end();

                                            //添加到队列

                                            taskArr[task.id] = {
                                                t:t,
                                                taskId:task.id,
                                                cycle:cycle,
                                                execTime:execTime,
                                                status:"running",
                                                uuid:req.uuid,
                                                type:task.type,
                                                nodeId:nodeIds[0],
                                                tname:task.tname
                                            };

                                            req.conn2.end();
                                            return res.status(200).json(reqs.success("sync task add success"));

                                        }
                                    });

                                    console.log("async task  "+task.id+' is running:' + new Date());
                                });

                            });



                        }

                    }

                })
            }

    });
    // let t = schedule.scheduleJob('* * * * * *', function(){
    //     console.log("task"+taskId+' is running:' + new Date());
    // });
    //
    // //加到队列中
    //
    // taskArr[taskId] = {
    //     t:t,
    //     taskId:taskId,
    //     cycle:cycle,
    //     status:"running",
    //     cookie:req.query.cookie
    // };
});

router.get('/remove', function(req, res, next) {
    //删除一个任务
    let taskId = req.query.tid;

    if(!taskId){
        return res.status(500).json(reqs.error(500,"tid error!"))
    }

    if(!taskArr[taskId]){
        return res.status(500).json(reqs.error(500,"task is not exist!"))
    }

    if(taskArr[taskId].status=="stop"){
        return res.status(500).json(reqs.error(500,"task is not running!"))
    }

    taskArr[taskId].t.cancel();

    taskArr[taskId].status="stop";

    return res.status(200).json(reqs.success("remove success!"))
});




module.exports = router;
