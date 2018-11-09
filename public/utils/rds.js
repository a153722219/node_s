const redis = require('redis'),
    RDS_PORT = 6379,                //端口号
    RDS_HOST = '120.77.173.91',    //服务器IP
    RDS_PWD = 'YuanHuiQiang0857Hust',     //密码
    RDS_OPTS = {};                //设置项
let client = redis.createClient(RDS_PORT,RDS_HOST,RDS_OPTS);

client.on('connect',function(){
    //client.set('author', 'Wilson',redis.print);
    //client.get('author', redis.print);
    console.log('connect');
});
client.on('ready',function(err){
    console.log('ready');
});

client.auth(RDS_PWD,function(){
    console.log('通过认证');
});

let rds = {
    init:()=>{
        client.select(1,function(err){
            if(err){
                console.log('err');
            }else{
                console.log('select db1');
            }
        });

        return client;
    }

};

module.exports=rds;