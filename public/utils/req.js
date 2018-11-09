/**
 * Created by mayn on 2018/11/1.
 */

let req = {
      success:(data)=>{
            return {
                errorInf:{
                    code:0,
                    message:""
                },
                data:data
            }
      },
      error:(code,message)=>{
          return {
              errorInf:{
                  code:code,
                  message:message
              },
              data:""
          }
      }
};

module.exports=req;