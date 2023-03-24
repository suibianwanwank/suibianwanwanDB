package com.ccsu.server;

import com.ccsu.statement.*;
import com.ccsu.tb.TableManager;

public class Executor {
    private long xid;

    TableManager tbm;

    public Executor(long xid, TableManager tbm) {
        this.xid = xid;
        this.tbm = tbm;
    }

    public byte[] execute(byte[] sql) throws Exception {
        Exception e1 = null;
        Object stat = Parser.parse(sql);
        byte[] bytes=null;
        try{

            if(stat instanceof Begin){
                this.xid= tbm.begin((Begin)stat);
                bytes="begin".getBytes();
            } else if (stat instanceof Commit) {
                tbm.commit(xid);
                this.xid=0;
                bytes="commit".getBytes();
            } else if (stat instanceof RollBack) {
                tbm.rollback(xid);
                xid=0;
            } else {
                bytes=execute2(stat);
            }
        }catch (Exception e){
            e1=e;
            throw e;
        }finally {
            if(e1==null){
                return bytes;

            }else{
                throw  e1;
//                return Result.packageMessage("发生错误:"+e1.getMessage());
            }
        }


    }

    private byte[] execute2(Object stat) throws Exception {
        boolean inTransaction=true;
        Exception e1 = null;
        if(xid == 0) {
            inTransaction=false;
            xid = tbm.begin(new Begin());
        }
        byte[] res = null;
        try{
            if(stat instanceof Show){
                res = tbm.show(xid);
            } else if (stat instanceof Create) {
                res=tbm.create(xid,(Create) stat);
            } else if (stat instanceof Insert) {
                res=tbm.insert(xid,(Insert) stat);
            } else if (stat instanceof Select) {
                res=tbm.read(xid, (Select) stat);
            } else if (stat instanceof Update) {
                res=tbm.update(xid,(Update) stat);
            } else if (stat instanceof Delete) {
                res=tbm.delete(xid,(Delete) stat);
            }
        }catch (Exception e){
            e1=e;
            throw e1;
        }finally {
            if(!inTransaction){
                if(e1==null){
                    tbm.commit(xid);
                }else{

                    tbm.rollback(xid);

                }
                xid=0;
            }

        }


        return res;
    }
}
