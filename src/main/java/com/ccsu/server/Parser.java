package com.ccsu.server;

import com.ccsu.common.Error;
import com.ccsu.statement.*;

import java.util.ArrayList;
import java.util.List;

public class Parser {

    public static Object parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            stat = switch (token) {
                case "begin" -> parseBegin(tokenizer);
                case "commit" ->parseCommit(tokenizer);
                case "create" -> parseCreate(tokenizer);
                case "insert" -> parseInsert(tokenizer);
                case "select" -> parseSelect(tokenizer);
                case "update" -> parseUpdate(tokenizer);
                case "delete" -> parseDelete(tokenizer);
                case "rollback" ->parseRollback(tokenizer);
                case "show" ->parseShow(tokenizer);
                default -> throw Error.InvalidCommandException;
            };
        } catch(Exception e) {
            statErr = e;
        }
        try {
            tokenizer.pop();
            String next = tokenizer.peek();
            if(!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch(Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if(statErr != null) {
            throw statErr;
        }
        return stat;
    }

    private static Show parseShow(Tokenizer tokenizer) {
        return new Show();
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception{
        Update update=new Update();
        List<String> fileName=new ArrayList<>();
        List<String> value=new ArrayList<>();
        update.tableName=tokenizer.peek();
        tokenizer.pop();

        String set = tokenizer.peek();
        tokenizer.pop();
        if(!set.equals("set")){
            throw Error.InvalidCommandException;
        }
        int isEnd=0;
        while(isEnd==0){
            String peek = tokenizer.peek();

            switch (peek) {
                case "where":
                    tokenizer.pop();
                    update.where = parseWhere2(tokenizer);
                    isEnd=1;
                    break;
                case ",":
                    tokenizer.pop();
                    break;
                default:
                    fileName.add(tokenizer.peek());
                    tokenizer.pop();
                    tokenizer.peek();
                    tokenizer.pop();
                    value.add(tokenizer.peek());
                    tokenizer.pop();
            }
        }
        update.fieldName=fileName.toArray(new String[0]);
        update.value=value.toArray(new String[0]);


        return update;
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        tokenizer.peek();
        tokenizer.pop();
        return new Commit();
    }

    private static RollBack parseRollback(Tokenizer tokenizer) throws Exception {
        tokenizer.peek();
        tokenizer.pop();
        return new RollBack();
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete=new Delete();
        if(!tokenizer.peek().equals("from")){
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName=tokenizer.peek();
        tokenizer.pop();
        if(isName(tableName))
            delete.tableName=tableName;
        if(!"where".equals(tokenizer.peek())){
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        delete.where=parseWhere2(tokenizer);


        return delete;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {

        Select select=new Select();
        Where2 where=null;
        List<String> fieldsName=new ArrayList<>();
        int a=0;
        boolean flag=true;
        while (flag){


            String peek = tokenizer.peek();
            tokenizer.pop();
            switch (peek){
                case "from":
                    a=1;
                    select.tableName=tokenizer.peek();
                    tokenizer.pop();

                    break;
                case "*":break;
                case "where":
                    where = parseWhere2(tokenizer);
                    flag=false;
                    break;
                case ";","":
                    flag=false;
                    break;
                case ",":
                    break;
                default:
                    fieldsName.add(peek);
                    break;


            }

        }

        select.fields=fieldsName.toArray(new String[0]);
        select.where=where;


        return select;
    }
    @Deprecated
    private static Where parseWhere(Tokenizer tokenizer) throws Exception{
        Where where=new Where();

//        List<SingleExpression> singleExpressions=new ArrayList<>();
//        List<String> logicOp=new ArrayList<>();


            SingleExpression singleExpression1=new SingleExpression();

            singleExpression1.field=tokenizer.peek();
            tokenizer.pop();

            singleExpression1.compareOp=tokenizer.peek();
            tokenizer.pop();

            singleExpression1.value=tokenizer.peek();
            tokenizer.pop();

        where.singleExp1=singleExpression1;
            String op = tokenizer.peek();
            tokenizer.pop();
            if(!op.equals(";")) {

                where.logicOp = op;

                SingleExpression singleExpression2 = new SingleExpression();

                singleExpression2.field = tokenizer.peek();
                tokenizer.pop();

                singleExpression2.compareOp = tokenizer.peek();
                tokenizer.pop();

                singleExpression2.value = tokenizer.peek();
                tokenizer.pop();

                op=tokenizer.peek();
                where.singleExp2=singleExpression2;

            }

        if(!op.equals(";")){
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        return where;
    }


    public static Where2 parseWhere2(Tokenizer tokenizer) throws Exception{
        Where2 where=new Where2();
        List<Object> singleExpressions=new ArrayList<>();
        List<String>logicalOps=new ArrayList<>();
        while(true){
            String peek = tokenizer.peek();
            tokenizer.pop();
            if(peek.equals("(")){
                //进入下一个函数
                singleExpressions.add(parseWhere2(tokenizer));
            } else if (peek.equals(")")) {
                where.singleExpression=singleExpressions;
                where.logicOp=logicalOps;
                return where;

                //计算完成结果
            }else if (isLogicalOperator(peek)) {
                logicalOps.add(peek);
            }else if(isName(peek)) {
                SingleExpression singleExpression=new SingleExpression();
                singleExpression.field=peek;

                singleExpression.compareOp=tokenizer.peek();
                tokenizer.pop();
                singleExpression.value=tokenizer.peek();
                tokenizer.pop();
                singleExpressions.add(singleExpression);
            } else if (isLogicalOperator(peek)) {
                logicalOps.add(peek);
            }else if (peek.equals(";")){
                where.singleExpression=singleExpressions;
                where.logicOp=logicalOps;
                return where;
            }else{
                throw Error.InvalidCommandException;
            }


        }



    }

    private static boolean isLogicalOperator(String operator) {
        return switch (operator) {
            case "and", "or" -> true;
            default -> false;
        };
    }

    enum CreateState{
        INIT,
        INDEX,
        KEYWORD
    }

    public static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())){
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();
        Create create=new Create();
        String name=tokenizer.peek();
        tokenizer.pop();
        if(!isName(name)){
            throw Error.InvalidCommandException;
        }
        create.tableName=name;
        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        List<String> fIndex = new ArrayList<>();
        List<String> fKey = new ArrayList<>();

        if(!tokenizer.peek().equals("(")){
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        //区分状态
        //初始状态，介入关键字状态。
        CreateState state=CreateState.INIT;
        String typeName;
        int isEnd=1;
        while(isEnd!=0){
            String peek = tokenizer.peek();
            tokenizer.pop();

            switch (state){
                case INIT ->{
                    if(isType(peek)){
                        fTypes.add(peek);
                        state=CreateState.KEYWORD;
                    } else if (peek.equals("index")) {
                        state=CreateState.INDEX;
                    } else if(isName(peek)){
                        fNames.add(peek);
                        typeName=name;
                    }else {
                        throw Error.InvalidCommandException;
                    }
                }
                case KEYWORD -> {
                    if(peek.equals(",")){
                        state=CreateState.INIT;
                    }else if(peek.equals(";")){
                        isEnd=0;
                    }else if(isKeyWord(peek)){
                        fKey.add(peek);
                    }
                }
                case INDEX ->{
                    if(peek.equals(",")){
                        state=CreateState.INIT;
                    }else if(isName(peek)){
                        if(fNames.contains(peek))fIndex.add(peek);
                        else throw Error.InvalidCommandException;
                    } else if (peek.equals(";")) {
                        isEnd=0;
                    }
                }

            }



//            tokenizer.pop();
//            String field=tokenizer.peek();
//            if(")".equals(field)) {
//                break;
//            }
//            if(!isName(field)) {
//                throw Error.InvalidCommandException;
//            }
//            tokenizer.pop();
//            String fieldType = tokenizer.peek();
//            if(!isType(fieldType)) {
//                throw Error.InvalidCommandException;
//            }
//            fNames.add(field);
//            fTypes.add(fieldType);
//
//            tokenizer.pop();
//            String next = tokenizer.peek();
//
//            if("index".equals(next)){
//                fIndex.add(field);
//                tokenizer.pop();
//                next=tokenizer.peek();
//            }
//            if(",".equals(next)){
//                continue;
//            }else if(")".equals(next)){
//
//            }else if(";".equals(next)){
//                break;
//            } else{
//               throw Error.InvalidCommandException;
//            }

        }
        create.fieldName = fNames.toArray(new String[0]);
        create.fieldType = fTypes.toArray(new String[0]);
        create.index = fIndex.toArray(new String[0]);




        return create;

    }

    private static boolean isKeyWord(String s) {
        return s.equals("not")||s.equals("null")||s.equals("key")||s.equals("primary");
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        Begin begin=new Begin();

        String peek = tokenizer.peek();
        tokenizer.pop();
        if(!peek.equals(";")){
            switch (peek){
                case "read":
                    String level = tokenizer.peek();
                    tokenizer.pop();
                    switch (level) {
                        case "uncommitted" -> begin.isolationLevel = Begin.READ_UNCOMMITTED;
                        case "committed" -> begin.isolationLevel = Begin.READ_COMMITTED;
                        default -> throw Error.InvalidCommandException;
                    }
                    if(tokenizer.peek().equals(";")){
                        tokenizer.pop();
                        break;
                    }else {
                        throw Error.InvalidCommandException;
                    }
                case "repeatable" :{
                    String read = tokenizer.peek();
                    tokenizer.pop();
                    if(read.equals("read")){
                        begin.isolationLevel=Begin.REPEATABLE_READ;
                    }
                    tokenizer.peek();
                    tokenizer.pop();
                    break;
                }


            }

        }else{
            begin.isolationLevel=Begin.READ_COMMITTED;
        }
        return begin;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();
        String into = tokenizer.peek();
        tokenizer.pop();
        if(!"into".equals(into)){
            throw Error.InvalidCommandException;
        }

        String tableName = tokenizer.peek();
        tokenizer.pop();
        if(!isName(tableName)){
            System.out.println("表不存在");
            throw Error.InvalidCommandException;
        }
        insert.tableName = tableName;

        List<String> fieldName=new ArrayList<>();
        List<String> values = new ArrayList<>();
        int parseState=0;
        int isEnd=0;
        while(isEnd==0){
            tokenizer.pop();
            String peek = tokenizer.peek();

            switch (peek){
                case ";":isEnd=1; break;
                case "":throw Error.InvalidCommandException;
                case "(":
                    parseState++;
                    break;
                case ")", "values",",":
                    break;
                default:
                    if(parseState==1&&isName(peek)){
                        fieldName.add(peek);

                    } else if (parseState==2) {
                        //TODO 类型校验
                        values.add(peek);
                    }else{
                        throw Error.InvalidCommandException;
                    }

            }


        }

        insert.values = values.toArray(new String[0]);
        insert.filedName=fieldName.toArray(new String[0]);
        //        insert.fieldType = fTypes.toArray(new String[fTypes.size()]);





        return insert;

    }





    private static boolean isName(String name) {
        return name.length() != 1 && Tokenizer.isAlphaBeta(name.getBytes()[0]);
    }

    private static boolean isType(String tp) {
        return ("int".equals(tp) || "long".equals(tp) ||
                "string".equals(tp));
    }

}
