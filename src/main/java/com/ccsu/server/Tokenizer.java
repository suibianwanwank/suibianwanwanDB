package com.ccsu.server;
import com.ccsu.common.Error;
public class Tokenizer {
    private byte[] stat;
    private int pos;
    private String currentToken;
    private boolean flushToken;
    private Exception err;

    public Tokenizer(byte[] statement) {

        this.stat=statement;
        this.flushToken=true;
        this.pos=0;
    }




    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    public String peek() throws Exception{
        if(err != null) {
            throw err;
        }
        if(flushToken){
            String token=null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }

            currentToken=token;
            flushToken=false;
        }
        return currentToken;
    }

    public void pop(){
        flushToken=true;
    }

    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    private Byte peekByte(){
        if(pos==stat.length)return null;
        return  stat[pos];
    }
    private String nextMetaState() throws Exception {
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                return "";
            }
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }
        byte b = peekByte();
        if(isSymbol(b)) {
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    private String nextTokenState() throws Exception{
        StringBuilder sb=new StringBuilder();
        while(true){
            Byte b = peekByte();
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }

    }

    private void popByte() {
        pos++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    private boolean isBlank(Byte b) {
        return (b==' '||b=='\n'||b=='\t');
    }

    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')' || b == ';');
    }

    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }


}
