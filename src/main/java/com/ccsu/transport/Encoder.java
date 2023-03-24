package com.ccsu.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public class Encoder {
    public static byte[] encode(Object o){
        if(o instanceof ByteBuf buf){

            return ByteBufUtil.getBytes(buf);

        }
        return new byte[0];
    }

    public static ByteBuf decode(byte[] o){

        return Unpooled.wrappedBuffer(o);
    }
}
