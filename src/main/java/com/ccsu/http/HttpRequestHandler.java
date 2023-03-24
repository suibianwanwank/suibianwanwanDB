package com.ccsu.http;

import com.ccsu.server.Executor;
import com.ccsu.tb.TableManager;
import com.ccsu.transport.Sql;
import com.google.gson.Gson;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.util.CharsetUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.netty.handler.codec.http.HttpHeaderNames.COOKIE;
import static io.netty.handler.codec.http.HttpHeaderNames.SET_COOKIE;
import static io.netty.handler.codec.http.HttpUtil.is100ContinueExpected;

public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private TableManager tbm;



    //TODO 手写一个定时map 否则内存占用过大
    private final static Map<String,Session> sessions=new ConcurrentHashMap<>();

    public  HttpRequestHandler(TableManager tbm) {
        super();
        this.tbm=tbm;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        Executor executor=new Executor(0,tbm);
//        map.put(ctx.channel(),executor);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//        map.remove(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        //100 Continue
        if (is100ContinueExpected(req)) {
            ctx.write(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }

        List<String> cookieHeaders = req.headers().getAll(COOKIE);
        String sessionId = null;
        for (String cookieHeader : cookieHeaders) {
            Set<io.netty.handler.codec.http.cookie.Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieHeader);
            for (Cookie cookie : cookies) {
                if (cookie.name().equals("sessionID")) {
                    sessionId = cookie.value();
                    break;
                }
            }
        }

        String s = req.content().toString(CharsetUtil.UTF_8);
        Gson gson=new Gson();
        Sql sql = gson.fromJson(s, Sql.class);



        Session session = getSession(sessionId);
        byte[] execute = session.getExecutor().execute(sql.sql.getBytes());


        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.copiedBuffer(new String(execute), CharsetUtil.UTF_8));

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");

        if(sessionId==null){
            response.headers().add(SET_COOKIE, io.netty.handler.codec.http.cookie.ServerCookieEncoder.STRICT.encode(new DefaultCookie("sessionID", session.getSessionId())));
        }



        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private Session getSession(String sessionId) {
        if (sessionId == null || !sessions.containsKey(sessionId)) {
            sessionId = UUID.randomUUID().toString();
            Executor executor=new Executor(0,tbm);
            sessions.put(sessionId,new Session(sessionId,executor));
        }
        return sessions.get(sessionId);
    }

    static class Session{
        private String sessionId;
        private Executor executor;

        public Session(String sessionId, Executor executor) {
            this.sessionId = sessionId;
            this.executor = executor;
        }

        public String getSessionId() {
            return sessionId;
        }

        public Executor getExecutor() {
            return executor;
        }


    }






}
