package ccsu.server;

import com.ccsu.transport.Encoder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;


public class MyHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("链接已经建立");
        HelloClient.channel=ctx.channel();
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        byte[] encode = Encoder.encode(msg);
        System.out.println("收到消息");
        System.out.println(new String(encode));
        super.channelRead(ctx, msg);
    }
}
