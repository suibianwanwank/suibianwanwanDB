package ccsu.server;

import com.ccsu.transport.Encoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.Scanner;

public class HelloClient {
    public static Channel channel;
    public static void main(String[] args) throws InterruptedException {

        String test="create table table1(\n" +
                "\ta22 int index,\n" +
                "\tb22 long\n" +
                ")";
        String test1="insert into table1(a22,b22) values(3,4);";
        String test5="delete from table1 where a22=45;";
        //启动类
        new Bootstrap()
                //选择eventloop
                .group(new NioEventLoopGroup())
                //3.选择客户端channel实现
                .channel(NioSocketChannel.class)
                //添加处理器
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new MyHandler());
                    }

                })
                .connect(new InetSocketAddress("localhost",7456))
                .sync();
        while (true){
            Scanner scanner=new Scanner(System.in);
            String s = scanner.nextLine();
            channel.writeAndFlush(Encoder.decode(s.getBytes()));

        }

    }

}
