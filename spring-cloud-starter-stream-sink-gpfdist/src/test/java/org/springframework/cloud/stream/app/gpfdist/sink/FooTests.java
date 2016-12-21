package org.springframework.cloud.stream.app.gpfdist.sink;

import org.junit.Test;
import org.reactivestreams.Processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.server.HttpServer;

public class FooTests {

	@Test
	public void testFoo() throws Exception {
		Processor<ByteBuf, ByteBuf> processor = WorkQueueProcessor.create(false);
		GpfdistServer server = new GpfdistServer(processor, 8080, 5, 1, 1, 10);
		server.start();

//		for (int i = 0; i < 100; i++) {
//			String d = Integer.toString(i) + "\n";
//			System.out.println("Sending " + i);
//			processor.onNext(Unpooled.copiedBuffer(d.getBytes()));
//		}
//		Thread.sleep(20000);
		server.stop();
	}

	@Test
	public void testHang() throws Exception {
		NettyContext httpServer = HttpServer
				.create(opts -> opts.listen("0.0.0.0", 0)
						.eventLoopGroup(new NioEventLoopGroup(10)))
				.newRouter(r -> r.get("/data", (request, response) -> {
					return response.send(Mono.empty());
				})).block();
		httpServer.dispose();
	}


}
