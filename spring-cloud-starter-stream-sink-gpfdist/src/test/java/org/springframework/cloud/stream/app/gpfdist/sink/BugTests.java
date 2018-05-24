package org.springframework.cloud.stream.app.gpfdist.sink;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.function.Function;

import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpMessage;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.tcp.TcpServer;

public class BugTests {

	@Test
	public void testBug1() throws Exception {
		Processor<ByteBuf, ByteBuf> processor = WorkQueueProcessor.create(false);
		NettyContext nettyContext = createProtocolListener(processor);

		for (int i = 100000; i < 110000; i++) {
			String d = "DATA" + i + "\n";
			processor.onNext(Unpooled.copiedBuffer(d.getBytes()));
		}

		Thread.sleep(20000);
//		nettyContext.dispose();
	}

	@Test
	public void testBug2() throws Exception {
		createProtocolListener2();
		Thread.sleep(20000);
	}

	@Test
	public void testBug3() throws Exception {
		createProtocolListener3();
		Thread.sleep(200000);
	}

	@Test
	public void testBug4() throws Exception {
		createProtocolListener4();
		Thread.sleep(20000);
	}

	@Test
	public void testBug5() throws Exception {
		createProtocolListener5();
		Thread.sleep(1200000);
	}

	private void createProtocolListener5() {
		Flux<ByteBuf> stream = Flux.just("DATA0\n", "DATA1\n", "DATA2\n", "DATA3\n", "DATA4\n", "DATA5\n", "DATA6\n", "DATA7\n", "DATA8\n", "DATA9\n")
				.map(d -> {
					return Unpooled.copiedBuffer(d.getBytes());
				});



		TcpServer.create("0.0.0.0", 8080).newHandler((in, out) -> {
			out.context().addHandler(new HttpResponseEncoder());
			return out.sendObject(headers()).send(stream);
		}).block();

	}

	Object headers() {
		DefaultHttpResponse res = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.ACCEPTED);
//		DefaultFullHttpResponse res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.ACCEPTED);
		//
		res
			.headers()
				.add("Content-type", "text/plain")
				.add("Expires", "0")
				.add("X-GPFDIST-VERSION", "Spring Dataflow")
				.add("X-GP-PROTO", "1")
				.add("Cache-Control", "no-cache")
				.add("Connection", "close");
		return res;
	}

	private NettyContext createProtocolListener4() {
		Flux<ByteBuf> stream = Flux.just("DATA0\n", "DATA1\n", "DATA2\n", "DATA3\n", "DATA4\n", "DATA5\n", "DATA6\n", "DATA7\n", "DATA8\n", "DATA9\n")
				.map(d -> {
					return Unpooled.copiedBuffer(d.getBytes());
				});

		NettyContext httpServer = HttpServer
				.create(opts -> opts.listen("0.0.0.0", 8080)
						.eventLoopGroup(new NioEventLoopGroup(1))
						)
				.newRouter(r -> r.get("/data", (request, response) -> {
//					response.chunkedTransfer(false);
					return response
//							.options(NettyPipeline.SendOptions::flushOnEach)
							.send(stream);
				})).block();
		return httpServer;
	}


	private NettyContext createProtocolListener3() {
		Flux<String> stream = Flux.just("DATA0\n", "DATA1\n", "DATA2\n", "DATA3\n", "DATA4\n", "DATA5\n", "DATA6\n", "DATA7\n", "DATA8\n", "DATA9\n");
		NettyContext httpServer = HttpServer
				.create(opts -> opts.listen("0.0.0.0", 8080)
						.option(ChannelOption.TCP_NODELAY, true)
						.option(ChannelOption.SO_LINGER, 5)
						.eventLoopGroup(new NioEventLoopGroup(1))
						)
				.newRouter(r -> r.get("/data", (request, response) -> {
//					response.chunkedTransfer(false);
					return response
							.options(NettyPipeline.SendOptions::flushOnEach)
							.sendString(stream);
				})).block();
		return httpServer;
	}

	private NettyContext createProtocolListener2() {
		Flux<ByteBuf> stream = Flux.just("DATA0\n", "DATA1\n", "DATA2\n", "DATA3\n", "DATA4\n", "DATA5\n", "DATA6\n", "DATA7\n", "DATA8\n", "DATA9\n")
				.map(d -> {
					return Unpooled.copiedBuffer(d.getBytes());
				});

		NettyContext httpServer = HttpServer
				.create(opts -> opts.listen("0.0.0.0", 8080)
						.eventLoopGroup(new NioEventLoopGroup(1))
						)
				.newRouter(r -> r.get("/data", (request, response) -> {
					response.chunkedTransfer(false);
					return response
							.options(NettyPipeline.SendOptions::flushOnEach)
							.send(stream
									.map(new CodecFunction(response.alloc())));
				})).block();
		return httpServer;
	}

	private static class CodecFunction implements Function<ByteBuf, ByteBuf> {
		final ByteBufAllocator alloc;

		public CodecFunction(ByteBufAllocator alloc) {
			this.alloc = alloc;
		}

		@Override
		public ByteBuf apply(ByteBuf t) {
			return alloc.buffer()
					.writeBytes(t);
		}
	}

	private NettyContext createProtocolListener(Processor<ByteBuf, ByteBuf> processor) {
		WorkQueueProcessor<ByteBuf> workProcessor = WorkQueueProcessor.create("gpfdist-sink-worker", 8192, false);

		Flux<ByteBuf> stream = Flux.from(processor)
				.subscribeWith(workProcessor);

		NettyContext httpServer = HttpServer
				.create(opts -> opts.listen("0.0.0.0", 8080)
						.eventLoopGroup(new NioEventLoopGroup(10)))
				.newRouter(r -> r.get("/data", (request, response) -> {
					response.chunkedTransfer(false);
					return response
							.options(NettyPipeline.SendOptions::flushOnEach)
							.send(stream
									.take(10)
									.map(new CodecFunction(response.alloc())));
				})).block();
		return httpServer;
	}


//	private NettyContext createProtocolListener2(Processor<ByteBuf, ByteBuf> processor) {
//		WorkQueueProcessor<ByteBuf> workProcessor = WorkQueueProcessor.create("gpfdist-sink-worker", 8192, false);
//
//		Flux<ByteBuf> stream = Flux.from(processor)
//				.subscribeWith(workProcessor);
//
//		NettyContext httpServer = HttpServer
//				.create(opts -> opts.listen("0.0.0.0", 8080)
//						.eventLoopGroup(new NioEventLoopGroup(10)))
//				.newRouter(r -> r.get("/data", (request, response) -> {
//					response.chunkedTransfer(false);
//					return response
//							.options(NettyPipeline.SendOptions::flushOnEach)
//							.send(stream
//									.take(10)
//									.timeout(Duration.ofSeconds(1), Flux.<ByteBuf> empty())
//									.map(new CodecFunction(response.alloc())));
//				})).block();
//		return httpServer;
//	}

}
