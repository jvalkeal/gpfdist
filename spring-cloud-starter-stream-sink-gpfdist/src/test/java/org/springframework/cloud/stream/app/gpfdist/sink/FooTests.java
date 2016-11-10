package org.springframework.cloud.stream.app.gpfdist.sink;

import org.junit.Test;
import org.reactivestreams.Processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.WorkQueueProcessor;

public class FooTests {

	@Test
	public void testFoo() throws Exception {
		Processor<ByteBuf, ByteBuf> processor = WorkQueueProcessor.create(false);
		GpfdistServer server = new GpfdistServer(processor, 8080, 1, 1, 1, 10);
		server.start();

		for (int i = 0; i < 100; i++) {
			String d = Integer.toString(i);
			System.out.println("Sending " + d);
			processor.onNext(Unpooled.copiedBuffer(d.getBytes()));
		}
		Thread.sleep(20000);
//		server.stop();
	}

}
