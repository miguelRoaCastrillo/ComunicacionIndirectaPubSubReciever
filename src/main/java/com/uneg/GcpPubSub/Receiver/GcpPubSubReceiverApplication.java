package com.uneg.GcpPubSub.Receiver;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

@SpringBootApplication
public class GcpPubSubReceiverApplication {

	public static void main(String[] args) {
		SpringApplication.run(com.uneg.GcpPubSub.Receiver.GcpPubSubReceiverApplication.class, args);
	}

	/* Para recibir mensajes de manera "manual"
	@Bean
	public PubSubInboundChannelAdapter messageChannelAdapter(
			@Qualifier("demoInputChannel") MessageChannel inputChannel,
			PubSubTemplate pubSubTemplate) {

		PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(pubSubTemplate
				, "musica-sub");
		adapter.setOutputChannel(inputChannel);
		return adapter;
	}

	@Bean
	public MessageChannel demoInputChannel() {
		return new DirectChannel();
	}

	@ServiceActivator(inputChannel = "demoInputChannel")
	public void messageReceiver(String message) {
		System.out.println("Message arrived: " + message);
	}
	 */
}