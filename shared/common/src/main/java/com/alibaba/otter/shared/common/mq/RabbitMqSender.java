package com.alibaba.otter.shared.common.mq;

import com.alibaba.otter.shared.common.model.config.data.mq.RabbitMqMediaSource;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Creator: laizhicheng
 * DateTime: 2020/9/24 18:00
 * Description: No Description
 */
public class RabbitMqSender {

    private RabbitMqMediaSource rabbitMqMediaSource;

    private Connection connection;
    private Channel channel;

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqSender.class);

    //TODO 或许要做LRU删除
    private static ConcurrentHashMap<RabbitMqMediaSource, RabbitMqSender> senderCache = new ConcurrentHashMap<RabbitMqMediaSource, RabbitMqSender>();

    private RabbitMqSender(RabbitMqMediaSource rabbitMqMediaSource) throws IOException, TimeoutException {
        assert rabbitMqMediaSource != null;
        this.rabbitMqMediaSource = rabbitMqMediaSource;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitMqMediaSource.getUsername());
        factory.setPassword(rabbitMqMediaSource.getPassword());
        final String[] address = rabbitMqMediaSource.getUrl().split(":");
        factory.setHost(address[0]);
        factory.setPort(Integer.parseInt(address[1]));
        factory.setVirtualHost(rabbitMqMediaSource.getVirtualHost());

        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    public static RabbitMqSender getSender(RabbitMqMediaSource rabbitMqMediaSource) throws Exception {
        RabbitMqSender sender = senderCache.get(rabbitMqMediaSource);
        if (sender == null) {
            final RabbitMqSender newSender = createNewSender(rabbitMqMediaSource);
            sender = senderCache.putIfAbsent(rabbitMqMediaSource, newSender);
            if (sender != null) {
                newSender.close();
            } else {
                sender = newSender;
            }
        }
        return sender;
    }

    public static RabbitMqSender createNewSender(RabbitMqMediaSource rabbitMqMediaSource) throws Exception {
        return new RabbitMqSender(rabbitMqMediaSource);
    }

    public void send(String exchange, String routeKey, String message) throws IOException {
        logger.debug("发送MQ：{}，{}", exchange, routeKey);
        channel.basicPublish(exchange, routeKey, null, message.getBytes(Charset.forName("utf8")));
    }

    public void close() throws Exception {
        channel.close();
        connection.close();
        for (Iterator<Map.Entry<RabbitMqMediaSource, RabbitMqSender>> it = senderCache.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<RabbitMqMediaSource, RabbitMqSender> next = it.next();
            if (this == next.getValue()) {
                it.remove();
                break;
            }
        }
    }
}
