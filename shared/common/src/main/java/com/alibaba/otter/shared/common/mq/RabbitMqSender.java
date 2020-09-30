package com.alibaba.otter.shared.common.mq;

import com.alibaba.otter.shared.common.model.config.data.mq.RabbitMqMediaSource;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.concurrent.TimeoutException;

/**
 * Creator: laizhicheng
 * DateTime: 2020/9/24 18:00
 * Description: No Description
 */
public class RabbitMqSender {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqSender.class);

    private ConnectionFactory connectionFactory;
    private volatile Connection connection;
    private boolean keepAlive;


    private final LinkedList<Channel> channelPool = new LinkedList<Channel>();
    private static final int CHANNEL_POOL_SIZE = 25;

    /**
     * 操作连接时要先获得锁
     */
    private final Object connectionMonitor = new Object();

    public RabbitMqSender(RabbitMqMediaSource rabbitMqMediaSource) {
        this(rabbitMqMediaSource, false);
    }

    public RabbitMqSender(RabbitMqMediaSource rabbitMqMediaSource, boolean keepAlive) {
        assert rabbitMqMediaSource != null;
        this.connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername(rabbitMqMediaSource.getUsername());
        connectionFactory.setPassword(rabbitMqMediaSource.getPassword());
        final String[] address = rabbitMqMediaSource.getUrl().split(":");
        connectionFactory.setHost(address[0]);
        connectionFactory.setPort(Integer.parseInt(address[1]));
        connectionFactory.setVirtualHost(rabbitMqMediaSource.getVirtualHost());

        this.keepAlive = keepAlive;
    }

    public static RabbitMqSender createNewSender(RabbitMqMediaSource rabbitMqMediaSource) {
        return new RabbitMqSender(rabbitMqMediaSource);
    }

    public void checkConnect() {
        initConnect();
        if (!keepAlive) {
            close(null);
        }
    }


    public void checkExchange(String exchange) {
        final Channel channel = getChannel();
        try {
            channel.exchangeDeclarePassive(exchange);
        } catch (Exception e) {
            throw new RuntimeException("exchage：" + exchange + "不存在");
        }
        if (!keepAlive) {
            close(channel);
        }
    }


    private void initConnect() {
        if (connection == null || !connection.isOpen()) {
            synchronized (connectionMonitor) {
                if (connection == null || !connection.isOpen()) {
                    try {
                        channelPool.clear();
                        connection = connectionFactory.newConnection();
                    } catch (IOException e) {
                        throw new RuntimeException("连接MQ失败", e);
                    } catch (TimeoutException e) {
                        throw new RuntimeException("连接MQ超时", e);
                    }
                }
            }
        }
    }

    public void send(String exchange, String routeKey, String message) throws IOException {
        final Channel channel = getChannel();
        try {
            channel.basicPublish(exchange, routeKey, null, message.getBytes(Charset.forName("utf8")));
            logger.debug("发送了MQ：{}，{}", exchange, routeKey);
        } finally {
            close(channel);
        }
    }

    private Channel getChannel() {
        Channel channel = null;
        if (connection != null && connection.isOpen()) { //先从缓存池找
            channel = findOpenChannel();
        }
        if (channel == null) { //从连接创建
            initConnect();
            try {
                channel = connection.createChannel();
            } catch (IOException e) {
                throw new RuntimeException("创建channel失败", e);
            }
        }
        return channel;
    }


    private Channel findOpenChannel() {
        Channel channel = null;
        synchronized (channelPool) {
            while (!channelPool.isEmpty()) {
                channel = channelPool.removeFirst();
                if (channel.isOpen()) {
                    break;
                }
            }
        }
        return channel;
    }

    private void close(Channel channel) {
        if (keepAlive) { //保持连接，放回channel池
            if ((channel != null && !channel.isOpen()) || channelPool.size() >= CHANNEL_POOL_SIZE) {
                try {
                    if (channel != null)
                        channel.close();
                } catch (Exception e) {
                    //
                }
                return;
            }
            channelPool.addLast(channel);
        } else {
            try {
                if (channel != null)
                    channel.close();
            } catch (AlreadyClosedException e) {
                //
            } catch (IOException e) {
                logger.warn("关闭通道失败", e);
            } catch (TimeoutException e) {
                logger.warn("关闭通道超时", e);
            }
            try {
                if (connection != null)
                    connection.close();
            } catch (AlreadyClosedException e) {
                //
            } catch (IOException e) {
                logger.warn("关闭连接失败", e);
            }
        }
    }

    public void destroy() {
        while (channelPool.size() > 0) {
            final Channel channel = channelPool.removeFirst();
            try {
                channel.close();
            } catch (AlreadyClosedException e) {
                //
            } catch (Exception e) {
                logger.warn("关闭通道失败", e);
            }
        }
        try {
            if (connection != null)
                connection.close();
        } catch (AlreadyClosedException e) {
            //
        } catch (Exception e) {
            logger.warn("关闭连接失败", e);
        }
    }
}
