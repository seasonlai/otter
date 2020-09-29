package com.alibaba.otter.node.etl.common.mq;

import com.alibaba.otter.shared.common.model.config.data.mq.RabbitMqMediaSource;
import com.alibaba.otter.shared.common.mq.RabbitMqSender;
import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;
import com.google.common.collect.OtterMigrateMap;
import org.springframework.beans.factory.DisposableBean;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Creator: laizhicheng
 * DateTime: 2020/9/28 16:56
 * Description: No Description
 */
public class RabbitMqSenderFactory implements DisposableBean {

    private Map<Long, Map<RabbitMqMediaSource, RabbitMqSender>> rabbitmqSenders;

    public RabbitMqSenderFactory() {
        rabbitmqSenders = OtterMigrateMap.makeSoftValueComputingMapWithRemoveListenr(new Function<Long, Map<RabbitMqMediaSource, RabbitMqSender>>() {
            public Map<RabbitMqMediaSource, RabbitMqSender> apply(@Nullable Long pipelineId) {
                return MigrateMap.makeComputingMap(new Function<RabbitMqMediaSource, RabbitMqSender>() {

                    public RabbitMqSender apply(@Nullable RabbitMqMediaSource rabbitMqMediaSource) {
                        return new RabbitMqSender(rabbitMqMediaSource, true);
                    }
                });
            }
        }, new OtterMigrateMap.OtterRemovalListener<Long, Map<RabbitMqMediaSource, RabbitMqSender>>() {
            @Override
            public void onRemoval(Long pipelineId, Map<RabbitMqMediaSource, RabbitMqSender> rabbitMqSenders) {
                if (rabbitMqSenders == null)
                    return;
                for (RabbitMqSender sender : rabbitMqSenders.values()) {
                    try {
                        sender.destroy();
                    } catch (Exception e) {
                    }
                }
            }
        });
    }

    public RabbitMqSender getSender(Long pipelineId, RabbitMqMediaSource rabbitMqMediaSource) {
        return rabbitmqSenders.get(pipelineId).get(rabbitMqMediaSource);
    }


    public void destory(Long pipelineId) {
        final Map<RabbitMqMediaSource, RabbitMqSender> removeMap = rabbitmqSenders.remove(pipelineId);
        if (removeMap != null) {
            for (RabbitMqSender sender : removeMap.values()) {
                try {
                    sender.destroy();
                } catch (Exception e) {
                }
            }
        }
    }

    @Override
    public void destroy() {
        for (Long pipelineId : rabbitmqSenders.keySet()) {
            destory(pipelineId);
        }
    }
}
