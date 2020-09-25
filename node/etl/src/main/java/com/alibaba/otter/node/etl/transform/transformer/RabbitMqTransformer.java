package com.alibaba.otter.node.etl.transform.transformer;

import com.alibaba.otter.shared.etl.model.EventData;

/**
 * Creator: laizhicheng
 * DateTime: 2020/9/24 16:03
 * Description: No Description
 */
public class RabbitMqTransformer implements OtterTransformer<EventData, EventData> {

    @Override
    public EventData transform(EventData data, OtterTransformerContext context) {
        if(data.getEventType().isDdl()){
            return null;
        }
        return data;
    }
}
