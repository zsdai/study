package com.demo.kafka.web;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/kafka")
public class CollectController {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    
    @Autowired
    private KafkaTemplate kafkaTemplate;
    
    @RequestMapping(value = "/send/{message}")
    public String send(@PathVariable String message) {
        try {
            logger.info("kafka的消息={}", message);
            kafkaTemplate.send("test", "key", message);
            logger.info("发送kafka成功.");
            return "发送kafka成功.";
        } catch (Exception e) {
            logger.error("发送kafka失败", e);
            return "发送kafka失败:"+e.getMessage();
        }
    }

}
