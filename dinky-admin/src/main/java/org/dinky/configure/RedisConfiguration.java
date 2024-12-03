package org.dinky.configure;

import org.dinky.configure.properties.DinkyRedisProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.embedded.RedisServer;

@Configuration
@EnableConfigurationProperties(DinkyRedisProperties.class)
public class RedisConfiguration {
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            prefix = "spring.redis",
            name = "mode",
            havingValue = "embedded",
            matchIfMissing = true)
    @Order(-1)
    public RedisServer redisServer(RedisProperties redisProperties){
        RedisServer redisServer = new RedisServer(redisProperties.getPort());
        redisServer.start();
        return  redisServer;
    }
    @Bean
    @Order(1)
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        //设置Redis链接工厂对象
        template.setConnectionFactory(redisConnectionFactory);

        // 设置键的序列化方式为 String
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());

        // 设置值的序列化方式为 JSON
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());

        return template;
    }
}
