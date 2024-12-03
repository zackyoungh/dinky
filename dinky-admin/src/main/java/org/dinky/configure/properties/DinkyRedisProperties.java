package org.dinky.configure.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.redis")
@Getter
@Setter
public class DinkyRedisProperties {
    private RedisRunMode mode;

    public enum RedisRunMode {
        Standalone, Embedded
    }
    public boolean isEmbedded(){
        return RedisRunMode.Embedded.equals(mode);
    }
}
