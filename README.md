# pulsar-examples
Pulsar examples. 该仓库提供了Apache Pulsar的Quick Start.

## 项目结构
- `common-dependency`, 公共依赖, 如工具类等
- `pulsar-demo`, Pulsar client 代码示例.
- `pulsar-springboot-demo`, Pulsar与SpringBoot集成代码示例.


## 启动类注解说明
`pulsar-springboot-demo`, 启动类注意: 启动类中默认不加载4种订阅模式定义的Bean, 请按需注释掉需要测试的消费者订阅类. 如, 希望测试Exclusive订阅, 注释掉即可.
```java
@ComponentScan(basePackages = {"com.ascentstream.demo"},
        excludeFilters = {
        @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = {
                //PulsarListener4Exclusive.class,
                PulsarListener4Failover.class,
                PulsarListener4Shared.class,
                PulsarListener4KeyShared.class,
        })
})
@EnablePulsar
public class PulsarSpringbootDemoApplication extends SpringBootServletInitializer
```

## 引用
SpringBoot与Pulsar集成, 详细配置请阅读[官方文档](https://docs.spring.io/spring-pulsar/reference/reference/pulsar.html)