package com.ascentstream.demo.business.healthcheck.controller;


import com.ascentstream.demo.pulsar.service.PulsarProducerService;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/healthcheck")
public class HealthcheckController {


    private final PulsarProducerService pulsarProducerService;

    public HealthcheckController(PulsarProducerService pulsarProducerService) {
        this.pulsarProducerService = pulsarProducerService;
    }


    @GetMapping("")
    public String healthcheck() {
        return "OKOKOK";
    }

    @GetMapping("/produce")
    public String produce() throws PulsarClientException {
        return pulsarProducerService.sendTestMessage().toString();
    }

    @GetMapping("/produce4json")
    public String produce4json() throws PulsarClientException {
        return pulsarProducerService.sendTestMessage4Json().toString();
    }

    @GetMapping("/produce4key")
    public String produceWithKey(@RequestParam(value = "key") String key) throws PulsarClientException {
        return pulsarProducerService.sendMessageWithKey("Message with key", key).toString();
    }

    @GetMapping("/produce4delay")
    public String produceWithKey(@RequestParam(value = "delay") long delay) throws PulsarClientException {
        return pulsarProducerService.sendDelayedMessage(delay).toString();
    }

}
