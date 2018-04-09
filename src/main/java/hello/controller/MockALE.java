package hello.controller;

import hello.model.ALE;
import hello.services.Poll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;


@RestController
public class MockALE {

    @Autowired
    private Poll poll;

    public MockALE(Poll poll) {
        this.poll = poll;
    }

    @GetMapping(path = "/{topic}/{watermark}")
    public CompletableFuture<List<ALE>> get(@PathVariable("topic") String topic, @PathVariable("watermark") String watermark) {
        System.out.println("api called with topic: " + topic + " \n\thighwatermark: " + watermark + " \n --------------- \n");
        return CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(10000, 20000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return Arrays.asList(ALE.builder().id(LocalDateTime.now().toEpochSecond(ZoneOffset.MAX))
                    .payload(topic + watermark + LocalDateTime.now().toEpochSecond(ZoneOffset.MAX))
                    .dateTimestamp(Date.from(Instant.now())).build(),
                    ALE.builder().id(LocalDateTime.now().toEpochSecond(ZoneOffset.MAX) + 1)
                            .payload(topic + watermark + LocalDateTime.now().toEpochSecond(ZoneOffset.MAX))
                            .dateTimestamp(Date.from(Instant.now())).build(),
                    ALE.builder().id(LocalDateTime.now().toEpochSecond(ZoneOffset.MAX) + 2)
                            .payload(topic + watermark + LocalDateTime.now().toEpochSecond(ZoneOffset.MAX))
                            .dateTimestamp(Date.from(Instant.now())).build(),
                    ALE.builder().id(LocalDateTime.now().toEpochSecond(ZoneOffset.MAX) + 3)
                            .payload(topic + watermark + LocalDateTime.now().toEpochSecond(ZoneOffset.MAX))
                            .dateTimestamp(Date.from(Instant.now())).build());
        });
    }

    @GetMapping(path = "/trigger/{topic}")
    public void trigger(@PathVariable("topic") String topic) {
        poll.poll(topic);
    }


}
