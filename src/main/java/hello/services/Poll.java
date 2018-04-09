package hello.services;

import hello.model.ALE;
import hello.model.Watermark;
import hello.repository.WatermarkRespository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;

@Service
public class Poll {

    @Autowired
    private WatermarkRespository watermarkRepo;

    @Autowired
    private CacheManager cacheManager;

    public void poll(String topic) {
        Long prevWatermark = watermarkRepo.findByTopic(topic).orElseGet(() -> Watermark.builder().highwaterMark(1L).build()).getHighwaterMark();
        Mono<ClientResponse> clientResponse = WebClient.create().get().uri("http://localhost:8080/{topic}/{watermark}", topic, prevWatermark).exchange();
        clientResponse.subscribe(s -> {
            System.out.println(s);
            s.bodyToMono(new ParameterizedTypeReference<List<ALE>>() {}).subscribe( success -> {
                ALE maxAle = success.stream().max((a,b) -> a.getId() > b.getId()? 1:-1).get();
                //System.out.println(maxAle);
                watermarkRepo.save(Watermark.builder().highwaterMark(maxAle.getId()).startDateTime(maxAle.getDateTimestamp()).topic(topic).build());
                System.out.println("\n\n--------success-------- \n list: " +  success + "\n max ale: " + maxAle + "\n prev watermark: " + prevWatermark + "\n new high: " + maxAle.getId() + "\n");
                        System.out.println(cacheManager.getCache("watermark").get(topic).get() + "\n\n");
                    },
                    error -> System.out.println(error),
                    () -> poll(topic));
            },
                e -> System.out.println("Error : " + e),
                () -> {
                    System.out.println("Done");

                });

    }
}
