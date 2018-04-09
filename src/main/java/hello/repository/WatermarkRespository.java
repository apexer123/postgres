package hello.repository;

import hello.model.Watermark;
import org.apache.ignite.springdata.repository.config.RepositoryConfig;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;


@Repository
public interface WatermarkRespository extends JpaRepository<Watermark, String> {
    @Cacheable(cacheNames = "watermark", key ="#topic")
    Optional<Watermark> findByTopic(String topic);

    @CachePut(cacheNames = "watermark", key ="#watermark.topic")
    Watermark save(Watermark watermark);
}
