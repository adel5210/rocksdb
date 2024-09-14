package com.adel.rocksdb;

import com.adel.rocksdb.repository.RocksDBRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.StopWatch;

import java.util.stream.IntStream;

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class RocksdbApplication implements CommandLineRunner {

    private final RocksDBRepository rocksDBRepository;

    public static void main(String[] args) {
        SpringApplication.run(RocksdbApplication.class, args);
    }

    /**
     * Process time for 1_000_000 entries
     * With logs: 16.6 sec
     * Without logs: 8.16 sec
     * @param args
     * @throws Exception
     */
    @Override
    public void run(final String... args) throws Exception {
        log.info("Initializing RocksDB repository...");
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        IntStream.range(0, 1_000_000).forEach(i -> rocksDBRepository.save("key_" + i, "value_" + i));
        IntStream.range(0, 1_000_000).forEach(i -> rocksDBRepository.find("key_" + i));
        stopWatch.stop();
        log.info("RocksDB operations completed in {} s", stopWatch.getTotalTimeSeconds());
//        log.info("Save operation: {}", rocksDBRepository.save("key1", "value1"));
//        log.info("Get operation: {}", rocksDBRepository.find("key1"));
//        log.info("Delete operation: {}", rocksDBRepository.delete("key1"));
//        log.info("Get operation: {}", rocksDBRepository.find("key1"));
    }
}
