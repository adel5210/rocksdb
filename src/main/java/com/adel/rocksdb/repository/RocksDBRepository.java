package com.adel.rocksdb.repository;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Repository;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

/**
 * @author Adel.Albediwy
 */
@Slf4j
@Repository
public class RocksDBRepository implements BaseRepository<String, Object>  {
    private final static String DB_NAME = "rocksdb_db";
    private RocksDB db;

    @PostConstruct
    public void init() {
        RocksDB.loadLibrary();

        final Options options = new Options();
        options.setCreateIfMissing(true);
        final File baseDir = new File("/tmp/rocks", DB_NAME);

        try{
            Files.createDirectories(baseDir.getParentFile().toPath());
            Files.createDirectories(baseDir.getAbsoluteFile().toPath());
            db = RocksDB.open(options, baseDir.getAbsolutePath());

            log.info("RocksDB instance initialized successfully.");
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public synchronized boolean save(final String key, final Object value) {
//        log.info("Saving key: {}, value: {}", key, value);

        try {
            db.put(key.getBytes(), SerializationUtils.serialize(value));
        } catch (RocksDBException e) {
//            log.error("Error saving key: {}, value: {}", key, value, e);
            return false;
        }

        return true;
    }

    @Override
    public synchronized Optional<Object> find(final String key) {

        try {
            byte[] bytes = db.get(key.getBytes());
            if(null!= bytes) {
                final Object value = SerializationUtils.deserialize(bytes);
                return Optional.of(value);
            }
        } catch (RocksDBException e) {
//            log.error("Error finding key: {}", key, e);
            return Optional.empty();
        }

        return Optional.empty();
    }

    @Override
    public synchronized boolean delete(final String key) {
//        log.info("Deleting key: {}", key);
        try {
            db.delete(key.getBytes());
        } catch (RocksDBException e) {
//            log.error("Error deleting key: {}", key, e);
            return false;
        }
        return true;
    }
}
