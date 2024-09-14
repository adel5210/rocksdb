package com.adel.rocksdb.repository;

import java.util.Optional;

/**
 * @author Adel.Albediwy
 */
public interface BaseRepository<K,V> {
    boolean save(K key, V value);
    Optional<V> find(K key);
    boolean delete(K key);
}
