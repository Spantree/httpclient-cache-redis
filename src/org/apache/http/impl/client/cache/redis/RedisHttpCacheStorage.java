package org.apache.http.impl.client.cache.redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.http.client.cache.HttpCacheEntry;
import org.apache.http.client.cache.HttpCacheStorage;
import org.apache.http.client.cache.HttpCacheUpdateCallback;
import org.apache.http.client.cache.HttpCacheUpdateException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisHttpCacheStorage implements HttpCacheStorage {

	JedisPool pool;
	
	public RedisHttpCacheStorage(String host, int port) {
		pool = new JedisPool(new JedisPoolConfig(), host, port);
	}
	
	public void putEntry(String url, HttpCacheEntry entry) throws IOException {
		Jedis jedis = pool.getResource();
		jedis.set(url.getBytes(), serializeEntry(entry));
		pool.returnResource(jedis);
	}

	public HttpCacheEntry getEntry(String url) throws IOException {
		Jedis jedis = pool.getResource();
		byte[] cacheEntryBytes = jedis.get(url.getBytes());
		pool.returnResource(jedis);
		return deserializeEntry(cacheEntryBytes);
	}

	public void removeEntry(String url) throws IOException {
		Jedis jedis = pool.getResource();
		jedis.del(url.getBytes());
		pool.returnResource(jedis);
	}

	public void updateEntry(String url, HttpCacheUpdateCallback callback)
			throws IOException, HttpCacheUpdateException {
		Jedis jedis = pool.getResource();
		
		byte[] key = url.getBytes();
		byte[] cacheEntryBytes = jedis.get(key);
		
		HttpCacheEntry entry = deserializeEntry(cacheEntryBytes);
		entry = callback.update(entry);
		
		jedis.set(key, serializeEntry(entry));
		
		pool.returnResource(jedis);
	}
	
	protected byte[] serializeEntry(HttpCacheEntry entry) throws IOException{
		if(entry==null) return null;
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out;

		out = new ObjectOutputStream(bos);
		out.writeObject(entry);
		
		byte[] entryBytes = bos.toByteArray();
		
		out.close();
		bos.close();
		
		return entryBytes;
	}
	
	protected HttpCacheEntry deserializeEntry(byte[] entryBytes) throws IOException {
		if(entryBytes==null) return null;
		
		ByteArrayInputStream bis = new ByteArrayInputStream(entryBytes);
		ObjectInput in = new ObjectInputStream(bis);
		HttpCacheEntry entry = null;
		try {
			entry = (HttpCacheEntry) in.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException("Couldn't deserialize HTTP entry from redis cache.",e);
		}
		
		bis.close();
		in.close();
		return entry;
	}
	
	public void shutdown() {
		pool.destroy();
	}

}
