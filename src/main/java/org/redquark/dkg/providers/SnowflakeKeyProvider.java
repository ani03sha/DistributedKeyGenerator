package org.redquark.dkg.providers;

import org.redquark.dkg.generators.KeyGenerator;
import org.redquark.dkg.generators.SnowflakeKeyGenerator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SnowflakeKeyProvider {

    private final KeyGenerator keyGenerator;

    public SnowflakeKeyProvider() {
        this.keyGenerator = new SnowflakeKeyGenerator();
    }

    public void generateKeys(int keyGenerationRate) {
        ExecutorService executor = Executors.newFixedThreadPool(keyGenerationRate);
        for (int i = 0; i < keyGenerationRate; i++) {
            executor.execute((Runnable) keyGenerator);
        }
        executor.shutdown();
        System.out.println("Finished all threads");
    }

    public Long getKey() {
        return keyGenerator.getNextGeneratedKey();
    }
}
