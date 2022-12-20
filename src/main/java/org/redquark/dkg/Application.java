package org.redquark.dkg;

import org.redquark.dkg.providers.SnowflakeKeyProvider;

public class Application {

    public static void main(String[] args) throws InterruptedException {
        SnowflakeKeyProvider provider = new SnowflakeKeyProvider();
        provider.generateKeys(20);
        Thread.sleep(2000);
        for (int i = 0; i < 20; i++) {
            System.out.println(provider.getKey());
        }
    }
}
