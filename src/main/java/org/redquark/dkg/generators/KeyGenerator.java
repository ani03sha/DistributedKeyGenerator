package org.redquark.dkg.generators;

public interface KeyGenerator {

    /**
     * This method generates unique key in a distributed fashion.
     */
    long getNextGeneratedKey();
}
