package org.redquark.dkg.generators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Enumeration;
import java.util.Queue;

public class SnowflakeKeyGenerator implements KeyGenerator, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeKeyGenerator.class);

    // Unused bit, which is set to 0 and is for future purposes.
    // It also acts as a signed bit making id positive.
    // Always set to 0.
    private static final int UNUSED_BIT = 1;

    // These bits represent the epoch time. Epoch time
    // is necessary to create sortable keys/IDs
    private static final int EPOCH_BITS = 41;

    // These bits represent the id of the nodes in a cluster.
    // In this cluster, various nodes are generating keys
    private static final int NODE_ID_BITS = 10;

    // These bits represent the sequence which is reset after
    // every one millisecond is elapsed.
    private static final int SEQUENCE_BITS = 12;

    // Maximum node id. Since nodes are being represented by
    // NODE_ID_BITS, there is a finite number of node in a
    // cluster. This variable represents maximum possible node id.
    private static final int MAXIMUM_NODE_ID = (1 << NODE_ID_BITS) - 1; // [0 to 1025]

    // Maximum possible sequence. Every node has the capacity to
    // generate a certain number of keys per millisecond. This
    // variable signifies that number.
    private static final int MAXIMUM_SEQUENCE_CAPACITY = (1 << SEQUENCE_BITS) - 1; // [0 to 4095]

    // December 20, 2022, 15:43.
    // This number will be used to calculate the elapsed time
    // and gives us the estimate until when we can create unique
    // keys.
    private static final long DEFAULT_EPOCH = 1671531200;
    // ID of the current node in the cluster.
    private final int currentNodeId;
    // List of all the generated keys by the service
    private final Queue<Long> generatedKeys = new ArrayDeque<>();
    // Thread-safe timestamp to compare
    private volatile long lastTimeStamp = -1L;
    // Current value of the sequence
    private volatile long currentSequence = 0L;

    public SnowflakeKeyGenerator() {
        this.currentNodeId = generateNodeId();
        if (currentNodeId < 0 || currentNodeId > MAXIMUM_NODE_ID) {
            throw new IllegalArgumentException("Invalid node id");
        }
    }

    /**
     * This method generates unique key in a distributed fashion.
     * It follows an implementation based on Twitter's snowflake.
     */
    private synchronized long getNextKey() {
        long currentTimestamp = getTimestamp();
        if (currentTimestamp < lastTimeStamp) {
            throw new IllegalArgumentException("Invalid system clock!");
        }
        if (currentTimestamp == lastTimeStamp) {
            currentSequence = (currentSequence + 1) & MAXIMUM_SEQUENCE_CAPACITY;
            if (currentSequence == 0) {
                LOGGER.warn("Sequence is exhausted. We will wait for the next millisecond");
                currentTimestamp = waitTillNextMillisecond(currentTimestamp);
            }
        } else {
            // Reset sequence for the next millisecond
            currentSequence = 0;
        }
        // Update the last timestamp
        lastTimeStamp = currentTimestamp;
        // Generate id of the node
        long id = currentTimestamp << (NODE_ID_BITS + SEQUENCE_BITS);
        id |= ((long) currentNodeId << SEQUENCE_BITS);
        id |= currentSequence;
        return id;
    }

    private int generateNodeId() {
        LOGGER.info("Creation of node id starts...");
        int nodeId;
        try {
            StringBuilder nodeIdString = new StringBuilder();
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                // Get current network interface
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                // Get the MAC address byte array
                byte[] mac = networkInterface.getHardwareAddress();
                if (mac != null) {
                    for (byte currentByte : mac) {
                        nodeIdString.append(String.format("%02X", currentByte));
                    }
                }
            }
            nodeId = nodeIdString.toString().hashCode();
        } catch (SocketException e) {
            LOGGER.error("Could not get network interface. Creating node id randomly");
            nodeId = (new SecureRandom()).nextInt();
        }
        nodeId &= MAXIMUM_NODE_ID;
        return nodeId;
    }

    private long getTimestamp() {
        return Instant.now().toEpochMilli() - DEFAULT_EPOCH;
    }

    private long waitTillNextMillisecond(long currentTimestamp) {
        while (currentTimestamp == lastTimeStamp) {
            currentTimestamp = getTimestamp();
        }
        return currentTimestamp;
    }

    /**
     * This method creates a key and add it to a queue for
     * the client service to pick
     */
    @Override
    public void run() {
        generatedKeys.offer(getNextKey());
    }

    public long getNextGeneratedKey() {
        if (!generatedKeys.isEmpty()) {
            return generatedKeys.remove();
        }
        throw new IllegalArgumentException("All keys are exhausted!");
    }
}
