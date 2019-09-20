/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import sirius.kernel.di.std.Register;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Used to generate IDs or keys in distributed environments where numeric sequences aren't suitable.
 */
@Register(classes = KeyGenerator.class)
public class KeyGenerator {

    private String seed = Hashing.sha512().hashLong(System.nanoTime()).toString();

    /**
     * Generates an unique random io.
     *
     * @return a random id in base 32 encoding
     */
    public String generateId() {
        byte[] rndBytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(rndBytes);
        return BaseEncoding.base32Hex().encode(rndBytes).replace("=", "");
    }

    /**
     * Generates a long unique id.
     * <p>
     * Due to its length and secure generation pattern such IDs are suitable for auth tokens and the like.
     *
     * @return a newly generated secure unique id
     */
    public String generateSecureId() {
        byte[] input = new byte[256];
        ThreadLocalRandom.current().nextBytes(input);
        return Hashing.sha256()
                      .newHasher()
                      .putString(seed, Charsets.UTF_8)
                      .putBytes(input)
                      .putLong(System.nanoTime())
                      .hash()
                      .toString();
    }
}
