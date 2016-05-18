/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis;

/**
 * Subscribes to a publish/subscribe topic which uses redis for one to many communication.
 * <p>
 * A class implementing this interface must wear a {@link sirius.kernel.di.std.Register} annotation to be
 * discovered by {@link Redis}. Once a message is published via {@link Redis#publish(String, String)} all subscribers
 * on all connected nodes will be notified via {@link #onMessage(String)}.
 */
public interface Subscriber {

    /**
     * Returns the name of the topic to subscribe to.
     *
     * @return the name of the topic in redis
     */
    String getTopic();

    /**
     * Invoked for each message received from redis for the subscribed topic.
     *
     * @param message the message that was published
     */
    void onMessage(String message);
}
