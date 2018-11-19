/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.ubika.nifi.processors;

import es.ubika.nifi.processors.common.AbstractRedisProcessor;
import es.ubika.nifi.processors.common.RedisMessage;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Tags({"redis,pubsub,subscribe,read,reader,queue"})
@CapabilityDescription("Subscribes to a Redis PubSub queue and receives the messages coming from the specified channel/pattern.")
@WritesAttributes({@WritesAttribute(attribute="redis.channel", description="The channel where the message came from")})
public class ReadRedisPubSubProcessor extends AbstractRedisProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private boolean isSubscribed = false;

    private volatile LinkedBlockingQueue<RedisMessage> redisQueue;

    private final AtomicBoolean scheduled = new AtomicBoolean(false);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = getAbstractPropertyDescriptors();
        this.descriptors = Collections.unmodifiableList(descriptors);
        logger = getLogger();
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if(redisQueue == null) {
            redisQueue = new LinkedBlockingQueue<>();
        }
        buildClient(context);
        scheduled.set(true);
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        scheduled.set(false);
        clientConnectLock.writeLock().lock();
        if(isCluster) {
            redisClusterConn.addListener(new RedisPubSubClusterCallback());
            RedisClusterPubSubCommands<String, String> sync = redisClusterConn.sync();
            if(context.getProperty(PATTERN).isSet() && context.getProperty(PATTERN).asBoolean()) {
                sync.punsubscribe(context.getProperty(CHANNEL).getValue());
            } else {
                sync.unsubscribe(context.getProperty(CHANNEL).getValue());
            }
        } else {
            redisConn.addListener(new RedisPubSubCallback());
            RedisPubSubCommands<String, String> sync = redisConn.sync();
            if(context.getProperty(PATTERN).isSet() && context.getProperty(PATTERN).asBoolean()) {
                sync.punsubscribe(context.getProperty(CHANNEL).getValue());
            } else {
                sync.unsubscribe(context.getProperty(CHANNEL).getValue());
            }
        }
        isSubscribed = false;
        logger.info("Unsubscribed the Redis client.");
        clientConnectLock.writeLock().unlock();
    }

    @OnStopped
    public void onStopped(final ProcessContext context) throws IOException {
        if(redisQueue != null && !redisQueue.isEmpty() && processSessionFactory != null) {
            ProcessSession session = processSessionFactory.createSession();
            transferQueue(session);
            if(isCluster) {
                redisConn.close();
            } else {
                redisClusterConn.close();
            }
        } else {
            if (redisQueue!= null && !redisQueue.isEmpty()){
                throw new ProcessException("Stopping the processor but there is no ProcessSessionFactory stored and there are messages in the Redis internal queue. Removing the processor now will " +
                        "clear the queue but will result in DATA LOSS. This is normally due to starting the processor, receiving messages and stopping before the onTrigger happens. The messages " +
                        "in the Redis internal queue cannot finish processing until the processor is triggered to run.");
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (redisQueue.isEmpty() && scheduled.get() && !isSubscribed) {
            logger.info("Creating subscription to PubSub queue.");
            if(isCluster) {
                redisClusterConn.addListener(new RedisPubSubClusterCallback());
                RedisClusterPubSubCommands<String, String> sync = redisClusterConn.sync();
                if(context.getProperty(PATTERN).isSet() && context.getProperty(PATTERN).asBoolean()) {
                    sync.psubscribe(context.getProperty(CHANNEL).getValue());
                } else {
                    sync.subscribe(context.getProperty(CHANNEL).getValue());
                }
            } else {
                redisConn.addListener(new RedisPubSubCallback());
                RedisPubSubCommands<String, String> sync = redisConn.sync();
                if(context.getProperty(PATTERN).isSet() && context.getProperty(PATTERN).asBoolean()) {
                    sync.psubscribe(context.getProperty(CHANNEL).getValue());
                } else {
                    sync.subscribe(context.getProperty(CHANNEL).getValue());
                }
            }
            isSubscribed = true;
        }

        if (redisQueue.isEmpty()) {
            return;
        }

        transferQueue(session);
    }

    private void transferQueue(ProcessSession session){
        while (!redisQueue.isEmpty()) {
            FlowFile messageFlowfile = session.create();
            final RedisMessage redisMessage = redisQueue.peek();

            Map<String, String> attrs = new HashMap<>();
            attrs.put("redis.channel", redisMessage.getChannel());

            messageFlowfile = session.putAllAttributes(messageFlowfile, attrs);

            messageFlowfile = session.write(messageFlowfile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(redisMessage.getValue().getBytes("UTF-8"));
                }
            });

            String transitUri = redisMessage.getHostname() + ":" + redisMessage.getChannel();
            session.getProvenanceReporter().receive(messageFlowfile, transitUri);
            session.transfer(messageFlowfile, SUCCESS);
            session.commit();
            if (!redisQueue.remove(redisMessage) && logger.isWarnEnabled()) {
                logger.warn(new StringBuilder("FlowFile ")
                        .append(messageFlowfile.getAttribute(CoreAttributes.UUID.key()))
                        .append(" for Redis message ")
                        .append(redisMessage)
                        .append(" had already been removed from queue, possible duplication of flow files")
                        .toString());
            }
        }
    }

    private class RedisPubSubCallback extends RedisPubSubAdapter<String, String> {

        @Override
        public void message(String channel, String message) {
            if (logger.isDebugEnabled()) {
                    logger.debug("Message arrived from channel {}. Message: {}", new Object[] {channel, message});
            }
            redisQueue.add(new RedisMessage(channel, message));
        }

        @Override
        public void message(String pattern, String channel, String message) {
          if (logger.isDebugEnabled()) {
            logger.debug("Message arrived from channel {}. Message: {}", new Object[] {channel, message});
          }
          redisQueue.add(new RedisMessage(channel, message));
        }
    }

    private class RedisPubSubClusterCallback extends RedisClusterPubSubAdapter<String, String> {

        @Override
        public void message(RedisClusterNode node, String channel, String message) {
            if (logger.isDebugEnabled()) {
                logger.debug("Message arrived from channel {} and node {}. Message: {}", new Object[] {channel, node.getUri().toString(), message});
            }
            redisQueue.add(new RedisMessage(channel, message, node.getUri().toString()));
        }

        @Override
        public void message(RedisClusterNode node, String pattern, String channel, String message) {
          if (logger.isDebugEnabled()) {
            logger.debug("Message arrived from channel {} and node {}. Message: {}", new Object[] {channel, node.getUri().toString(), message});
          }
          redisQueue.add(new RedisMessage(channel, message, node.getUri().toString()));
        }
    }
}
