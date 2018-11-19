package es.ubika.nifi.processors.common;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.nifi.components.*;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractRedisProcessor extends AbstractSessionFactoryProcessor {

  protected ComponentLog logger;
  protected StatefulRedisPubSubConnection<String, String> redisConn;
  protected StatefulRedisClusterPubSubConnection<String, String> redisClusterConn;
  protected boolean isCluster = false;
  protected final ReadWriteLock clientConnectLock = new ReentrantReadWriteLock(true);

  public ProcessSessionFactory processSessionFactory;

  public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor
          .Builder().name("HOSTNAME")
          .displayName("Host Name")
          .description("Network Address of the Redis instance.")
          .required(true)
          .defaultValue("localhost")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  public static final PropertyDescriptor PORT = new PropertyDescriptor
          .Builder().name("PORT")
          .displayName("Port")
          .description("Port to access the Redis server.")
          .required(false)
          .defaultValue("6379")
          .addValidator(StandardValidators.PORT_VALIDATOR)
          .build();

  public static final PropertyDescriptor CHANNEL = new PropertyDescriptor
          .Builder().name("CHANNEL")
          .displayName("Channel")
          .description("Channel or pattern to subscribe to. If it's a pattern set to 'true' the pattern property.")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  public static final PropertyDescriptor PATTERN = new PropertyDescriptor
          .Builder().name("PATTERN")
          .displayName("Is Pattern")
          .description("Set to 'true' if the channel value is a pattern.")
          .required(false)
          .defaultValue("false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  public static final PropertyDescriptor DATABASE = new PropertyDescriptor
          .Builder().name("DATABASE")
          .displayName("Database")
          .description("Database to connect to.")
          .required(false)
          .defaultValue("0")
          .addValidator(StandardValidators.INTEGER_VALIDATOR)
          .build();

  public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
          .Builder().name("PASSWORD")
          .displayName("Password")
          .description("Password to connect to Redis in case authentication is enabled.")
          .required(false)
          .build();

  public static final PropertyDescriptor CLUSTER_MODE = new PropertyDescriptor
          .Builder().name("CLUSTER_MODE")
          .displayName("Cluster Mode")
          .description("Set to 'true' if Redis is configured in a Cluster")
          .required(false)
          .defaultValue("false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  public static List<PropertyDescriptor> getAbstractPropertyDescriptors(){
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(HOSTNAME);
    descriptors.add(PORT);
    descriptors.add(PASSWORD);
    descriptors.add(DATABASE);
    descriptors.add(CHANNEL);
    descriptors.add(PATTERN);
    descriptors.add(CLUSTER_MODE);
    return descriptors;
  }

  protected void buildClient(ProcessContext context){
    RedisURI.Builder uriBuilder = RedisURI.builder();
    uriBuilder.withHost(context.getProperty(HOSTNAME).getValue());
    if(context.getProperty(PORT).isSet()) {
      uriBuilder.withPort(context.getProperty(PORT).asInteger());
    }
    if(context.getProperty(PASSWORD).isSet()) {
      uriBuilder.withPassword(context.getProperty(PASSWORD).getValue());
    }
    if(context.getProperty(DATABASE).isSet()) {
      uriBuilder.withDatabase(context.getProperty(DATABASE).asInteger());
    }
    RedisURI redisURI = uriBuilder.build();
    clientConnectLock.writeLock().lock();
    try{
      if(context.getProperty(CLUSTER_MODE).isSet() && context.getProperty(CLUSTER_MODE).asBoolean()) {
        RedisClusterClient redisClient = RedisClusterClient.create(redisURI);
        redisClient.setOptions(ClusterClientOptions.builder().autoReconnect(true).build());
        isCluster = true;
        redisClusterConn = redisClient.connectPubSub();
      } else {
        RedisClient redisClient = RedisClient.create(redisURI);
        redisClient.setOptions(ClientOptions.builder().autoReconnect(true).build());
        isCluster = false;
        redisConn = redisClient.connectPubSub();
      }
    } finally {
      clientConnectLock.writeLock().unlock();
    }
  }

  @Override
  public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
    if (processSessionFactory == null) {
      processSessionFactory = sessionFactory;
    }
    ProcessSession session = sessionFactory.createSession();
    try {
      onTrigger(context, session);
      session.commit();
    } catch (final Throwable t) {
      getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
      session.rollback(true);
      throw t;
    }
  }

  public abstract void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;

}
