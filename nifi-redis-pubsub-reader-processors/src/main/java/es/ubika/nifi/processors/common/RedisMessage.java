package es.ubika.nifi.processors.common;

public class RedisMessage {

  private String channel;
  private String value;
  private String hostname;

  public RedisMessage(String channel, String value) {
    this.channel = channel;
    this.value = value;
  }

  public RedisMessage(String channel, String value, String hostname) {
    this.channel = channel;
    this.value = value;
    this.hostname = hostname;
  }

  public String getChannel() {
    return channel;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  @Override
  public String toString() {
    return "RedisMessage{" +
            "channel='" + channel + '\'' +
            ", value='" + value + '\'' +
            ", hostname='" + hostname + '\'' +
            '}';
  }
}
