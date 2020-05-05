package org.apache.drill.exec.server.rest;

import java.net.SocketAddress;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.rpc.AbstractDisposableUserClientConnection;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.UserSession;

import io.netty.channel.ChannelFuture;

public abstract class BaseWebUserConnection extends AbstractDisposableUserClientConnection implements ConnectionThrottle {

  protected WebSessionResources webSessionResources;

  public BaseWebUserConnection(WebSessionResources webSessionResources) {
    this.webSessionResources = webSessionResources;
  }

  @Override
  public UserSession getSession() {
    return webSessionResources.getSession();
  }

  @Override
  public ChannelFuture getChannelClosureFuture() {
    return webSessionResources.getCloseFuture();
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return webSessionResources.getRemoteAddress();
  }

  @Override
  public void setAutoRead(boolean enableAutoRead) { }

  public WebSessionResources resources() {
    return webSessionResources;
  }

  protected String webDataType(MajorType majorType) {
    StringBuilder dataType = new StringBuilder(majorType.getMinorType().name());

    // For DECIMAL type
    if (majorType.hasPrecision()) {
      dataType.append("(");
      dataType.append(majorType.getPrecision());

      if (majorType.hasScale()) {
        dataType.append(", ");
        dataType.append(majorType.getScale());
      }

      dataType.append(")");
    } else if (majorType.hasWidth()) {
      // Case for VARCHAR columns with specified width
      dataType.append("(");
      dataType.append(majorType.getWidth());
      dataType.append(")");
    }
    return dataType.toString();
  }
}
