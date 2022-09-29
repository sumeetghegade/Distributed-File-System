package qddfs;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.15.0)",
    comments = "Source: qddfs-1.proto")
public final class NameServerGrpc {

  private NameServerGrpc() {}

  public static final String SERVICE_NAME = "qddfs.NameServer";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.NSCreateRequest,
      qddfs.Qddfs1.NSCreateReply> getDoCreateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "doCreate",
      requestType = qddfs.Qddfs1.NSCreateRequest.class,
      responseType = qddfs.Qddfs1.NSCreateReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.NSCreateRequest,
      qddfs.Qddfs1.NSCreateReply> getDoCreateMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.NSCreateRequest, qddfs.Qddfs1.NSCreateReply> getDoCreateMethod;
    if ((getDoCreateMethod = NameServerGrpc.getDoCreateMethod) == null) {
      synchronized (NameServerGrpc.class) {
        if ((getDoCreateMethod = NameServerGrpc.getDoCreateMethod) == null) {
          NameServerGrpc.getDoCreateMethod = getDoCreateMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.NSCreateRequest, qddfs.Qddfs1.NSCreateReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.NameServer", "doCreate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSCreateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSCreateReply.getDefaultInstance()))
                  .setSchemaDescriptor(new NameServerMethodDescriptorSupplier("doCreate"))
                  .build();
          }
        }
     }
     return getDoCreateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.NSReadRequest,
      qddfs.Qddfs1.NSReadReply> getDoReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "doRead",
      requestType = qddfs.Qddfs1.NSReadRequest.class,
      responseType = qddfs.Qddfs1.NSReadReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.NSReadRequest,
      qddfs.Qddfs1.NSReadReply> getDoReadMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.NSReadRequest, qddfs.Qddfs1.NSReadReply> getDoReadMethod;
    if ((getDoReadMethod = NameServerGrpc.getDoReadMethod) == null) {
      synchronized (NameServerGrpc.class) {
        if ((getDoReadMethod = NameServerGrpc.getDoReadMethod) == null) {
          NameServerGrpc.getDoReadMethod = getDoReadMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.NSReadRequest, qddfs.Qddfs1.NSReadReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.NameServer", "doRead"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSReadReply.getDefaultInstance()))
                  .setSchemaDescriptor(new NameServerMethodDescriptorSupplier("doRead"))
                  .build();
          }
        }
     }
     return getDoReadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.NSDeleteRequest,
      qddfs.Qddfs1.NSDeleteReply> getDoDeleteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "doDelete",
      requestType = qddfs.Qddfs1.NSDeleteRequest.class,
      responseType = qddfs.Qddfs1.NSDeleteReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.NSDeleteRequest,
      qddfs.Qddfs1.NSDeleteReply> getDoDeleteMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.NSDeleteRequest, qddfs.Qddfs1.NSDeleteReply> getDoDeleteMethod;
    if ((getDoDeleteMethod = NameServerGrpc.getDoDeleteMethod) == null) {
      synchronized (NameServerGrpc.class) {
        if ((getDoDeleteMethod = NameServerGrpc.getDoDeleteMethod) == null) {
          NameServerGrpc.getDoDeleteMethod = getDoDeleteMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.NSDeleteRequest, qddfs.Qddfs1.NSDeleteReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.NameServer", "doDelete"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSDeleteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSDeleteReply.getDefaultInstance()))
                  .setSchemaDescriptor(new NameServerMethodDescriptorSupplier("doDelete"))
                  .build();
          }
        }
     }
     return getDoDeleteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.NSListRequest,
      qddfs.Qddfs1.NSListReply> getListMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "list",
      requestType = qddfs.Qddfs1.NSListRequest.class,
      responseType = qddfs.Qddfs1.NSListReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.NSListRequest,
      qddfs.Qddfs1.NSListReply> getListMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.NSListRequest, qddfs.Qddfs1.NSListReply> getListMethod;
    if ((getListMethod = NameServerGrpc.getListMethod) == null) {
      synchronized (NameServerGrpc.class) {
        if ((getListMethod = NameServerGrpc.getListMethod) == null) {
          NameServerGrpc.getListMethod = getListMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.NSListRequest, qddfs.Qddfs1.NSListReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.NameServer", "list"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSListRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSListReply.getDefaultInstance()))
                  .setSchemaDescriptor(new NameServerMethodDescriptorSupplier("list"))
                  .build();
          }
        }
     }
     return getListMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.NSRegisterRequest,
      qddfs.Qddfs1.NSRegisterReply> getRegisterFilesAndTombstonesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "registerFilesAndTombstones",
      requestType = qddfs.Qddfs1.NSRegisterRequest.class,
      responseType = qddfs.Qddfs1.NSRegisterReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.NSRegisterRequest,
      qddfs.Qddfs1.NSRegisterReply> getRegisterFilesAndTombstonesMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.NSRegisterRequest, qddfs.Qddfs1.NSRegisterReply> getRegisterFilesAndTombstonesMethod;
    if ((getRegisterFilesAndTombstonesMethod = NameServerGrpc.getRegisterFilesAndTombstonesMethod) == null) {
      synchronized (NameServerGrpc.class) {
        if ((getRegisterFilesAndTombstonesMethod = NameServerGrpc.getRegisterFilesAndTombstonesMethod) == null) {
          NameServerGrpc.getRegisterFilesAndTombstonesMethod = getRegisterFilesAndTombstonesMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.NSRegisterRequest, qddfs.Qddfs1.NSRegisterReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.NameServer", "registerFilesAndTombstones"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSRegisterRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSRegisterReply.getDefaultInstance()))
                  .setSchemaDescriptor(new NameServerMethodDescriptorSupplier("registerFilesAndTombstones"))
                  .build();
          }
        }
     }
     return getRegisterFilesAndTombstonesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.NSAddRequest,
      qddfs.Qddfs1.NSAddReply> getAddFileOrTombstoneMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "addFileOrTombstone",
      requestType = qddfs.Qddfs1.NSAddRequest.class,
      responseType = qddfs.Qddfs1.NSAddReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.NSAddRequest,
      qddfs.Qddfs1.NSAddReply> getAddFileOrTombstoneMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.NSAddRequest, qddfs.Qddfs1.NSAddReply> getAddFileOrTombstoneMethod;
    if ((getAddFileOrTombstoneMethod = NameServerGrpc.getAddFileOrTombstoneMethod) == null) {
      synchronized (NameServerGrpc.class) {
        if ((getAddFileOrTombstoneMethod = NameServerGrpc.getAddFileOrTombstoneMethod) == null) {
          NameServerGrpc.getAddFileOrTombstoneMethod = getAddFileOrTombstoneMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.NSAddRequest, qddfs.Qddfs1.NSAddReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.NameServer", "addFileOrTombstone"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSAddRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSAddReply.getDefaultInstance()))
                  .setSchemaDescriptor(new NameServerMethodDescriptorSupplier("addFileOrTombstone"))
                  .build();
          }
        }
     }
     return getAddFileOrTombstoneMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.NSBeatRequest,
      qddfs.Qddfs1.NSBeatReply> getHeartBeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "heartBeat",
      requestType = qddfs.Qddfs1.NSBeatRequest.class,
      responseType = qddfs.Qddfs1.NSBeatReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.NSBeatRequest,
      qddfs.Qddfs1.NSBeatReply> getHeartBeatMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.NSBeatRequest, qddfs.Qddfs1.NSBeatReply> getHeartBeatMethod;
    if ((getHeartBeatMethod = NameServerGrpc.getHeartBeatMethod) == null) {
      synchronized (NameServerGrpc.class) {
        if ((getHeartBeatMethod = NameServerGrpc.getHeartBeatMethod) == null) {
          NameServerGrpc.getHeartBeatMethod = getHeartBeatMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.NSBeatRequest, qddfs.Qddfs1.NSBeatReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.NameServer", "heartBeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSBeatRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.NSBeatReply.getDefaultInstance()))
                  .setSchemaDescriptor(new NameServerMethodDescriptorSupplier("heartBeat"))
                  .build();
          }
        }
     }
     return getHeartBeatMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static NameServerStub newStub(io.grpc.Channel channel) {
    return new NameServerStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static NameServerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new NameServerBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static NameServerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new NameServerFutureStub(channel);
  }

  /**
   */
  public static abstract class NameServerImplBase implements io.grpc.BindableService {

    /**
     */
    public void doCreate(qddfs.Qddfs1.NSCreateRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSCreateReply> responseObserver) {
      asyncUnimplementedUnaryCall(getDoCreateMethod(), responseObserver);
    }

    /**
     */
    public void doRead(qddfs.Qddfs1.NSReadRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSReadReply> responseObserver) {
      asyncUnimplementedUnaryCall(getDoReadMethod(), responseObserver);
    }

    /**
     */
    public void doDelete(qddfs.Qddfs1.NSDeleteRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSDeleteReply> responseObserver) {
      asyncUnimplementedUnaryCall(getDoDeleteMethod(), responseObserver);
    }

    /**
     */
    public void list(qddfs.Qddfs1.NSListRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSListReply> responseObserver) {
      asyncUnimplementedUnaryCall(getListMethod(), responseObserver);
    }

    /**
     */
    public void registerFilesAndTombstones(qddfs.Qddfs1.NSRegisterRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSRegisterReply> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterFilesAndTombstonesMethod(), responseObserver);
    }

    /**
     */
    public void addFileOrTombstone(qddfs.Qddfs1.NSAddRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSAddReply> responseObserver) {
      asyncUnimplementedUnaryCall(getAddFileOrTombstoneMethod(), responseObserver);
    }

    /**
     */
    public void heartBeat(qddfs.Qddfs1.NSBeatRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSBeatReply> responseObserver) {
      asyncUnimplementedUnaryCall(getHeartBeatMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getDoCreateMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.NSCreateRequest,
                qddfs.Qddfs1.NSCreateReply>(
                  this, METHODID_DO_CREATE)))
          .addMethod(
            getDoReadMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.NSReadRequest,
                qddfs.Qddfs1.NSReadReply>(
                  this, METHODID_DO_READ)))
          .addMethod(
            getDoDeleteMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.NSDeleteRequest,
                qddfs.Qddfs1.NSDeleteReply>(
                  this, METHODID_DO_DELETE)))
          .addMethod(
            getListMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.NSListRequest,
                qddfs.Qddfs1.NSListReply>(
                  this, METHODID_LIST)))
          .addMethod(
            getRegisterFilesAndTombstonesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.NSRegisterRequest,
                qddfs.Qddfs1.NSRegisterReply>(
                  this, METHODID_REGISTER_FILES_AND_TOMBSTONES)))
          .addMethod(
            getAddFileOrTombstoneMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.NSAddRequest,
                qddfs.Qddfs1.NSAddReply>(
                  this, METHODID_ADD_FILE_OR_TOMBSTONE)))
          .addMethod(
            getHeartBeatMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.NSBeatRequest,
                qddfs.Qddfs1.NSBeatReply>(
                  this, METHODID_HEART_BEAT)))
          .build();
    }
  }

  /**
   */
  public static final class NameServerStub extends io.grpc.stub.AbstractStub<NameServerStub> {
    private NameServerStub(io.grpc.Channel channel) {
      super(channel);
    }

    private NameServerStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NameServerStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new NameServerStub(channel, callOptions);
    }

    /**
     */
    public void doCreate(qddfs.Qddfs1.NSCreateRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSCreateReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDoCreateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void doRead(qddfs.Qddfs1.NSReadRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSReadReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDoReadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void doDelete(qddfs.Qddfs1.NSDeleteRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSDeleteReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDoDeleteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void list(qddfs.Qddfs1.NSListRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSListReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getListMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void registerFilesAndTombstones(qddfs.Qddfs1.NSRegisterRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSRegisterReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRegisterFilesAndTombstonesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void addFileOrTombstone(qddfs.Qddfs1.NSAddRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSAddReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAddFileOrTombstoneMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartBeat(qddfs.Qddfs1.NSBeatRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSBeatReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getHeartBeatMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class NameServerBlockingStub extends io.grpc.stub.AbstractStub<NameServerBlockingStub> {
    private NameServerBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private NameServerBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NameServerBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new NameServerBlockingStub(channel, callOptions);
    }

    /**
     */
    public qddfs.Qddfs1.NSCreateReply doCreate(qddfs.Qddfs1.NSCreateRequest request) {
      return blockingUnaryCall(
          getChannel(), getDoCreateMethod(), getCallOptions(), request);
    }

    /**
     */
    public qddfs.Qddfs1.NSReadReply doRead(qddfs.Qddfs1.NSReadRequest request) {
      return blockingUnaryCall(
          getChannel(), getDoReadMethod(), getCallOptions(), request);
    }

    /**
     */
    public qddfs.Qddfs1.NSDeleteReply doDelete(qddfs.Qddfs1.NSDeleteRequest request) {
      return blockingUnaryCall(
          getChannel(), getDoDeleteMethod(), getCallOptions(), request);
    }

    /**
     */
    public qddfs.Qddfs1.NSListReply list(qddfs.Qddfs1.NSListRequest request) {
      return blockingUnaryCall(
          getChannel(), getListMethod(), getCallOptions(), request);
    }

    /**
     */
    public qddfs.Qddfs1.NSRegisterReply registerFilesAndTombstones(qddfs.Qddfs1.NSRegisterRequest request) {
      return blockingUnaryCall(
          getChannel(), getRegisterFilesAndTombstonesMethod(), getCallOptions(), request);
    }

    /**
     */
    public qddfs.Qddfs1.NSAddReply addFileOrTombstone(qddfs.Qddfs1.NSAddRequest request) {
      return blockingUnaryCall(
          getChannel(), getAddFileOrTombstoneMethod(), getCallOptions(), request);
    }

    /**
     */
    public qddfs.Qddfs1.NSBeatReply heartBeat(qddfs.Qddfs1.NSBeatRequest request) {
      return blockingUnaryCall(
          getChannel(), getHeartBeatMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class NameServerFutureStub extends io.grpc.stub.AbstractStub<NameServerFutureStub> {
    private NameServerFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private NameServerFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NameServerFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new NameServerFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.NSCreateReply> doCreate(
        qddfs.Qddfs1.NSCreateRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDoCreateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.NSReadReply> doRead(
        qddfs.Qddfs1.NSReadRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDoReadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.NSDeleteReply> doDelete(
        qddfs.Qddfs1.NSDeleteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDoDeleteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.NSListReply> list(
        qddfs.Qddfs1.NSListRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getListMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.NSRegisterReply> registerFilesAndTombstones(
        qddfs.Qddfs1.NSRegisterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRegisterFilesAndTombstonesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.NSAddReply> addFileOrTombstone(
        qddfs.Qddfs1.NSAddRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getAddFileOrTombstoneMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.NSBeatReply> heartBeat(
        qddfs.Qddfs1.NSBeatRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getHeartBeatMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DO_CREATE = 0;
  private static final int METHODID_DO_READ = 1;
  private static final int METHODID_DO_DELETE = 2;
  private static final int METHODID_LIST = 3;
  private static final int METHODID_REGISTER_FILES_AND_TOMBSTONES = 4;
  private static final int METHODID_ADD_FILE_OR_TOMBSTONE = 5;
  private static final int METHODID_HEART_BEAT = 6;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final NameServerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(NameServerImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DO_CREATE:
          serviceImpl.doCreate((qddfs.Qddfs1.NSCreateRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSCreateReply>) responseObserver);
          break;
        case METHODID_DO_READ:
          serviceImpl.doRead((qddfs.Qddfs1.NSReadRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSReadReply>) responseObserver);
          break;
        case METHODID_DO_DELETE:
          serviceImpl.doDelete((qddfs.Qddfs1.NSDeleteRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSDeleteReply>) responseObserver);
          break;
        case METHODID_LIST:
          serviceImpl.list((qddfs.Qddfs1.NSListRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSListReply>) responseObserver);
          break;
        case METHODID_REGISTER_FILES_AND_TOMBSTONES:
          serviceImpl.registerFilesAndTombstones((qddfs.Qddfs1.NSRegisterRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSRegisterReply>) responseObserver);
          break;
        case METHODID_ADD_FILE_OR_TOMBSTONE:
          serviceImpl.addFileOrTombstone((qddfs.Qddfs1.NSAddRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSAddReply>) responseObserver);
          break;
        case METHODID_HEART_BEAT:
          serviceImpl.heartBeat((qddfs.Qddfs1.NSBeatRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.NSBeatReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class NameServerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    NameServerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return qddfs.Qddfs1.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("nameServer");
    }
  }

  private static final class NameServerFileDescriptorSupplier
      extends NameServerBaseDescriptorSupplier {
    NameServerFileDescriptorSupplier() {}
  }

  private static final class NameServerMethodDescriptorSupplier
      extends NameServerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    NameServerMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (NameServerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new NameServerFileDescriptorSupplier())
              .addMethod(getDoCreateMethod())
              .addMethod(getDoReadMethod())
              .addMethod(getDoDeleteMethod())
              .addMethod(getListMethod())
              .addMethod(getRegisterFilesAndTombstonesMethod())
              .addMethod(getAddFileOrTombstoneMethod())
              .addMethod(getHeartBeatMethod())
              .build();
        }
      }
    }
    return result;
  }
}
