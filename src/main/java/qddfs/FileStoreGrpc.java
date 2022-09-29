package qddfs;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.15.0)",
    comments = "Source: qddfs-1.proto")
public final class FileStoreGrpc {

  private FileStoreGrpc() {}

  public static final String SERVICE_NAME = "qddfs.FileStore";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.CreateFileRequest,
      qddfs.Qddfs1.CreateFileReply> getCreateFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "createFile",
      requestType = qddfs.Qddfs1.CreateFileRequest.class,
      responseType = qddfs.Qddfs1.CreateFileReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.CreateFileRequest,
      qddfs.Qddfs1.CreateFileReply> getCreateFileMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.CreateFileRequest, qddfs.Qddfs1.CreateFileReply> getCreateFileMethod;
    if ((getCreateFileMethod = FileStoreGrpc.getCreateFileMethod) == null) {
      synchronized (FileStoreGrpc.class) {
        if ((getCreateFileMethod = FileStoreGrpc.getCreateFileMethod) == null) {
          FileStoreGrpc.getCreateFileMethod = getCreateFileMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.CreateFileRequest, qddfs.Qddfs1.CreateFileReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.FileStore", "createFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.CreateFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.CreateFileReply.getDefaultInstance()))
                  .setSchemaDescriptor(new FileStoreMethodDescriptorSupplier("createFile"))
                  .build();
          }
        }
     }
     return getCreateFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.ReadFileRequest,
      qddfs.Qddfs1.ReadFileReply> getReadFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "readFile",
      requestType = qddfs.Qddfs1.ReadFileRequest.class,
      responseType = qddfs.Qddfs1.ReadFileReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.ReadFileRequest,
      qddfs.Qddfs1.ReadFileReply> getReadFileMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.ReadFileRequest, qddfs.Qddfs1.ReadFileReply> getReadFileMethod;
    if ((getReadFileMethod = FileStoreGrpc.getReadFileMethod) == null) {
      synchronized (FileStoreGrpc.class) {
        if ((getReadFileMethod = FileStoreGrpc.getReadFileMethod) == null) {
          FileStoreGrpc.getReadFileMethod = getReadFileMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.ReadFileRequest, qddfs.Qddfs1.ReadFileReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.FileStore", "readFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.ReadFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.ReadFileReply.getDefaultInstance()))
                  .setSchemaDescriptor(new FileStoreMethodDescriptorSupplier("readFile"))
                  .build();
          }
        }
     }
     return getReadFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.DeleteFileRequest,
      qddfs.Qddfs1.DeleteFileReply> getDeleteFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "deleteFile",
      requestType = qddfs.Qddfs1.DeleteFileRequest.class,
      responseType = qddfs.Qddfs1.DeleteFileReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.DeleteFileRequest,
      qddfs.Qddfs1.DeleteFileReply> getDeleteFileMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.DeleteFileRequest, qddfs.Qddfs1.DeleteFileReply> getDeleteFileMethod;
    if ((getDeleteFileMethod = FileStoreGrpc.getDeleteFileMethod) == null) {
      synchronized (FileStoreGrpc.class) {
        if ((getDeleteFileMethod = FileStoreGrpc.getDeleteFileMethod) == null) {
          FileStoreGrpc.getDeleteFileMethod = getDeleteFileMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.DeleteFileRequest, qddfs.Qddfs1.DeleteFileReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.FileStore", "deleteFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.DeleteFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.DeleteFileReply.getDefaultInstance()))
                  .setSchemaDescriptor(new FileStoreMethodDescriptorSupplier("deleteFile"))
                  .build();
          }
        }
     }
     return getDeleteFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.ListRequest,
      qddfs.Qddfs1.ListReply> getListMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "list",
      requestType = qddfs.Qddfs1.ListRequest.class,
      responseType = qddfs.Qddfs1.ListReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.ListRequest,
      qddfs.Qddfs1.ListReply> getListMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.ListRequest, qddfs.Qddfs1.ListReply> getListMethod;
    if ((getListMethod = FileStoreGrpc.getListMethod) == null) {
      synchronized (FileStoreGrpc.class) {
        if ((getListMethod = FileStoreGrpc.getListMethod) == null) {
          FileStoreGrpc.getListMethod = getListMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.ListRequest, qddfs.Qddfs1.ListReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.FileStore", "list"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.ListRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.ListReply.getDefaultInstance()))
                  .setSchemaDescriptor(new FileStoreMethodDescriptorSupplier("list"))
                  .build();
          }
        }
     }
     return getListMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.CopyFileRequest,
      qddfs.Qddfs1.CopyFileReply> getCopyFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "copyFile",
      requestType = qddfs.Qddfs1.CopyFileRequest.class,
      responseType = qddfs.Qddfs1.CopyFileReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.CopyFileRequest,
      qddfs.Qddfs1.CopyFileReply> getCopyFileMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.CopyFileRequest, qddfs.Qddfs1.CopyFileReply> getCopyFileMethod;
    if ((getCopyFileMethod = FileStoreGrpc.getCopyFileMethod) == null) {
      synchronized (FileStoreGrpc.class) {
        if ((getCopyFileMethod = FileStoreGrpc.getCopyFileMethod) == null) {
          FileStoreGrpc.getCopyFileMethod = getCopyFileMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.CopyFileRequest, qddfs.Qddfs1.CopyFileReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.FileStore", "copyFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.CopyFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.CopyFileReply.getDefaultInstance()))
                  .setSchemaDescriptor(new FileStoreMethodDescriptorSupplier("copyFile"))
                  .build();
          }
        }
     }
     return getCopyFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<qddfs.Qddfs1.BumpVersionRequest,
      qddfs.Qddfs1.BumpVersionReply> getBumpVersionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "bumpVersion",
      requestType = qddfs.Qddfs1.BumpVersionRequest.class,
      responseType = qddfs.Qddfs1.BumpVersionReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<qddfs.Qddfs1.BumpVersionRequest,
      qddfs.Qddfs1.BumpVersionReply> getBumpVersionMethod() {
    io.grpc.MethodDescriptor<qddfs.Qddfs1.BumpVersionRequest, qddfs.Qddfs1.BumpVersionReply> getBumpVersionMethod;
    if ((getBumpVersionMethod = FileStoreGrpc.getBumpVersionMethod) == null) {
      synchronized (FileStoreGrpc.class) {
        if ((getBumpVersionMethod = FileStoreGrpc.getBumpVersionMethod) == null) {
          FileStoreGrpc.getBumpVersionMethod = getBumpVersionMethod = 
              io.grpc.MethodDescriptor.<qddfs.Qddfs1.BumpVersionRequest, qddfs.Qddfs1.BumpVersionReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "qddfs.FileStore", "bumpVersion"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.BumpVersionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  qddfs.Qddfs1.BumpVersionReply.getDefaultInstance()))
                  .setSchemaDescriptor(new FileStoreMethodDescriptorSupplier("bumpVersion"))
                  .build();
          }
        }
     }
     return getBumpVersionMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FileStoreStub newStub(io.grpc.Channel channel) {
    return new FileStoreStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FileStoreBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new FileStoreBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FileStoreFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new FileStoreFutureStub(channel);
  }

  /**
   */
  public static abstract class FileStoreImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<qddfs.Qddfs1.CreateFileRequest> createFile(
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.CreateFileReply> responseObserver) {
      return asyncUnimplementedStreamingCall(getCreateFileMethod(), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<qddfs.Qddfs1.ReadFileRequest> readFile(
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.ReadFileReply> responseObserver) {
      return asyncUnimplementedStreamingCall(getReadFileMethod(), responseObserver);
    }

    /**
     */
    public void deleteFile(qddfs.Qddfs1.DeleteFileRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.DeleteFileReply> responseObserver) {
      asyncUnimplementedUnaryCall(getDeleteFileMethod(), responseObserver);
    }

    /**
     */
    public void list(qddfs.Qddfs1.ListRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.ListReply> responseObserver) {
      asyncUnimplementedUnaryCall(getListMethod(), responseObserver);
    }

    /**
     */
    public void copyFile(qddfs.Qddfs1.CopyFileRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.CopyFileReply> responseObserver) {
      asyncUnimplementedUnaryCall(getCopyFileMethod(), responseObserver);
    }

    /**
     */
    public void bumpVersion(qddfs.Qddfs1.BumpVersionRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.BumpVersionReply> responseObserver) {
      asyncUnimplementedUnaryCall(getBumpVersionMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCreateFileMethod(),
            asyncClientStreamingCall(
              new MethodHandlers<
                qddfs.Qddfs1.CreateFileRequest,
                qddfs.Qddfs1.CreateFileReply>(
                  this, METHODID_CREATE_FILE)))
          .addMethod(
            getReadFileMethod(),
            asyncBidiStreamingCall(
              new MethodHandlers<
                qddfs.Qddfs1.ReadFileRequest,
                qddfs.Qddfs1.ReadFileReply>(
                  this, METHODID_READ_FILE)))
          .addMethod(
            getDeleteFileMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.DeleteFileRequest,
                qddfs.Qddfs1.DeleteFileReply>(
                  this, METHODID_DELETE_FILE)))
          .addMethod(
            getListMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.ListRequest,
                qddfs.Qddfs1.ListReply>(
                  this, METHODID_LIST)))
          .addMethod(
            getCopyFileMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.CopyFileRequest,
                qddfs.Qddfs1.CopyFileReply>(
                  this, METHODID_COPY_FILE)))
          .addMethod(
            getBumpVersionMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                qddfs.Qddfs1.BumpVersionRequest,
                qddfs.Qddfs1.BumpVersionReply>(
                  this, METHODID_BUMP_VERSION)))
          .build();
    }
  }

  /**
   */
  public static final class FileStoreStub extends io.grpc.stub.AbstractStub<FileStoreStub> {
    private FileStoreStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileStoreStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileStoreStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileStoreStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<qddfs.Qddfs1.CreateFileRequest> createFile(
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.CreateFileReply> responseObserver) {
      return asyncClientStreamingCall(
          getChannel().newCall(getCreateFileMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<qddfs.Qddfs1.ReadFileRequest> readFile(
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.ReadFileReply> responseObserver) {
      return asyncBidiStreamingCall(
          getChannel().newCall(getReadFileMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void deleteFile(qddfs.Qddfs1.DeleteFileRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.DeleteFileReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDeleteFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void list(qddfs.Qddfs1.ListRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.ListReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getListMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void copyFile(qddfs.Qddfs1.CopyFileRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.CopyFileReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCopyFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void bumpVersion(qddfs.Qddfs1.BumpVersionRequest request,
        io.grpc.stub.StreamObserver<qddfs.Qddfs1.BumpVersionReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getBumpVersionMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class FileStoreBlockingStub extends io.grpc.stub.AbstractStub<FileStoreBlockingStub> {
    private FileStoreBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileStoreBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileStoreBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileStoreBlockingStub(channel, callOptions);
    }

    /**
     */
    public qddfs.Qddfs1.DeleteFileReply deleteFile(qddfs.Qddfs1.DeleteFileRequest request) {
      return blockingUnaryCall(
          getChannel(), getDeleteFileMethod(), getCallOptions(), request);
    }

    /**
     */
    public qddfs.Qddfs1.ListReply list(qddfs.Qddfs1.ListRequest request) {
      return blockingUnaryCall(
          getChannel(), getListMethod(), getCallOptions(), request);
    }

    /**
     */
    public qddfs.Qddfs1.CopyFileReply copyFile(qddfs.Qddfs1.CopyFileRequest request) {
      return blockingUnaryCall(
          getChannel(), getCopyFileMethod(), getCallOptions(), request);
    }

    /**
     */
    public qddfs.Qddfs1.BumpVersionReply bumpVersion(qddfs.Qddfs1.BumpVersionRequest request) {
      return blockingUnaryCall(
          getChannel(), getBumpVersionMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class FileStoreFutureStub extends io.grpc.stub.AbstractStub<FileStoreFutureStub> {
    private FileStoreFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileStoreFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileStoreFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileStoreFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.DeleteFileReply> deleteFile(
        qddfs.Qddfs1.DeleteFileRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDeleteFileMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.ListReply> list(
        qddfs.Qddfs1.ListRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getListMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.CopyFileReply> copyFile(
        qddfs.Qddfs1.CopyFileRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCopyFileMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<qddfs.Qddfs1.BumpVersionReply> bumpVersion(
        qddfs.Qddfs1.BumpVersionRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getBumpVersionMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DELETE_FILE = 0;
  private static final int METHODID_LIST = 1;
  private static final int METHODID_COPY_FILE = 2;
  private static final int METHODID_BUMP_VERSION = 3;
  private static final int METHODID_CREATE_FILE = 4;
  private static final int METHODID_READ_FILE = 5;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final FileStoreImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(FileStoreImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DELETE_FILE:
          serviceImpl.deleteFile((qddfs.Qddfs1.DeleteFileRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.DeleteFileReply>) responseObserver);
          break;
        case METHODID_LIST:
          serviceImpl.list((qddfs.Qddfs1.ListRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.ListReply>) responseObserver);
          break;
        case METHODID_COPY_FILE:
          serviceImpl.copyFile((qddfs.Qddfs1.CopyFileRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.CopyFileReply>) responseObserver);
          break;
        case METHODID_BUMP_VERSION:
          serviceImpl.bumpVersion((qddfs.Qddfs1.BumpVersionRequest) request,
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.BumpVersionReply>) responseObserver);
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
        case METHODID_CREATE_FILE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.createFile(
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.CreateFileReply>) responseObserver);
        case METHODID_READ_FILE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.readFile(
              (io.grpc.stub.StreamObserver<qddfs.Qddfs1.ReadFileReply>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class FileStoreBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FileStoreBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return qddfs.Qddfs1.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FileStore");
    }
  }

  private static final class FileStoreFileDescriptorSupplier
      extends FileStoreBaseDescriptorSupplier {
    FileStoreFileDescriptorSupplier() {}
  }

  private static final class FileStoreMethodDescriptorSupplier
      extends FileStoreBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    FileStoreMethodDescriptorSupplier(String methodName) {
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
      synchronized (FileStoreGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FileStoreFileDescriptorSupplier())
              .addMethod(getCreateFileMethod())
              .addMethod(getReadFileMethod())
              .addMethod(getDeleteFileMethod())
              .addMethod(getListMethod())
              .addMethod(getCopyFileMethod())
              .addMethod(getBumpVersionMethod())
              .build();
        }
      }
    }
    return result;
  }
}
