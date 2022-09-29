package fileStore;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import qddfs.FileStoreGrpc;
import qddfs.Qddfs1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


public class FileStore extends FileStoreGrpc.FileStoreImplBase {


    @Override
    public StreamObserver<Qddfs1.CreateFileRequest> createFile(StreamObserver<Qddfs1.CreateFileReply> responseObserver) {

        return new StreamObserver<Qddfs1.CreateFileRequest>() {
            final CountDownLatch latch = new CountDownLatch(1);
            int rc = 1;
            File myCreatedFile;
            RandomAccessFile fileAccess;
            FileChannel fChannel;
            ManagedChannel channel;
            StreamObserver<Qddfs1.CreateFileRequest> successorRequestObserver;
            String fileName;
            int newVersion;
            boolean haveChild = false;
            FileStoreGrpc.FileStoreStub stub;
            String message = "";
            String fileNameNoPort = null;

            @Override
            public void onNext(Qddfs1.CreateFileRequest req) {


                //---****CREATE****---
                if (req.hasCreate()) {
                    System.out.println("Received create request for:");
                    //1. Get all file information
                    Qddfs1.FileCreate fileInfo = req.getCreate();
                    fileName = fileInfo.getName();
                    if (fileName.contains(":") || fileName.contains("/"))
                        fileNameNoPort = fileName.replaceAll("[^a-zA-Z0-9]", "");
                    else
                        fileNameNoPort = fileName;
                    newVersion = fileInfo.getVersion();

                    System.out.println(fileName + " " + newVersion);

                    //2. Get replica list
                    ProtocolStringList list = fileInfo.getChainList();
                    List<String> replicaList = new ArrayList<String>();
                    for (String a : list) {
                        replicaList.add(a);
                    }
                    //2.1 Remove my own ip from the list if it is present
                    replicaList.remove(FSManager.ipAddr);


                    System.out.println("ChainList without successor: " + replicaList);

                    //3. Check if file already exists using metadata
                    if (FSManager.metaData.containsKey(fileName)) {
                        System.out.println("File exists in metadata");
                        int currVersion = Integer.parseInt(FSManager.metaData.get(fileName));
                        System.out.println("My version: " + currVersion);
                        System.out.println("version received: " + newVersion);
                        //4.1 Check if older version, if yes --> return with rc = 1
                        if (newVersion < currVersion) {
                            System.out.println("Old version received");
                            rc = 1;
                            onCompleted();
                            return;
                        }

                        //4.2 Backup existing copy of file
                        else {
                            System.out.println("Backup existing file");
                            String filePath = FSManager.dataLoc + fileNameNoPort;
                            File existingFile = new File(filePath);
                            File renamedFile = new File(filePath + "_copy");
                            existingFile.renameTo(renamedFile);
                        }
                    }
                    //5. Check if file has already been deleted
                    else if (FSManager.deletedFilesData.containsKey(fileName)) {
                        System.out.println("File has already been deleted, checking version");
                        int deletedFileVersion = Integer.parseInt(FSManager.deletedFilesData.get(fileName));
                        if (newVersion <= deletedFileVersion) {
                            System.out.println("Old version received for deleted file");
                            rc = 1;
                            message = "File already deleted. Check version number";
                            if (FSManager.deletedFilesData.containsKey(fileName)) {
                                FSManager.deletedFilesData.remove(fileName);
                                FSManager.setDeletedFilesData();
                            }
                            onCompleted();
                            return;
                        }
                    }


                    //6. If I am here:: All good, create the file
                    System.out.println("Start file creation");
                    String filePath = FSManager.dataLoc + fileNameNoPort;
                    myCreatedFile = new File(filePath);
                    try {
                        myCreatedFile.createNewFile();
                        System.out.println("file created");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    //7. If replicas present send create request to next replica
                    haveChild = replicaList.size() != 0;
                    if (haveChild) {
                        System.out.println("I have nodes in front of me");
                        String mySuccessor = replicaList.get(0);
                        System.out.println("MySuccessor: " + mySuccessor);
                        replicaList.remove(mySuccessor);

                        channel = ManagedChannelBuilder.forTarget(mySuccessor).usePlaintext().build();
                        stub = FileStoreGrpc.newStub(channel);
                        successorRequestObserver = observeReplica(channel);
                        Qddfs1.FileCreate create = Qddfs1.FileCreate.newBuilder().addAllChain(replicaList).setVersion(newVersion).setName(fileName).build();
                        successorRequestObserver.onNext(Qddfs1.CreateFileRequest.newBuilder().setCreate(create).build());
                        System.out.println("Sent create request to successor");
                    }
                    // I am last or single node
                    else {
                        System.out.println("No nodes in front of me");
                        latch.countDown();
                    }
                }


                //---****DATA****---
                if (req.hasData()) {
                    System.out.println("Sending data chunk");
                    Qddfs1.FileData fileData = req.getData();
                    long offSet = fileData.getOffset();
                    ByteString data = fileData.getData();
                    try {
                        fileAccess = new RandomAccessFile(myCreatedFile, "rw");
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    fChannel = fileAccess.getChannel();
                    byte[] arr = data.toByteArray();
                    try {
                        fChannel.write(ByteBuffer.wrap(arr), offSet);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (haveChild) {
                        successorRequestObserver.onNext(Qddfs1.CreateFileRequest.newBuilder().setData(Qddfs1.FileData.newBuilder().setData(data).setOffset(offSet)).build());
                    }


                }

                //---****CLOSE****---
                if (req.hasClose()) {
                    System.out.println("Received close");
                    if (!haveChild) {
                        rc = 0;
                    } else {
                        System.out.println("Sending close to successor");
                        Qddfs1.FileClose fileClose = Qddfs1.FileClose.newBuilder().build();
                        successorRequestObserver.onNext(Qddfs1.CreateFileRequest.newBuilder().setClose(fileClose).build());
                        successorRequestObserver.onCompleted();
                    }
                }
            }


            private StreamObserver<Qddfs1.CreateFileRequest> observeReplica(ManagedChannel channel) {

                StreamObserver<Qddfs1.CreateFileRequest> requestObserver = FileStoreGrpc.newStub(channel)
                        .createFile(new StreamObserver<Qddfs1.CreateFileReply>() {
                            int successorRC = 2;

                            @Override
                            public void onNext(Qddfs1.CreateFileReply value) {
                                successorRC = value.getRc();
                            }

                            @Override
                            public void onError(Throwable t) {
                                t.printStackTrace();
                                successorRC = 2;
                                latch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                rc = successorRC;
                                latch.countDown();
                            }
                        });
                return requestObserver;
            }

            @Override
            public void onError(Throwable t) {
                Qddfs1.CreateFileReply.Builder resp = Qddfs1.CreateFileReply.newBuilder();
                resp.setRc(rc);
                responseObserver.onNext(resp.build());
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                System.out.println("In oncompleted");
                if (haveChild) {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                Qddfs1.CreateFileReply.Builder resp = Qddfs1.CreateFileReply.newBuilder();
                System.out.println("RC: " + rc);
                if (rc == 0) {
                    //Save metadata
                    System.out.println("Saving metadata");
                    FSManager.metaData.put(fileName, String.valueOf(newVersion));
                    try {
                        FSManager.fileSizeData.put(fileName, String.valueOf(fileAccess.length()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    FSManager.setMetaData();
                    FSManager.setFileSizeData();
                    System.out.println("Saved metadata");

                    message = "File successfully created";

                    if (FSManager.isNSAvailable) {
                        System.out.println("Telling NS to add file in metadata");
                        try {
                            Qddfs1.NSAddRequest request = Qddfs1.NSAddRequest.newBuilder().setHostPort(FSManager.ipAddr).setEntry(Qddfs1.FSEntry.newBuilder().setName(fileName).setVersion(newVersion).setSize(Long.parseLong(FSManager.fileSizeData.get(fileName))).setIsTombstone(false).build()).build();
                            Qddfs1.NSAddReply reply = FSManager.nameServer.addFileOrTombstone(request);
                            System.out.println("Received rc: " + reply.getRc());
                        } catch (Exception e) {
                            System.out.println("ERROR: Add file to NS failed");
                            e.printStackTrace();
                        }

                    }


                    File backupFile = new File(FSManager.dataLoc + fileNameNoPort + "_copy");
                    if (backupFile.exists()) {
                        System.out.println("Deleted backup file");
                        backupFile.delete();
                    }
                }
                if (rc == 1) {
                    System.out.println("Older version of file received... ");
                    File backupFile = new File(FSManager.dataLoc + fileNameNoPort + "_copy");
                    if (backupFile.exists()) {
                        File restoredFile = new File(FSManager.dataLoc + fileNameNoPort);
                        backupFile.renameTo(restoredFile);
                    }
                    message = "Older version of file received";
                }
                if (rc == 2) {
                    if (myCreatedFile != null && myCreatedFile.exists())
                        myCreatedFile.delete();
                    File backupFile = new File(FSManager.dataLoc + fileNameNoPort + "_copy");
                    if (backupFile.exists()) {
                        System.out.println("Restoring backup file");
                        File restoredFile = new File(FSManager.dataLoc + fileNameNoPort);
                        backupFile.renameTo(restoredFile);
                    }
                    if (message.isEmpty())
                        message = "Some error occurred";
                }

                if (fChannel != null && fileAccess != null) {
                    try {
                        System.out.println("Closing channel");
                        fChannel.close();
                        fileAccess.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (channel != null)
                    channel.shutdown();
                resp.setRc(rc);
                resp.setMessage(message);
                responseObserver.onNext(resp.build());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<Qddfs1.ReadFileRequest> readFile(StreamObserver<Qddfs1.ReadFileReply> responseObserver) {

        return new StreamObserver<Qddfs1.ReadFileRequest>() {
            Qddfs1.OpenResult openRes;
            String fileName;
            String fileNameNoPort;
            FileChannel fChannel;
            RandomAccessFile fileAccess;

            long offset;


            @Override
            public void onNext(Qddfs1.ReadFileRequest value) {
                System.out.println("Starting read");
                if (value.hasRead()) {
                    System.out.println("in hasRead");
                    Qddfs1.FileRead fileInfo = value.getRead();
                    fileName = fileInfo.getName();
                    if (fileName.contains(":") || fileName.contains("/"))
                        fileNameNoPort = fileName.replaceAll("[^a-zA-Z0-9]", "");
                    else
                        fileNameNoPort = fileName;
                    System.out.println("Filename received: " + fileName);
                    if (FSManager.metaData.containsKey(fileName)) {
                        System.out.println("File exists, now reading");
                        String filePath = FSManager.dataLoc + fileNameNoPort;
                        try {
                            fileAccess = new RandomAccessFile(filePath, "rw");
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }

                    } else if (FSManager.deletedFilesData.containsKey(fileName)) {
                        System.out.println("You are trying to read a deleted file");
                        openRes = Qddfs1.OpenResult.newBuilder().setRc(1).setVersion(Integer.parseInt(FSManager.deletedFilesData.get(fileName))).setError("File has been deleted").setLength(-1).build();
                        responseObserver.onNext(Qddfs1.ReadFileReply.newBuilder().setOpen(openRes).build());
                        responseObserver.onCompleted();
                        return;
                    } else {
                        System.out.println("File not found");
                        openRes = Qddfs1.OpenResult.newBuilder().setRc(1).setVersion(-1).setError("File not found").setLength(-1).build();
                        responseObserver.onNext(Qddfs1.ReadFileReply.newBuilder().setOpen(openRes).build());
                        responseObserver.onCompleted();
                        return;
                    }
                    try {
                        openRes = Qddfs1.OpenResult.newBuilder().setRc(0).setVersion(Integer.parseInt(FSManager.metaData.get(fileName))).setLength(fileAccess.length()).build();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    responseObserver.onNext(Qddfs1.ReadFileReply.newBuilder().setOpen(openRes).build());


                }
                if (value.hasReq()) {
                    System.out.println("Received req");
                    Qddfs1.ReadRequest req = value.getReq();
                    int length = req.getLength();
                    offset = req.getOffset();
                    System.out.println("Received req... offset: " + offset);

                    if (FSManager.metaData.containsKey(fileName)) {
                        fChannel = fileAccess.getChannel();
                        ByteBuffer bf = ByteBuffer.allocate(length);
                        try {
                            int writtenDataLen = fChannel.read(bf, offset);

                            bf.flip();
                            Qddfs1.FileData data = Qddfs1.FileData.newBuilder().setData(ByteString.copyFrom(bf)).setOffset(offset).build();
                            responseObserver.onNext(Qddfs1.ReadFileReply.newBuilder().setData(data).build());

                            if (writtenDataLen != length)
                                responseObserver.onCompleted();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

                if (value.hasClose()) {
                    System.out.println("Received close");
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                if (fChannel != null && fileAccess != null) {
                    try {
                        fChannel.close();
                        fileAccess.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                responseObserver.onCompleted();
            }
        };
    }


    @Override
    public void deleteFile(Qddfs1.DeleteFileRequest request, StreamObserver<Qddfs1.DeleteFileReply> responseObserver) {
        System.out.println("In delete");
        String message = null;
        String fileName = request.getName();
        String fileNameNoPort;
        if (fileName.contains(":") || fileName.contains("/"))
            fileNameNoPort = fileName.replaceAll("[^a-zA-Z0-9]", "");
        else
            fileNameNoPort = fileName;
        int version = request.getVersion();
        int rc = 0;
        Qddfs1.DeleteFileReply.Builder reply = Qddfs1.DeleteFileReply.newBuilder();
        if (!FSManager.metaData.containsKey(fileName) && FSManager.metaData.containsKey(fileName)) {
            System.out.println("File exists");
            if (Integer.parseInt(FSManager.metaData.get(fileName)) > version) {
                System.out.println("You are trying to delete an older version of the file");
                rc = 1;
                message = "You are trying to delete an older version of the file";
            } else {
                System.out.println("Deleting file");
                String filePath = FSManager.dataLoc + fileNameNoPort;
                File file = new File(filePath);
                //Deleting file
                file.delete();
                FSManager.metaData.remove(fileName);
                FSManager.setMetaData();

                FSManager.fileSizeData.remove(fileName);
                FSManager.setFileSizeData();
                //tombstone handle
                FSManager.deletedFilesData.put(fileName, String.valueOf(version));
                FSManager.setDeletedFilesData();
                rc = 0;


                if (FSManager.isNSAvailable) {
                    try {
                        Qddfs1.NSAddRequest addTombStoneRequest = Qddfs1.NSAddRequest.newBuilder().setHostPort(FSManager.ipAddr).setEntry(Qddfs1.FSEntry.newBuilder().setName(fileName).setVersion(version).setSize(0).setIsTombstone(true).build()).build();
                        Qddfs1.NSAddReply addTombStonereply = FSManager.nameServer.addFileOrTombstone(addTombStoneRequest);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
                message = "File deleted successfully";
            }
        } else {
            System.out.println("File does not exist");
            if (FSManager.deletedFilesData.containsKey(fileName)) {
                rc = 1;
                message = "File deleted already";
            } else {
                FSManager.deletedFilesData.put(fileName, String.valueOf(version));
                FSManager.setDeletedFilesData();
                rc = 0;
                message = "File deleted successfully";
            }
        }
        reply.setRc(rc);
        reply.setMessage(message);
        responseObserver.onNext(reply.build());
        responseObserver.onCompleted();
    }


    @Override
    public void list(Qddfs1.ListRequest request, StreamObserver<Qddfs1.ListReply> responseObserver) {

        System.out.println("Received list request");
        List<Qddfs1.FSEntry> fsEntries = new ArrayList<>();

        for (Map.Entry<String, String> entry : FSManager.metaData.entrySet()) {
            Qddfs1.FSEntry qddfsEntry = Qddfs1.FSEntry.newBuilder().setName(entry.getKey()).setVersion(Integer.parseInt(entry.getValue())).setSize(0).setIsTombstone(false).build();
            fsEntries.add(qddfsEntry);
        }
        for (Map.Entry<String, String> entry : FSManager.deletedFilesData.entrySet()) {
            Qddfs1.FSEntry qddfsEntry = Qddfs1.FSEntry.newBuilder().setName(entry.getKey()).setVersion(Integer.parseInt(entry.getValue())).setSize(0).setIsTombstone(true).build();
            fsEntries.add(qddfsEntry);
        }

        System.out.println("Returning list: " + fsEntries);

        Qddfs1.ListReply reply = Qddfs1.ListReply.newBuilder().addAllEntries(fsEntries).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();

    }

    @Override
    public void bumpVersion(Qddfs1.BumpVersionRequest request, StreamObserver<Qddfs1.BumpVersionReply> responseObserver) {
        List<String> fileList = request.getNameList();
        int bumpToVer = request.getNewVersion();
        for (String fileName : fileList) {
            if (FSManager.metaData.containsKey(fileName)) {
                int currVersion = Integer.parseInt(FSManager.metaData.get(fileName));
                if (bumpToVer > currVersion) {
                    FSManager.metaData.put(fileName, String.valueOf(currVersion));
                    FSManager.setMetaData();
                    if (FSManager.isNSAvailable) {
                        try {
                            Qddfs1.NSAddRequest addFileRequest = Qddfs1.NSAddRequest.newBuilder().setHostPort(FSManager.ipAddr).setEntry(Qddfs1.FSEntry.newBuilder().setName(fileName).setVersion(bumpToVer).setSize(Long.parseLong(FSManager.fileSizeData.get(fileName))).setIsTombstone(FSManager.deletedFilesData.containsKey(fileName)).build()).build();
                            Qddfs1.NSAddReply reply = FSManager.nameServer.addFileOrTombstone(addFileRequest);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        Qddfs1.BumpVersionReply reply = Qddfs1.BumpVersionReply.newBuilder().build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}



