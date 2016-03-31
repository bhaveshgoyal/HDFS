javac  -cp .:./libs/protobuf-java-2.6.1.jar:./Client/source:./NameNode/source:./DataNode/source -d ./Client/bin ./Client/source/Client.java
java  -cp .:./libs/protobuf-java-2.6.1.jar:./Client/bin:./NameNode/bin:./DataNode/bin Client

