javac  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/source:./DataNode/source -d ./DataNode/bin ./DataNode/source/DataNode.java ./DataNode/source/IDataNode.java
java  -cp .:./libs/protobuf-java-2.6.1.jar:./DataNode/bin DataNode $(((RANDOM % 50) + 1))

