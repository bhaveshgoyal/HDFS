compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/source -d ./NameNode/bin ./NameNode/source/NameNode.java	
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/source:./DataNode/source -d ./DataNode/bin ./DataNode/source/DataNode.java ./DataNode/source/IDataNode.java
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./Client/source:./NameNode/source:./DataNode/source -d ./Client/bin ./Client/source/Client.java
	protoc -I=./ --java_out=./ ./hdfs.proto
	javac -cp ./libs/protobuf-java-2.6.1.jar -d ./bin ./com/bagl/protobuf/Hdfs.java
		
nn-compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/source -d ./NameNode/bin ./NameNode/source/NameNode.java	

dn-compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/source:./DataNode/source -d ./DataNode/bin ./DataNode/source/DataNode.java ./DataNode/source/IDataNode.java

client-compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./Client/source:./NameNode/source:./DataNode/source -d ./Client/bin ./Client/source/Client.java

nn-trigger:
	java  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/bin NameNode

dn-trigger:
	java  -cp .:./libs/protobuf-java-2.6.1.jar:./DataNode/bin DataNode `shuf -i 1-100 -n 1`


client-trigger:
	java  -cp .:./libs/protobuf-java-2.6.1.jar:./Client/bin:./NameNode/bin:./DataNode/bin Client

nn-rmi:
	cd ./NameNode/bin; rmiregistry &

dn-rmi:
	cd ./DataNode/bin; rmiregistry &

protobuf:
	protoc -I=./ --java_out=./ ./hdfs.proto
	javac -cp ./libs/protobuf-java-2.6.1.jar -d ./bin ./com/bagl/protobuf/Hdfs.java

