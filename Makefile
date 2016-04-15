compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/source -d ./bin ./NameNode/source/NameNode.java	
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/source:./DataNode/source -d ./bin ./DataNode/source/DataNode.java ./DataNode/source/IDataNode.java
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./Client/source:./NameNode/source:./DataNode/source:./JobTracker/source -d ./bin ./Client/source/Client.java
	protoc -I=./ --java_out=./ ./hdfs.proto 
	javac -cp ./libs/protobuf-java-2.6.1.jar -d ./bin ./com/bagl/protobuf/Hdfs.java
		
nn-compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/source -d ./bin ./NameNode/source/NameNode.java	

dn-compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./NameNode/source:./DataNode/source -d ./bin ./DataNode/source/DataNode.java ./DataNode/source/IDataNode.java

client-compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./Client/source:./NameNode/source:./DataNode/source -d /bin ./Client/source/Client.java
tt-compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./Client/source:./NameNode/source:./DataNode/source:./TaskTracker/source:./JobTracker/source  -d ./bin ./TaskTracker/source/TaskTracker.java ./TaskTracker/source/MapPool.java

jt-compile:
	javac  -cp .:./libs/protobuf-java-2.6.1.jar:./Client/source:./NameNode/source:./DataNode/source:./TaskTracker/source:./JobTracker/source  -d ./bin ./JobTracker/source/JobTracker.java ./JobTracker/source/IJobTracker.java

nn-trigger:
	java  -cp .:./libs/protobuf-java-2.6.1.jar:./bin NameNode

dn-trigger:
	java  -cp .:./libs/protobuf-java-2.6.1.jar:./bin DataNode `shuf -i 1-100 -n 1`

jt-trigger:
	java  -cp .:./libs/protobuf-java-2.6.1.jar:./bin JobTracker

tt-trigger:
	java  -cp .:./libs/protobuf-java-2.6.1.jar:./bin TaskTracker

client-trigger:
	java  -cp .:./libs/protobuf-java-2.6.1.jar:./bin:./NameNode/bin:./DataNode/bin Client

nn-rmi:
	cd ./NameNode/bin; rmiregistry &

dn-rmi:
	cd ./DataNode/bin; rmiregistry &

clean:
	rm -rf Blocks
	mkdir Blocks
	rm BlockReport
	touch BlockReport

protobuf:
	protoc -I=./ --java_out=./ ./hdfs.proto
	javac -cp ./libs/protobuf-java-2.6.1.jar -d ./bin ./com/bagl/protobuf/Hdfs.java

