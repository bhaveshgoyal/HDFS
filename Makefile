compile:
	javac -d ./bin source/DataNode.java source/NameNode.java source/INameNode.java

protobuf:
	protoc -I=./ --java_out=./source ./hdfs.proto
	javac -cp ./libs/protobuf-java-2.6.1.jar -d ./bin source/Hdfs.java

