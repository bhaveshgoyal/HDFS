rmi:
	cd ./NameNode/bin; rmiregistry &

protobuf:
	protoc -I=./ --java_out=./ ./hdfs.proto
	javac -cp ./libs/protobuf-java-2.6.1.jar -d ./bin ./com/bagl/protobuf/Hdfs.java

