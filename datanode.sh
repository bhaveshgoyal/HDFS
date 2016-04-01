echo "Compiling DataNode source..."
make dn-compile
make dn-rmi
echo "Compilation Successful"
echo "Triggering DataNode for HDFS File Storage..."
make dn-trigger
