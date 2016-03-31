echo "Compiling DataNode source..."
make dn-compile
echo "Compilation Successful"
echo "Triggering DataNode for HDFS File Storage..."
make dn-trigger
