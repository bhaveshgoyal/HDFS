echo "Compiling NameNode source..."
make nn-compile
make nn-rmi
echo "Compilation Successful"
echo "Triggering NameNode..."
make nn-trigger
