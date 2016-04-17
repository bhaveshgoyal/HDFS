echo "Compiling JobTracker source..."
make jt-compile
make jt-rmi
echo "Compilation Successful"
echo "Triggering JobTracker..."
make jt-trigger
