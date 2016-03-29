import java.rmi.*;

public interface INameNode extends java.rmi.Remote
{	
	String sayHello() throws RemoteException;
}


