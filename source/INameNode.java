import java.rmi.*;

public interface INameNode extends java.rmi.Remote
{	

        /* HeartBeatResponse heartBeat(HeartBeatRequest) */
//      /* Heartbeat messages between NameNode and DataNode */
        byte[] heartBeat(byte[] inp ) throws RemoteException;
}


