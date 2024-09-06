package uni.climatemonitor.server;

import uni.climatemonitor.common.IDatabaseService;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class ServerMain {

    public static void main(String args[]) throws RemoteException {
        DatabaseServiceImpl dbService = new DatabaseServiceImpl();
        IDatabaseService ids = (IDatabaseService) UnicastRemoteObject.exportObject(dbService, 8080);
        Registry registry = LocateRegistry.createRegistry(1099);
        registry.rebind("dbService", dbService);
        System.out.println("Server ready");
    }
}
