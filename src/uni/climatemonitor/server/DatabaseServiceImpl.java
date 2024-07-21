package uni.climatemonitor.server;

import uni.climatemonitor.common.Coordinates;
import uni.climatemonitor.common.IDatabaseService;
import uni.climatemonitor.common.Location;
import uni.climatemonitor.common.Operator;
import uni.climatemonitor.common.MonitoringCenter;
import uni.climatemonitor.common.ClimateParameter;

import java.rmi.RemoteException;
import java.sql.*;
import java.util.ArrayList;

public class DatabaseServiceImpl implements IDatabaseService {
    static final String dbURL = "jdbc:mysql://127.0.0.1:3306/ClimateMonitoring";
    static final String dbUsername = "luca";
    static final String password = "ClimateMonitoring";

    public boolean operatorExists(String username, String taxCode) throws RemoteException {
        final String query = "SELECT COUNT(*) AS recordCount FROM operators WHERE username=\"" + username + "\"";

        try {
            ResultSet results = getStatement().executeQuery(query);
            results.next();
            int count = results.getInt("recordCount");
            System.out.println("Count of operators with username " + username + ": " + count);
            return (count == 1? true : false);

        } catch (SQLException e) {
            // something wrong
            e.printStackTrace();
            return false;
        }

    }

    public boolean pushOperator(Operator o) throws RemoteException {
        final MonitoringCenter mc = o.getMonitoringCenter();
        final String query = String.format("""
                INSERT INTO operators (name, tax_code, email, username, pwd, center) VALUES (%s, %s, %s, %s, %s, &s)
                """, o.getName(), o.getTaxCode(), o.getEmail(), o.getUsername(), o.getPassword(), mc.);

        try {
            Statement statement = getStatement();
            int rowsInserted = statement.executeUpdate(query);
            if (rowsInserted != 1) {
                throw(new AssertionError());
            }
            return true;

        } catch (SQLException e) {
            // something wrong
            e.printStackTrace();
            return false;
        } catch (AssertionError e) {
            // something wrong
            e.printStackTrace();
            return false;
        }

    }

    public boolean isOperatorEnabledForLocation(Location l) throws RemoteException {
        return true;
    }

    public MonitoringCenter getMonitoringCenterForOperator(Operator o) throws RemoteException {
        return null;
    }

    public ArrayList<Location> filterLocationsByName(String filterOnName) throws RemoteException {
        ArrayList<Location> outList = new ArrayList<Location>();
        return outList;
    }

    public ArrayList<Location> filterLocationsByCoordinates(Coordinates coordinates) throws RemoteException {
        ArrayList<Location> outList = new ArrayList<Location>();
        return outList;
    }

    public boolean locationExists(Location l) throws RemoteException { return true; }

    public boolean pushLocation(Location l) throws RemoteException { return true; }

    public ClimateParameter getClimateParameterForDate(Date date) throws RemoteException {
        return null;
    }

    public boolean pushClimateParameter(ClimateParameter p) throws RemoteException {
        return true;
    }

    public MonitoringCenter getMonitoringCenterFromName(String name) throws RemoteException {
        return null;
    }

    public boolean pushMonitoringCenter(MonitoringCenter c) throws RemoteException{
        return true;
    }

    private Statement getStatement() throws SQLException {
        Connection conn = DriverManager.getConnection(dbURL, dbUsername, password);
        Statement statement = conn.createStatement();
        return statement;
    }

    // tests
    public static void main(String args[]) throws RemoteException, SQLException {
        DatabaseServiceImpl d = new DatabaseServiceImpl();

        System.out.println(d.operatorExists("mrossi82", "pippoo"));
        System.out.println(d.operatorExists("sdf", "pippoo"));

        Operator o = new Operator("Luca Bianchi", "BNCLCU91L26L682G", "lbianchi@yahoo.com", "lbianchi", "Varese10#", );
        System.out.println(d.pushOperator(o));
        System.out.println(d.operatorExists("lbianchi", "pippoo"));

    }
}
