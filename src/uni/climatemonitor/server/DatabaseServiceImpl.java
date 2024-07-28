package uni.climatemonitor.server;

import uni.climatemonitor.common.Coordinates;
import uni.climatemonitor.common.IDatabaseService;
import uni.climatemonitor.common.Location;
import uni.climatemonitor.common.Operator;
import uni.climatemonitor.common.MonitoringCenter;
import uni.climatemonitor.common.ClimateParameter;

import java.rmi.RemoteException;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class DatabaseServiceImpl implements IDatabaseService {
    static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/ClimateMonitoring";
    static final String DB_USERNAME = "luca";
    static final String PASSWORD = "ClimateMonitoring";
    static final double MAX_DIST = 50_000.0;

    public boolean operatorExists(String username) throws RemoteException {
        final String query = "SELECT COUNT(*) FROM Operator WHERE username=\"" + username + "\"";

        try {
            ResultSet results = getStatement().executeQuery(query);
            int count = countRowsFromSelectCountQuery(results);
            return (count == 1? true : false);

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return false;
        }
    }

    public boolean pushOperator(Operator o) throws RemoteException {
        final MonitoringCenter mc = o.getMonitoringCenter();
        final String query = String.format("""
                INSERT INTO Operator (name, tax_code, email, username, pwd, id) VALUES ("%s", "%s", "%s", "%s", "%s", "%s")""",
                o.getName(), o.getTaxCode(), o.getEmail(), o.getUsername(), o.getPassword(), mc.getId());

        return pushSomethingToDB(query);
    }

    public boolean isOperatorEnabledForLocation(String username, Location l) throws RemoteException {
        final String locationId = l.getGeonameID();
        String query = String.format("""
            select count(*) from Operator where id = ( select center_id from Monitors where area_id = "%s") and username = "%s"
            """, locationId, username);

        try {
            Statement statement = getStatement();
            ResultSet results = statement.executeQuery(query);
            int count = countRowsFromSelectCountQuery(results);
            return (count == 1? true : false);

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return false;
        }
    }

    public MonitoringCenter getMonitoringCenterForOperator(String username) throws RemoteException {
        final String query = String.format("""
                select * from MonitoringCenter where id = (select id from Operator where username = "%s")""",
                username);
        try {
            Statement statement = getStatement();
            ResultSet results = statement.executeQuery(query);
            // results can only be composed by a single row, because we are imposing
            // "where id = something", and since id is the primary key, it will always be unique
            results.next();
            MonitoringCenter monitoringCenter = new MonitoringCenter(
                    results.getString("name"), results.getString("address"), results.getString("id"));
            return monitoringCenter;
        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return null;
        }
    }

    public ArrayList<Location> filterLocationsByName(String filterOnName) throws RemoteException {
        ArrayList<Location> outList = new ArrayList<Location>();

        final String query = String.format("""
               select * from Location where name like '%s%%'""", filterOnName);

        try {
            Statement statement = getStatement();
            ResultSet results = statement.executeQuery(query);

            // create array list
            while (results.next()) {
                final String geonameID = results.getString("id");
                final String name = results.getString("name");
                final String asciiName = results.getString("ascii_name");
                final String state = results.getString("state");
                final double latitude = results.getDouble("latitude");
                final double longitude = results.getDouble("longitude");

                final Location l = new Location(geonameID, name, asciiName, state, latitude, longitude);
                outList.add(l);
            }
            return outList;

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return null;
        }
    }

    public ArrayList<Location> filterLocationsByCoordinates(Coordinates coordinates) throws RemoteException {
        ArrayList<Location> outList = new ArrayList<Location>();

        final String query = "select * from Location";

        try {
            Statement statement = getStatement();
            ResultSet results = statement.executeQuery(query);

            // these are useful for the distance algorithm
            Map<Double, Location> sortedMap;
            Map<Double, Location> unsortedMap = new HashMap<>();

            // create array list
            while (results.next()) {
                // create Location object
                final String geonameID = results.getString("id");
                final String name = results.getString("name");
                final String asciiName = results.getString("ascii_name");
                final String state = results.getString("state");
                final double latitude = results.getDouble("latitude");
                final double longitude = results.getDouble("longitude");
                final Location l = new Location(geonameID, name, asciiName, state, latitude, longitude);

                // compute distance between given coordinates and the current
                double dist = coordinates.distance( l.getCoordinates() );
                // accept only those locations with a distance <= 50km
                if (dist <= MAX_DIST) {
                    unsortedMap.put(dist, l);
                }
            }

            // get a sorted map by keys: keys are distances from the searched
            // coordinates to the ones in the locations' file.
            sortedMap = new TreeMap<Double, Location>(unsortedMap);
            // at the end we can add to the suggestion list in the right order,
            // from the nearest to the furthest
            for (Map.Entry<Double, Location> entry : sortedMap.entrySet()){
                outList.add(entry.getValue());
            }
            return outList;

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return null;
        }
    }

    public boolean locationExists(Location l) throws RemoteException {
        final String query = "SELECT COUNT(*) FROM Location WHERE id = \"" + l.getGeonameID() + "\"";

        try {
            ResultSet results = getStatement().executeQuery(query);
            int count = countRowsFromSelectCountQuery(results);
            return (count == 1? true : false);

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return false;
        }
    }

    public boolean pushLocation(Location l) throws RemoteException {
        final String query = String.format("""
                INSERT INTO Location (id, name, ascii_name, state, latitude, longitude) VALUES ("%s", "%s", "%s", "%s", %f. %f)""",
                l.getGeonameID(), l.getAsciiName(), l.getAsciiName(), l.getState(), l.getCoordinates().getLatitude(), l.getCoordinates().getLongitude());

        return pushSomethingToDB(query);
    }

    public ClimateParameter getClimateParameterForDate(Location l, LocalDate date) throws RemoteException {
        final String query = String.format("""
                select * from ClimateParameter where (date = "%s" and geoname_id = "%s")""" , date, l.getGeonameID());

        try {
            Statement statement = getStatement();
            ResultSet results = statement.executeQuery(query);
            // results can only be composed by a single row, because we are imposing
            // "where date = ... and geoname_id = ...", and since (date, geoname_id) is the primary key, it will always be unique
            results.next();
            ClimateParameter climateParameter = new ClimateParameter(l.getGeonameID(),
                    results.getInt("wind"), results.getInt("humidity"), results.getInt("pressure"),
                    results.getInt("temperature"), results.getInt("rainfall"), results.getInt("glaciers_alt"),
                    results.getInt("glaciers_mass"), results.getString("notes"), date, results.getString("who"));
            return climateParameter;
        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return null;
        }
    }

    public boolean pushClimateParameter(ClimateParameter p) throws RemoteException {
        final String query = String.format("""
                INSERT INTO ClimateParameter (date, geoname_id, wind, humidity, pressure, temperature, rainfall, glaciers_alt, glaciers_mass, notes, who) VALUES ("%s", "%s", %d, %d, %d, %d, %d, %d, %d, "%s", "%s")""",
                p.getDate().toString(), p.getGeonameId(), p.getWind(), p.getHumidity(), p.getPressure(), p.getTemperature(), p.getRainfall(), p.getGlaciersAlt(), p.getGlaciersMass(), p.getNotes(), p.getWho());

        return pushSomethingToDB(query);
    }

    public MonitoringCenter getMonitoringCenterFromName(String name) throws RemoteException {
        final String query = String.format("""
                select * from MonitoringCenter where (name = "%s")""" , name);

        try {
            Statement statement = getStatement();
            ResultSet results = statement.executeQuery(query);
            // results can only be composed by a single row, because we are imposing
            // "where name = ...", and name is unique in the table
            results.next();
            MonitoringCenter monitoringCenter = new MonitoringCenter(name, results.getString("address"), results.getString("id"));
            return monitoringCenter;
        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return null;
        }
    }

    /**
     * ! SISTEMARE, NON CONOSCO ID PRIMA DI CREARLO, DEVE ESSERE QUESTA FUNZIONE CHE ME LO PASSA
     * @param c
     * @return
     * @throws RemoteException
     */
    public boolean pushMonitoringCenter(MonitoringCenter c) throws RemoteException {
        final String query = String.format("""
                INSERT INTO MonitoringCenter (id, name, address) VALUES ("%s", "%s", "%s")""",
                c.getId(), c.getName());

        return pushSomethingToDB(query);
    }

    public boolean isMonitoringCentersTableEmpty() throws RemoteException {
        final String query = String.format("select count(*) from MonitoringCenter");

        try {
            ResultSet results = getStatement().executeQuery(query);
            int count = countRowsFromSelectCountQuery(results);
            return (count == 0? true : false);

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return false;
        }

    }

    private Statement getStatement() throws SQLException {
        Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, PASSWORD);
        Statement statement = conn.createStatement();
        return statement;
    }

    /**
     * Count rows of a ResultSet object result of a "SELECT COUNT(*)" query.
     * @param results ResultSet object
     * @return the number of rows
     * @throws SQLException
     */
    private int countRowsFromSelectCountQuery(ResultSet results) throws SQLException {
        results.next();
        return results.getInt(1);
    }

    /**
     * Push something to the DB according to query
     * @param query string that contains the query
     * @return true if transaction is ok, false otherwise
     */
    private boolean pushSomethingToDB(String query) {
        try {
            Statement statement = getStatement();
            int rowsInserted = statement.executeUpdate(query);
            // assert that new row is only one, if not returns an error
            assert rowsInserted == 1;
            return true;

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return false;
        }
    }

    // tests
    public static void main(String args[]) throws RemoteException, SQLException {
        DatabaseServiceImpl d = new DatabaseServiceImpl();

        // operators related tests
        Location l = new Location("3164699", "Varese", "Varese", "Italy", 45.82058, 8.82511);
        System.out.println(d.operatorExists("lbianchi"));
        System.out.println(d.isOperatorEnabledForLocation("lbianchi", l));
        System.out.println(d.getMonitoringCenterForOperator("lbianchi"));

        // filters related tests
        System.out.println(d.filterLocationsByName("mila"));
        System.out.println(d.filterLocationsByCoordinates(new Coordinates(45.46427, 9.18951)));

        // check if the format works as expected
        final String query = String.format("""
                INSERT INTO Location (id, name, ascii_name, state, latitude, longitude) VALUES ("%s", "%s", "%s", "%s", %f. %f)""",
                l.getGeonameID(), l.getAsciiName(), l.getAsciiName(), l.getState(), l.getCoordinates().getLatitude(), l.getCoordinates().getLongitude());
        System.out.println(query);

        // another check on format
        ClimateParameter p = new ClimateParameter("3164699", 5, 4, 3, 3, 4,
                4, 5, "", LocalDate.of(2024, 12, 23), "BNCLCU91L26L682G");
        System.out.println(d.getClimateParameterForDate(l, LocalDate.of(2024, 12, 23)).getGeonameId());

        // check monitoring center get
        System.out.println(d.getMonitoringCenterFromName("Centro Climatico di Como").getId());
    }
}
