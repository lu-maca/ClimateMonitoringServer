package uni.climatemonitor.server;

import uni.climatemonitor.common.Coordinates;
import uni.climatemonitor.common.IDatabaseService;
import uni.climatemonitor.common.Location;
import uni.climatemonitor.common.Operator;
import uni.climatemonitor.common.IClient;
import uni.climatemonitor.common.MonitoringCenter;
import uni.climatemonitor.common.ClimateParameter;

import java.io.Serial;
import java.rmi.RemoteException;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class DatabaseServiceImpl implements IDatabaseService {
    static String DB_URL = "jdbc:mysql://%s:%d/ClimateMonitoring";
    static final String DB_USERNAME = "luca";
    static final String PASSWORD = "ClimateMonitoring";
    static final double MAX_DIST = 50_000.0;

    HashMap<IClient, Location> clientsInLocation;

    public DatabaseServiceImpl(String host, int port) {
        DB_URL = String.format(DB_URL, host, port);
        clientsInLocation = new HashMap<>();
    }

    @Override
    public synchronized void registerClientForLocation(IClient client, Location l) {
        clientsInLocation.put(client, l);
    }

    @Override
    public synchronized void unregisterClientForLocation(IClient client) {
        clientsInLocation.remove(client);
    }

    @Override
    public Operator operatorExists(String username) throws RemoteException {
        final String query = String.format("""
                select distinct  tax_code, o.name as oper_name, email, username, pwd, center_id, m.name as center_name, address from Operator o join Monitors on (id = center_id) join MonitoringCenter m using (id)  where (username = "%s")""", username);

        try {
            ResultSet results = getStatement().executeQuery(query);
            if (!results.next()) { return null; }
            MonitoringCenter mc = new MonitoringCenter(results.getString("center_name"), results.getString("address"), results.getString("center_id") );
            Operator operator = new Operator(results.getString("oper_name"), results.getString("tax_code"),
                                            results.getString("email"), results.getString("username"),
                                            results.getString("pwd"), mc);
            return operator;

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean pushOperator(Operator o) throws RemoteException {
        final MonitoringCenter mc = o.getMonitoringCenter();
        final String query = String.format("""
                INSERT INTO Operator (name, tax_code, email, username, pwd, id) VALUES ("%s", "%s", "%s", "%s", "%s", "%s")""",
                o.getName(), o.getTaxCode(), o.getEmail(), o.getUsername(), o.getPassword(), mc.getId());

        return pushSomethingToDB(query);
    }

    @Override
    public boolean isOperatorEnabledForLocation(String username, Location l) throws RemoteException {
        final String locationId = l.getGeonameID();
        String query = String.format("""
                 select * from Operator join Monitors on (id = center_id) where area_id = "%s"
                """, locationId);

        try {
            Statement statement = getStatement();
            ResultSet results = statement.executeQuery(query);
            while (results.next()) {
                if (results.getString("username").equals(username)) {
                    return true;
                }
            }
            return false;

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public MonitoringCenter getMonitoringCenterForOperator(String tax_code) throws RemoteException {
        final String query = String.format("""
                select * from MonitoringCenter where id = (select id from Operator where tax_code = "%s")""",
                tax_code);
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

    @Override
    public ArrayList<Location> getLocationsFromMonitoringCenter(String mc_id) throws RemoteException {
        ArrayList<Location> outList = new ArrayList<Location>();

        final String query = String.format("""
                select * from Monitors join Location on (area_id = id) where (center_id = "%s")""", mc_id);

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

    @Override
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

    @Override
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

    @Override
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

    @Override
    public boolean pushLocation(Location l) throws RemoteException {
        final String maxIdQuery = "select max(id) from Location";
        Statement statement = null;
        int maxId;
        try {
            statement = getStatement();
            ResultSet results = statement.executeQuery(maxIdQuery);
            results.next();

            maxId = Integer.parseInt(results.getString("max(id)"));

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        int newId = maxId + 1;

        final String query = String.format("""
                INSERT INTO Location (id, name, ascii_name, state, latitude, longitude) VALUES ("%s", "%s", "%s", "%s", %f, %f)""",
                newId, l.getAsciiName(), l.getAsciiName(), l.getState(), l.getCoordinates().getLatitude(), l.getCoordinates().getLongitude());

        return pushSomethingToDB(query);
    }

    @Override
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

    @Override
    public boolean pushClimateParameter(ClimateParameter p) throws RemoteException {
        final String query = String.format("""
                INSERT INTO ClimateParameter (date, geoname_id, wind, humidity, pressure, temperature, rainfall, glaciers_alt, glaciers_mass, notes, who) VALUES ("%s", "%s", %d, %d, %d, %d, %d, %d, %d, "%s", "%s")""",
                p.getDate().toString(), p.getGeonameId(), p.getWind(), p.getHumidity(), p.getPressure(), p.getTemperature(), p.getRainfall(), p.getGlaciersAlt(), p.getGlaciersMass(), p.getNotes(), p.getWho());
        boolean rc = pushSomethingToDB(query);

        if (!rc) { return false; }

        // synchronize clientsInLocation
        synchronized (clientsInLocation) {
            for (Map.Entry<IClient, Location> entry : clientsInLocation.entrySet()) {
                Location thisLocation = entry.getValue();
                if (thisLocation.getGeonameID().equals(p.getGeonameId())) {
                    IClient client = entry.getKey();
                    client.updateMe(p);
                }
            }
        }
        return true;
    }

    @Override
    public ArrayList<ClimateParameter> getClimateParameterHistory(Location l) {
        final String query = String.format("""
                select * from ClimateParameter where (geoname_id="%s") order by date desc""", l.getGeonameID());
        ArrayList<ClimateParameter> outList = new ArrayList<>();

        try {
            Statement statement = getStatement();
            ResultSet results = statement.executeQuery(query);

            // create array list
            while (results.next()) {
                final String geonameId = results.getString("geoname_id");
                final int wind = results.getInt("wind");
                final int humidity = results.getInt("humidity");
                final int pressure = results.getInt("pressure");
                final int temperature = results.getInt("temperature");
                final int rainfall = results.getInt("rainfall");
                final int glaciers_alt = results.getInt("glaciers_alt");
                final int glaciers_mass = results.getInt("glaciers_mass");
                final String notes = results.getString("notes");
                final String who = results.getString("who");
                final LocalDate date = results.getDate("date").toLocalDate();

                final ClimateParameter cp = new ClimateParameter(geonameId, wind, humidity, pressure, temperature,
                                                                rainfall, glaciers_alt, glaciers_mass, notes, date, who);
                outList.add(cp);
            }
            return outList;

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return null;
        }

    }

    @Override
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

    @Override
    public String pushMonitoringCenter(MonitoringCenter c, ArrayList<String> monitoredAreas) throws RemoteException {
        // multiple transactions

        String query_monitors = """
                INSERT INTO Monitors (area_id, center_id) VALUES ("%s", "%s")""";
        final String query_max_id = "SELECT MAX(id) FROM MonitoringCenter";
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(DB_URL, DB_USERNAME, PASSWORD);
            // set autocommit false so that we can have multiple transactions
            conn.setAutoCommit(false);
            Statement statement = conn.createStatement();

            ResultSet res = statement.executeQuery(query_max_id);
            res.next();
            int max_id = res.getInt(1);

            // set max possible id
            final String query_mon_center = String.format("""
                INSERT INTO MonitoringCenter (id, name, address) VALUES ("%d", "%s", "%s")""",
                    max_id+1, c.getName(), c.getAddress());
            statement.addBatch(query_mon_center);

            for (String l : monitoredAreas) {
                String query_area = String.format(query_monitors, l, max_id+1);
                statement.addBatch(query_area);
            }
            statement.executeBatch();
            conn.commit();

            return String.format("%d", max_id+1);

        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
            e.printStackTrace();
            return "-1";
        }
    }

    @Override
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

    @Override
    public ArrayList<MonitoringCenter> getAllMonitoringCenters() throws RemoteException {
        ArrayList<MonitoringCenter> outList = new ArrayList<>();

        final String query = "select * from MonitoringCenter";

        try {
            Statement statement = getStatement();
            ResultSet results = statement.executeQuery(query);

            // create array list
            while (results.next()) {
                final String name = results.getString("name");
                final String address = results.getString("address");
                final String id = results.getString("id");

                final MonitoringCenter mc = new MonitoringCenter(name, address, id);
                outList.add(mc);
            }
            return outList;

        } catch (Exception e) {
            // something wrong
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean addLocationToMonitoringCenter(Location l, MonitoringCenter mc) {
        final String query = String.format("""
                insert into Monitors (area_id, center_id) values (%s, %s)""", l.getGeonameID(), mc.getId());
        return pushSomethingToDB(query);
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
        DatabaseServiceImpl d = new DatabaseServiceImpl("127.0.0.1", 3307);

      /*  // operators related tests
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
        System.out.println(d.getMonitoringCenterFromName("Centro Climatico di Como").getId());*/

        // add monitors for Milano
        /*ArrayList<Location> areas = d.filterLocationsByCoordinates(new Coordinates(45.46427, 9.18951));
        ArrayList<String> areas_ids = new ArrayList<>();
        for (Location l : areas) {areas_ids.add(l.getGeonameID());}
        MonitoringCenter mc_milan = new MonitoringCenter("Centro Metereologico di Milano", "Piazzale Loreto 12, Milano (MI)", "0");
        System.out.println(d.pushMonitoringCenter(mc_milan, areas_ids));*/

        System.out.println(d.operatorExists("lbianchi"));
        System.out.println(d.getAllMonitoringCenters());


    }
}
