import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

/**
 *W07Practical.java.
 */
public class W07Practical {
    private String line = "";
    private final int passangerId = 0;
    private final int survived = 1;
    private final int pClass = 2;
    private final int name = 3;
    private final int sex = 4;
    private final int age = 5;
    private final int sibSp = 6;
    private final int parch = 7;
    private final int ticket = 8;
    private final int fare = 9;
    private final int cabin = 10;
    private final int embarked = 11;

    /**
     * Connects to a database, makes a table, reads a csv into the table and prints "OK".
     * @param dbFileName        this is used for the suffix of jdbc:sqlite: which is used to connect to the database
     * @param csv               this takes the csv files
     * @throws SQLException     throws possible SQLException
     */
    public void tryToAccessDB(String dbFileName, String csv) throws SQLException {

        Connection connection = null;
        try {
            String dbUrl = "jdbc:sqlite:" + dbFileName;
            connection = DriverManager.getConnection(dbUrl);

            createTable(connection);
            readCSV(csv, connection);
            System.out.println("OK");

            connection.close();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Connects to database and takes 3 arguments (<db_file> <action> [input_file]) to satisfy the 5 cases.
     * @param args              takes a string array of arguments from 0 to 2
     * @throws SQLException     throws possible SQLException
     */
    public void action(String[] args) throws SQLException {

        Connection connection = null;
        try {
            String dbUrl = "jdbc:sqlite:" + args[0];
            connection = DriverManager.getConnection(dbUrl);
            switch (args[1]) {
                case "create":
                    tryToAccessDB(args[0], args[2]);
                    break;

                case "query1":
                    printTable(connection);
                    break;

                case "query2":
                    totalSurvivors(connection);
                    break;

                case "query3":
                    countingSurvivorByClass(connection);
                    break;

                case "query4":
                    minimumAge(connection);
                    break;

                default:
                    break;
            }
            connection.close();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Used to connect to the database and update the tables after dropping if it exists.
     * @param connection        used to access the database
     * @throws SQLException     throws possible SQLException
     */
    public void createTable(Connection connection) throws SQLException {

        Statement statement = connection.createStatement();

        statement.executeUpdate("DROP TABLE IF EXISTS person");
        statement.executeUpdate("CREATE TABLE person (passengerId INT PRIMARY KEY, survived INT, pClass INT, name VARCHAR(100), sex VARCHAR(100), age FLOAT, sibSp INT, parch INT, ticket VARCHAR(100), fare FLOAT, cabin VARCHAR(100), embarked VARCHAR(100))");

        statement.close();
    }

    /** First try in inserting values
     * private void insertPersonUsingPreparedStatement(int passengerId, int survived, int pClass, String name, String sex, float age, int sibSp, int parch, String ticket, float fare, String cabin, String embarked, Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("INSERT INTO person VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        statement.setInt(1, passengerId);
        statement.setInt(2, survived);
        statement.setInt(3, pClass);
        statement.setString(4, name);
        statement.setString(5, sex);
        statement.setFloat(6, age);
        statement.setInt(7, sibSp);
        statement.setInt(8, parch);
        statement.setString(9, ticket);
        statement.setFloat(10, fare);
        statement.setString(11, cabin);
        statement.setString(12, embarked);

        statement.executeUpdate();
        statement.close();
    }
     */

    /**
     * Prints the table.
     * @param connection        used to access the database
     * @throws SQLException     throws possible SQLException
     */
    public void printTable(Connection connection) throws SQLException {

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person");

        System.out.println("\npassengerId, survived, pClass, name, sex, age, sibSp, parch, ticket, fare, cabin, embarked");
        while (resultSet.next()) {

            // can access resultset columns by index
            int passengerId = resultSet.getInt("passengerId");
            int survived = resultSet.getInt("survived");
            int pClass = resultSet.getInt("pClass");
            String name = resultSet.getString("name");
            String sex = resultSet.getString("sex");
            Float age;
            if (resultSet.getString("age") == null) {
                age = null;
            } else {
                age = resultSet.getFloat("age");
            }
            int sibSp = resultSet.getInt("sibSp");
            int parch = resultSet.getInt("parch");
            String ticket = resultSet.getString("ticket");
            float fare = resultSet.getFloat("fare");
            String cabin = resultSet.getString("cabin");
            String embarked = resultSet.getString("embarked");

            System.out.println(passengerId + ", " + survived + ", " + pClass + ", " + name + ", " + sex + ", " + age + ", " + sibSp + ", " + parch + ", " + ticket + ", " + fare + ", " + cabin + ", " + embarked);

        }
        statement.close();
    }

    /**
     * Reads the csv file and update the values into the database.
     * @param inputfile         reads the csv file
     * @param connection        used to access the database
     * @throws SQLException     throws possible SQLException
     */
    public void readCSV(String inputfile, Connection connection) throws SQLException {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputfile))) {
            PreparedStatement statement = connection.prepareStatement("INSERT INTO person VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] elements = line.split(",");
                validityCheckInt(elements, statement, passangerId);
                validityCheckInt(elements, statement, survived);
                validityCheckInt(elements, statement, pClass);
                validityCheckString(elements, statement, name);
                validityCheckString(elements, statement, sex);
                validityCheckFloat(elements, statement, age);
                validityCheckInt(elements, statement, sibSp);
                validityCheckInt(elements, statement, parch);
                validityCheckString(elements, statement, ticket);
                validityCheckFloat(elements, statement, fare);
                validityCheckString(elements, statement, cabin);
                if (elements.length == embarked) {
                    statement.setNull(embarked + 1, Types.VARCHAR);
                } else {
                    statement.setString(embarked + 1, elements[embarked]);
                }
                statement.executeUpdate();
            }
            statement.close();
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO Exception: " + e.getMessage());
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Out Of Bounds: " + e.getMessage());
        }
    }

    /**
     * Counts the total number of survivors by selecting people who lived and counting.
     * @param connection        used to access the database
     * @throws SQLException     throws possible SQLException
     */
    public void totalSurvivors(Connection connection) throws SQLException {
        int counting = 0;
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM person where survived = 1");
        while (resultSet.next()) {
            counting++;
        }
        System.out.println("Number of Survivors");
        System.out.println(counting);
        statement.close();
    }

    /**
     * Counts the number of people in a certain group which depends on class and if they survived.
     * @param connection        used to access the database
     * @throws SQLException     throws possible SQLException
     */
    public void countingSurvivorByClass(Connection connection) throws SQLException {
        int counting = 0;
        int counting2 = 0;
        int counting3 = 0;
        int counting4 = 0;
        int counting5 = 0;
        int counting6 = 0;

        Statement statement = connection.createStatement();
        System.out.println("pClass, survived, count");
        ResultSet resultSet = statement.executeQuery("SELECT pClass, survived FROM person where pClass = 1 AND survived = 0");
        while (resultSet.next()) {
            counting++;
        }
        System.out.println("1, 0, " + counting);
        ResultSet resultSet2 = statement.executeQuery("SELECT pClass, survived FROM person where pClass = 1 AND survived = 1");
        while (resultSet2.next()) {
            counting2++;
        }
        System.out.println("1, 1, " + counting2);
        ResultSet resultSet3 = statement.executeQuery("SELECT pClass, survived FROM person where pClass = 2 AND survived = 0");
        while (resultSet3.next()) {
            counting3++;
        }
        System.out.println("2, 0, " + counting3);
        ResultSet resultSet4 = statement.executeQuery("SELECT pClass, survived FROM person where pClass = 2 AND survived = 1");
        while (resultSet4.next()) {
            counting4++;
        }
        System.out.println("2, 1, " + counting4);
        ResultSet resultSet5 = statement.executeQuery("SELECT pClass, survived FROM person where pClass = 3 AND survived = 0");
        while (resultSet5.next()) {
            counting5++;
        }
        System.out.println("3, 0, " + counting5);
        ResultSet resultSet6 = statement.executeQuery("SELECT pClass, survived FROM person where pClass = 3 AND survived = 1");
        while (resultSet6.next()) {
            counting6++;
        }
        System.out.println("3, 1, " + counting6);
        statement.close();
    }

    /**
     * Finds the minimum age of a certain group of people which depends on sex and if they survived.
     * @param connection        used to access the database
     * @throws SQLException     throws possible SQLException
     */
    public void minimumAge(Connection connection) throws  SQLException {
        float min = 0;
        float min2 = 0;
        float min3 = 0;
        float min4 = 0;
        Statement statement = connection.createStatement();
        System.out.println("sex, survived, minimum age");
        ResultSet resultSet = statement.executeQuery("SELECT min(age) FROM person where sex = 'female' AND survived = 0");
        min = resultSet.getFloat(1);
        System.out.println("female, 0, " + min);
        ResultSet resultSet2 = statement.executeQuery("SELECT min(age) FROM person where sex = 'female' AND survived = 1");
        min2 = resultSet2.getFloat(1);
        System.out.println("female, 1, " + min2);
        ResultSet resultSet3 = statement.executeQuery("SELECT min(age) FROM person where sex = 'male' AND survived = 0");
        min3 = resultSet3.getFloat(1);
        System.out.println("male, 0, " + min3);
        ResultSet resultSet4 = statement.executeQuery("SELECT min(age) FROM person where sex = 'male' AND survived = 1");
        min4 = resultSet4.getFloat(1);
        System.out.println("male, 1, " + min4);
        statement.close();
    }

    /**
     * Sets empty elements to null then updates the table with the String elements.
     * @param elements          takes the array of elements
     * @param statement         checks the prepared statement
     * @param index             takes the index of the columns
     * @throws SQLException     throws possible SQLException
     */
    public static void validityCheckString(String[] elements, PreparedStatement statement, int index) throws SQLException {
        if (elements[index].isEmpty()) {
            statement.setString(index + 1, null);
        } else {
            statement.setString(index + 1, elements[index]);
        }
    }

    /**
     * Sets empty elements to null then updates the table with the Int elements.
     * @param elements          takes the array of elements
     * @param statement         checks the prepared statement
     * @param index             takes the index of the columns
     * @throws SQLException     throws possible SQLException
     */
    public static void validityCheckInt(String[] elements, PreparedStatement statement, int index)throws SQLException {
        if (elements[index].isEmpty()) {
            statement.setNull(index + 1, Types.INTEGER);
        } else {
            statement.setInt(index + 1, Integer.parseInt(elements[index]));
        }
    }

    /**
     * Sets empty elements to null then updates the table with the Float elements.
     * @param elements          takes the array of elements
     * @param statement         checks the prepared statement
     * @param index             takes the index of the columns
     * @throws SQLException     throws possible SQLException
     */
    public static void validityCheckFloat(String[] elements, PreparedStatement statement, int index)throws SQLException {
        if (elements[index].isEmpty()) {
            statement.setNull(index + 1, Types.INTEGER);
        } else {
            statement.setFloat(index + 1, Float.parseFloat(elements[index]));
        }
    }

    /**
     * Main method for the W07Practical.java file.
     * @param args              takes a string of arguments (<db_file> <action> [input_file])
     * @throws SQLException     throws possible SQLException
     */
    public static void main(String[] args) throws SQLException {

        if (args.length < 1) {
            System.out.println("Usage: java -cp sqlite-jdbc.jar:. W07Practical <db_file> <action> [input_file]");
        } else {
            W07Practical practical = new W07Practical();
            practical.action(args);
        }
    }
}
