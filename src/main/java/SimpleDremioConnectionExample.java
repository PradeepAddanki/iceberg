import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class SimpleDremioConnectionExample {

    private static final Logger log = LoggerFactory.getLogger(SimpleDremioConnectionExample.class);

    public static void main(String[] args) {
        try {
            // Load the Dremio JDBC driver
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
            // Construct the connection URL
            String url = "jdbc:arrow-flight-sql://localhost:32010/?useEncryption=false&schema=@addanki";
            // Establish the connection
            Connection conn = DriverManager.getConnection(url, "addanki", "Cts@2024");
            // Execute a query
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM data_20200602");

            // Process the result set
            while (rs.next()) {
                System.out.println(rs.getString("myString")); // Replace column_name with an actual column
            }

            // Close the resources
            rs.close();
            stmt.close();
            conn.close();

        } catch (ClassNotFoundException | SQLException e) {
            log.error("Exception in DremioConnectionExample ",e.getCause());
        }
    }
}
