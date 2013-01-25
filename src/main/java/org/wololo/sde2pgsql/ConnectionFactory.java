package org.wololo.sde2pgsql;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.log4j.Logger;

import com.esri.sde.sdk.client.SeConnection;
import com.esri.sde.sdk.client.SeException;

public class ConnectionFactory {

	static Logger log = Logger.getLogger(ConnectionFactory.class);

	static Properties properties = new Properties();

	static {
		try {
			properties.load(new FileReader("SDE2PostGIS.properties"));
		} catch (FileNotFoundException e) {
			log.fatal("Properties file not found");
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			log.fatal("Could not read properties file");
			e.printStackTrace();
			System.exit(-1);
		}
	};

	static SeConnection createSeConnection() throws SeException {
		String host = properties.getProperty("sde.host");
		String port = properties.getProperty("sde.port");
		String db = properties.getProperty("sde.db");
		String user = properties.getProperty("sde.user");
		String password = properties.getProperty("sde.password");

		SeConnection seConnection = new SeConnection(host, port, db, user, password);

		return seConnection;
	}

	static Connection createPostGISConnection() throws SQLException, ClassNotFoundException {
		String host = properties.getProperty("postgis.host");
		String port = properties.getProperty("postgis.port");
		String db = properties.getProperty("postgis.db");
		String user = properties.getProperty("postgis.user");
		String password = properties.getProperty("postgis.password");

		Class.forName("org.postgresql.Driver");
		Connection connection = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + db, user, password);

		((org.postgresql.PGConnection)connection).addDataType("geometry",org.postgis.PGgeometry.class);
		((org.postgresql.PGConnection)connection).addDataType("box3d",org.postgis.PGbox3d.class);
		
		connection.setAutoCommit(false);
		
		return connection;
	}
}
