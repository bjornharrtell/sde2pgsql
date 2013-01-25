package org.wololo.sde2pgsql;

import java.sql.SQLException;

import org.apache.log4j.PropertyConfigurator;

import com.esri.sde.sdk.client.SeException;

public class ConsoleApplication {
	public static void main(String[] args) throws SeException, SQLException, ClassNotFoundException {
		
		PropertyConfigurator.configure("SDE2PostGIS.properties");
		
		SDE2PostGISExporter sde2postgis = new SDE2PostGISExporter();

		String sdeName = args[0];
		String schemaName = args[1];
		int buffer = Integer.parseInt(args[2]);
		int srid = Integer.parseInt(args[3]);
		
		sde2postgis.export(sdeName, schemaName, buffer, srid);
	}
}
