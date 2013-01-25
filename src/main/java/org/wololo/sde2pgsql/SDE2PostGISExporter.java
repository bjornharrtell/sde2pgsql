package org.wololo.sde2pgsql;

import java.sql.*;
import java.util.*;

import org.apache.log4j.Logger;
import org.postgis.*;
import org.postgis.binary.*;

import com.esri.sde.sdk.client.*;

public class SDE2PostGISExporter {
	SeConnection seConnection = null;
	SeTable seTable;
	Connection connection = null;
	PreparedStatement preparedStatement = null;

	int buffer;
	int srid;
	String sdeQualifiedName;
	String destinationSchema;
	String destinationTable;

	/**
	 * Prepared list of column definition strings for PostGIS table DDL
	 */
	ArrayList<String> columnDefinitions;

	/**
	 * Cached list of SDE column type codes
	 */
	ArrayList<Integer> sdeColumnTypes;

	/**
	 * Map PostGIS column index to SDE column index
	 */
	Map<Integer, Integer> columnMapping;

	/**
	 * Number of columns in PostGIS table
	 */
	int columns;

	public static final Map<Integer, String> sdeColumnTypeMap;

	static {
		sdeColumnTypeMap = new HashMap<Integer, String>();
		sdeColumnTypeMap.put(SeColumnDefinition.TYPE_INT16, "smallint");
		sdeColumnTypeMap.put(SeColumnDefinition.TYPE_INT32, "integer");
		sdeColumnTypeMap.put(SeColumnDefinition.TYPE_INT64, "bigint");
		sdeColumnTypeMap.put(SeColumnDefinition.TYPE_FLOAT32, "real");
		sdeColumnTypeMap.put(SeColumnDefinition.TYPE_FLOAT64, "double precision");
		sdeColumnTypeMap.put(SeColumnDefinition.TYPE_DATE, "timestamp");
		sdeColumnTypeMap.put(SeColumnDefinition.TYPE_STRING, "character varying");
		// sdeColumnTypeMap.put(SeColumnDefinition.TYPE_SHAPE, "geometry");
	}

	private static Logger log = Logger.getLogger(SDE2PostGISExporter.class);

	void addPostGISColumnDefinition(String typeName, String type, int index) {
		columnMapping.put(columnDefinitions.size(), index);
		columnDefinitions.add(typeName + " " + type);
	}

	void convertSDEColumns() throws SeException, SQLException {
		log.info("Converting SDE column types to PostgreSQL column types");

		columnMapping = new HashMap<Integer, Integer>();
		columnDefinitions = new ArrayList<String>();
		sdeColumnTypes = new ArrayList<Integer>();

		int index = 0;
		int shapeIndex = 0;
		for (SeColumnDefinition seColumnDefinition : seTable.describe()) {
			String name = "\"" + seColumnDefinition.getName() + "\"";
			int size = seColumnDefinition.getSize();
			int type = seColumnDefinition.getType();
			sdeColumnTypes.add(type);

			if (sdeColumnTypeMap.containsKey(type)) {
				String typeName = sdeColumnTypeMap.get(type);
				if (type == SeColumnDefinition.TYPE_STRING) {
					typeName += "(" + size + ")";
				}
				addPostGISColumnDefinition(name, typeName, index);
			}

			// save index for SDE shape column
			if (type == SeColumnDefinition.TYPE_SHAPE) {
				shapeIndex = index;
			}

			index++;
		}

		// save column mapping for PostGIS geometry column
		columnMapping.put(columnDefinitions.size(), shapeIndex);
		columns = columnDefinitions.size() + 1;
	}

	void createPreparedStatement() throws SQLException {
		log.info("Creating prepared statement to be used for inserts");

		String sql = "INSERT INTO " + destinationTable + " VALUES (";
		for (int i = 0; i < columnDefinitions.size(); i++) {
			sql += "?,";
		}
		// add param geom column
		sql += "?)";

		preparedStatement = connection.prepareStatement(sql);
	}

	/**
	 * Create PostGIS table from inspecting SDE-table
	 */
	void createTable() throws SeException, SQLException {
		log.info("Creating new table in schema " + destinationSchema);

		seTable = new SeTable(seConnection, sdeQualifiedName);

		String featureName = seTable.getName();

		destinationTable = destinationSchema + ".\"" + featureName + "\"";

		convertSDEColumns();

		String sql = "";
		sql += "CREATE TABLE " + destinationTable + " ";
		sql += "(";
		for (String columnDefinition : columnDefinitions) {
			sql += columnDefinition + ",";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql += ")";

		log.debug("About to commit '" + sql + "'");

		Statement statement = connection.createStatement();
		statement.executeUpdate(sql);
		statement.close();

		sql = "";

		log.info("Creating spatial column");

		statement = connection.createStatement();
		String wktType = "GEOMETRY"; // determineSpatialType(seTable);
		sql += "SELECT AddGeometryColumn('"
				+ destinationSchema
				+ "', '"
				+ featureName
				+ "', 'geom', "
				+ srid
				+ ", '"
				+ wktType
				+ "', 2)";
		log.debug("About to commit '" + sql + "'");
		statement.execute(sql);
		statement.close();
	}

	/**
	 * Determine SDE spatial type and return as WKT geometry type. If unknown return "GEOMETRY".
	 */
	String determineSpatialType(SeTable seTable) throws SeException {
		log.info("Determining spatial type");

		String shapeColumnName = null;

		for (SeColumnDefinition seColumnDefinition : seTable.describe()) {
			if (seColumnDefinition.getType() == SeColumnDefinition.TYPE_SHAPE) {
				shapeColumnName = seColumnDefinition.getName();
			}
		}

		SeLayer seLayer = new SeLayer(seConnection, seTable.getName(), shapeColumnName);

		int shapeTypes = seLayer.getShapeTypes();

		String wktType = "GEOMETRY";
		switch (shapeTypes) {
		case SeLayer.TYPE_POINT:
			wktType = "POINT";
			break;
		case SeLayer.TYPE_LINE:
			wktType = "LINESTRING";
			break;
		case SeLayer.TYPE_SIMPLE_LINE:
			wktType = "LINESTRING";
			break;
		case SeLayer.TYPE_POLYGON:
			wktType = "POLYGON";
			break;
		case SeLayer.TYPE_MULTI_POINT:
			wktType = "MULIPOINT";
			break;
		case SeLayer.TYPE_MULTI_LINE:
			wktType = "MULTILINESTRING";
			break;
		case SeLayer.TYPE_MULTI_SIMPLE_LINE:
			wktType = "MULTILINESTRING";
			break;
		case SeLayer.TYPE_MULTI_POLYGON:
			wktType = "MULTIPOLYGON";
			break;
		}

		log.info("Spatial type is " + wktType);

		return wktType;
	}

	/**
	 * Export SDE table name to PostGIS destination schema. Will always clean up connections.
	 * 
	 * @param buffer
	 *            set to something sensible, like dividing total features by 100-1000 but use lower
	 *            if individual features are complex.
	 */
	public void export(String sdeQualifiedName, String destinationSchema, int buffer, int srid)
			throws SeException, SQLException, ClassNotFoundException {
		log.info("Exporting " + sdeQualifiedName + " to schema " + destinationSchema);

		this.sdeQualifiedName = sdeQualifiedName;
		this.destinationSchema = destinationSchema;
		this.buffer = buffer;
		this.srid = srid;

		try {
			log.info("Connecting to ArcSDE");
			seConnection = ConnectionFactory.createSeConnection();

			log.info("Connecting to PostgreSQL");
			connection = ConnectionFactory.createPostGISConnection();

			createTable();

			createPreparedStatement();

			query();

		} finally {
			log.info("Finalizing and disposing resources");

			if (seConnection != null)
				seConnection.close();

			if (connection != null)
				connection.close();
		}
	}

	void parseColumnValue(SeRow row, int index) throws SQLException, SeException {
		int sdeIndex = columnMapping.get(index);

		index++;

		// TODO: handle nulls here? need to set second arg correctly..
		// if (row.getIndicator(index) == SeRow.SE_IS_NULL_VALUE)
		// preparedStatement.setNull(index, 0);

		switch (sdeColumnTypes.get(sdeIndex)) {

		case SeColumnDefinition.TYPE_INT16:
			preparedStatement.setShort(index, row.getShort(sdeIndex));
			break;
		case SeColumnDefinition.TYPE_INT32:
			preparedStatement.setInt(index, row.getInteger(sdeIndex));
			break;
		case SeColumnDefinition.TYPE_INT64:
			preparedStatement.setLong(index, row.getLong(sdeIndex));
			break;
		case SeColumnDefinition.TYPE_FLOAT32:
			preparedStatement.setFloat(index, row.getFloat(sdeIndex));
			break;
		case SeColumnDefinition.TYPE_FLOAT64:
			preparedStatement.setDouble(index, row.getDouble(sdeIndex));
			break;
		case SeColumnDefinition.TYPE_DATE:
			Calendar calendar = row.getTime(sdeIndex);
			if (calendar != null) {
				preparedStatement.setDate(index, new java.sql.Date(calendar.getTimeInMillis()));
			} else {
				preparedStatement.setDate(index, null);
			}
			break;
		case SeColumnDefinition.TYPE_STRING:
			preparedStatement.setString(index, row.getString(sdeIndex));
			break;
		case SeColumnDefinition.TYPE_SHAPE:
			SeShape shape = row.getShape(sdeIndex);
			byte[] wkb = (byte[]) shape.asWKB(shape.getWKBSize()).get(1);
			Geometry geometry = new BinaryParser().parse(wkb);
			geometry.setSrid(srid);
			PGgeometryLW geom = new PGgeometryLW(geometry);
			preparedStatement.setObject(index, geom);
			break;
		}
	}

	/**
	 * Construct query for all shapes execute and call readShapes. Will always clean up query.
	 */
	void query() throws SeException, SQLException {
		SeSqlConstruct sqlConstruct = new SeSqlConstruct(seTable.getQualifiedName());
		SeColumnDefinition[] seColumnDefinitions = seTable.describe();
		String[] columns = new String[seColumnDefinitions.length];
		int index = 0;
		for (SeColumnDefinition seColumnDefinition : seTable.describe()) {
			columns[index] = seColumnDefinition.getName();
			index++;
		}

		SeQuery seQuery = null;
		try {
			seQuery = new SeQuery(seConnection, columns, sqlConstruct);

			seQuery.prepareQuery();
			seQuery.execute();

			String destinationTable = destinationSchema + ".\"" + seTable.getName() + "\"";

			readShapes(seQuery);

		} finally {
			if (seQuery != null)
				seQuery.close();
		}
	}

	/**
	 * Read all shapes from SDE table to PostGIS as buffered insert transactions.
	 */
	void readShapes(SeQuery query) throws SeException, SQLException {
		log.info("Reading shapes");

		SeRow row = null;

		int totalCount = 0;
		int count = 0;
		while ((row = query.fetch()) != null) {
			for (int i = 0; i < columns; i++) {
				parseColumnValue(row, i);
			}

			preparedStatement.executeUpdate();

			count++;
			totalCount++;

			if (count >= buffer) {
				log.info("Commiting batch, now at shape " + totalCount);
				connection.commit();
				count = 0;
			}
		}

		if (count > 0) {
			log.info("Commiting last batch ending at shape " + totalCount);
			connection.commit();
		}
	}
}
