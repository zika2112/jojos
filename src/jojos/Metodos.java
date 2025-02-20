package jojos;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Scanner;
import java.sql.Connection;

public class Metodos {
	
	public static void menu () {
		final String menu =
				 "1. Mostrar datos de tabla 1"
				+ "\n2. Mostrar datos de tabla 2"
				+ "\n3. Mostrar datos de tabla 3"
				+ "\n4. Mostrar datos de tabla 4"
				+ "\n5. Mostrar datos de tabla 5"
				+ "\n6. Mostrar datos de varias tablas"
				+ "\n7. Insertar de datos en una tabla"
				+ "\n8. Modificar datos de una tabla"
				+ "\n9. Eliminar datos de una tabla"
				+ "\n10.Salir";
		System.out.println(menu);
		
	}
	
	public static void mostrartabla1(Connection conexion) throws SQLException {

		String consulta = "SELECT * FROM Joestar ";

		PreparedStatement ps = conexion.prepareStatement(consulta);
		ResultSet res = ps.executeQuery();
		ResultSetMetaData rmd = res.getMetaData();

		System.out.printf("%3s%28s%39s%51s%49s%51s%n", rmd.getColumnName(1), rmd.getColumnName(2), rmd.getColumnName(3),
				rmd.getColumnName(4), rmd.getColumnName(5), rmd.getColumnName(6));
		
		for (int i = 0; i < 3 + 30 + 40 + 50 + 50 + 50; i++)
			System.out.print("=");
		System.out.println();

		while (res.next()) {

			System.out.printf("%3d%30s%40s%50s%50s%50s%n", res.getInt(1), res.getString(2), res.getString(3),
					res.getString(4), res.getString(5), res.getString(6));
		}

	}

	public static void mostrartabla2(Connection conexion) throws SQLException {

		String consulta = "SELECT * FROM amigo ";

		PreparedStatement ps = conexion.prepareStatement(consulta);
		ResultSet res = ps.executeQuery();
		ResultSetMetaData rmd = res.getMetaData();

		System.out.printf("%3s%28s%39s%51s%49s%51s%n", rmd.getColumnName(1), rmd.getColumnName(2), rmd.getColumnName(3),
				rmd.getColumnName(4), rmd.getColumnName(5), rmd.getColumnName(6));
		
		for (int i = 0; i < 3 + 30 + 40 + 50 + 50 + 50; i++)
			System.out.print("=");
		System.out.println();

		while (res.next()) {

			System.out.printf("%3d%30s%40s%50s%50s%50s%n", res.getInt(1), res.getString(2), res.getString(3),
					res.getString(4), res.getString(5), res.getString(6));
		}


	}

	public static void mostrartabla3(Connection conexion) throws SQLException {

		String consulta = "SELECT * FROM enemigos ";

		PreparedStatement ps = conexion.prepareStatement(consulta);
		ResultSet res = ps.executeQuery();
		ResultSetMetaData rmd = res.getMetaData();

		System.out.printf("%3s%28s%39s%51s%49s%51s%n", rmd.getColumnName(1), rmd.getColumnName(2), rmd.getColumnName(3),
				rmd.getColumnName(4), rmd.getColumnName(5), rmd.getColumnName(6));
		
		for (int i = 0; i < 3 + 30 + 40 + 50 + 50 + 50; i++)
			System.out.print("=");
		System.out.println();

		while (res.next()) {

			System.out.printf("%3d%30s%40s%50s%50s%50s%n", res.getInt(1), res.getString(2), res.getString(3),
					res.getString(4), res.getString(5), res.getString(6));
		}

	}

	public static void mostrartabla4(Connection conexion) throws SQLException {

		String consulta = "SELECT * FROM parte ";

		PreparedStatement ps = conexion.prepareStatement(consulta);
		ResultSet res = ps.executeQuery();
		ResultSetMetaData rmd = res.getMetaData();

		System.out.printf("%3s%28s%n", rmd.getColumnName(1), rmd.getColumnName(2));
		
		for (int i = 0; i < 3 + 30 ; i++)
			System.out.print("=");
		System.out.println();

		while (res.next()) {

			System.out.printf("%3d%30s%n", res.getInt(1), res.getString(2));
		}


	}

	public static void mostrartabla5(Connection conexion) throws SQLException {

		String consulta = "SELECT * FROM stand ";

		PreparedStatement ps = conexion.prepareStatement(consulta);
		ResultSet res = ps.executeQuery();
		ResultSetMetaData rmd = res.getMetaData();

		System.out.printf("%3s%28s%39s%n", rmd.getColumnName(1), rmd.getColumnName(2), rmd.getColumnName(3));
		
		for (int i = 0; i < 3 + 30 + 40 + 6; i++)
			System.out.print("=");
		System.out.println();

		while (res.next()) {

			System.out.printf("%3d%34s%42s%n", res.getInt(1), res.getString(2)+"  ", res.getString(3));
		}

	}
}