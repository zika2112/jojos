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
	
		
	
	
	
	public static void mostrarTabla1(Connection conexion) throws SQLException {

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

	public static void mostrarTabla2(Connection conexion) throws SQLException {

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

	public static void mostrarTabla3(Connection conexion) throws SQLException {

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

	public static void mostrarTabla4(Connection conexion) throws SQLException {

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

	public static void mostrarTabla5(Connection conexion) throws SQLException {

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
	public static void mostrarVariasTablas(Connection conexion,Scanner input) throws SQLException {
		
		String consulta = new String();
		final String menuVariasT =
				 "1. Mostrar datos de 2 tablas"
				+ "\n2. Mostrar datos de de 3 tablas"
				+ "\n3. Mostrar datos de de 4 tablas"
				+ "\n4. Mostrar datos de todas las tablas";
	
		System.out.println(menuVariasT);
		System.out.print("Opción: ");
		int opcion = input.nextInt();	
		
		
		switch(opcion) {
		
		case 1:
			System.out.println("Selecciona el numero de la primera tabla");
			consulta="Select * from amigo,joestar;";
					
			break;
			
		case 2:
			break;
			
		case 3:
			break;
			
		case 4:	
			break;
		}
		

		PreparedStatement ps = conexion.prepareStatement(consulta);
		ResultSet res = ps.executeQuery();
		ResultSetMetaData rmd = res.getMetaData();

		
		while (res.next()) {

			System.out.println( res.getInt(1)+ res.getString(2)+ res.getString(3)+
					res.getString(4)+ res.getString(5)+ res.getString(6));
		}
		
	}
	
	public static void insertarEnTabla(Connection conexion, Scanner input) throws SQLException {
		String consulta = new String();
		
		final String menuInsertT =
				 "Para insertar tenermos las siguientes tablas:"
				 + "\n1. Joestar: con los valores: parte (numero),nombre,apellido,edad(numero),habilidad,poder. "
				 + "\n2. amigo: con los valores: parte (numero),nombre,apellido,edad(numero),habilidad,poder. "
				 + "\n3. enemigo: con los valores: parte (numero),nombre,apellido,edad(numero),habilidad,poder. "
				 + "\n4. parte: con los valores: parte (numero),nombre"
				 + "\n5. Stand: con los valores: parte (numero),nombre,habilidad";
		
		System.out.println(menuInsertT);
		System.out.print("Elige una tabla: ");
		int opcion = input.nextInt();
		
		switch (opcion) {
		
		case 1:
			consulta = "INSERT INTO Joestar(parte,nombre,apellido,edad,habilidad,poder) "
						+ "VALUES (?,?,?,?,?,?)";
			
			PreparedStatement ps = conexion.prepareStatement(consulta);
			
			
			String pregunta = " ";
			String continuar = new String();
			
			do {
				System.out.println("¿La informacion que vas a agregar esta entre la parte 1-6 de jojos?  (S/N)");
				pregunta = input.nextLine();
				
			if(pregunta.equals("n")) {
				
				System.out.println("Indica la parte que de la cual es la informacion para poder agregar la: ");
				int agregarPart =input.nextInt();
				input.nextLine();
				System.out.println(" ");
				
				System.out.println("Cola el nombre de la parte que quieres agregar");
				String agregarNombrePart = input.nextLine();
				
				ps.setInt(1, agregarPart);
				ps.setString(2, agregarNombrePart);
				
				ps.executeUpdate();
				ps.clearParameters();
				
				System.out.println("Ahora puedes agregar la informacion sin problemas. ");
				
			}else if (pregunta.equals("s")) {
				System.out.println("Vamos a agregar los dato entonces. ");
				
			}else {
				System.out.println("La opcion no es valida vuelve a intentar");
			}
				
			}while(pregunta!="s" && pregunta!="n");
			
			
			System.out.println(" ");
			System.out.println("Introduce el numero de la parte: ");
			int parte = input.nextInt();
			input.nextLine();
			
			System.out.println("Introduce el nombre: ");
			String nombre = input.nextLine();
			
			
			System.out.println("Introduce el apellido: ");
			String apellido = input.nextLine();
			
			
			System.out.println("Introduce el valor de la edad: ");
			int edad = input.nextInt();
			input.nextLine();
			
			System.out.println("Introduce el nombre de la habilidad ");
			String habilidad = input.nextLine();
			
			
			System.out.println("Introduce el poder de la habilidad: ");
			String poder = input.nextLine();
			
			
			ps.setInt(1, parte);
			ps.setString(2, nombre);
			ps.setString(3, apellido);
			ps.setInt(4, edad);
			ps.setString(5, habilidad);
			ps.setString(6, poder);
			
			ps.executeUpdate();
			ps.clearParameters();
			
			break;	
		case 2:
			consulta = "INSERT INTO amigo(parte,nombre,apellido,edad,habilidad,poder) "
					+ "VALUES (?,?,?,?,?,?)";
			PreparedStatement ps2 = conexion.prepareStatement(consulta);
			ResultSet res2 = ps2.executeQuery();
			
			break;
		case 3:
			consulta = "INSERT INTO enemigo(parte,nombre,apellido,edad,habilidad,poder) "
					+ "VALUES (?,?,?,?,?,?)";
			PreparedStatement ps3 = conexion.prepareStatement(consulta);
			ResultSet res3 = ps3.executeQuery();
			
			break;
		case 4:
			consulta = "INSERT INTO parte (parte,nombre) "
					+ "VALUES (?,?)";
			PreparedStatement ps4 = conexion.prepareStatement(consulta);
			ResultSet res4 = ps4.executeQuery();
			
			break;
		case 5:
			consulta = "INSERT INTO Stand(parte,nombre,habilidad) "
					+ "VALUES (?,?,?)";
			PreparedStatement ps5 = conexion.prepareStatement(consulta);
			ResultSet res5 = ps5.executeQuery();
			
			break;
		}
		
		
	}
}