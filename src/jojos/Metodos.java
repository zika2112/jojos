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
			
		default:
			System.out.println("Opcion erronea intenta de nuevo");
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
		input.nextLine();

		switch (opcion) {

		case 1:
			consulta = "INSERT INTO Joestar(parte,nombre,apellido,edad,habilidad,poder) " + "VALUES (?,?,?,?,?,?)";

			PreparedStatement ps = conexion.prepareStatement(consulta);
			System.out.println("¿La información que vas a agregar esta entre la parte (1-7)?  (SI/NO)");

			String pregunta = input.nextLine().toLowerCase();
			boolean seguir = false;

			do {
				if (pregunta.equals("no")) {

					String consultaP = "INSERT INTO parte(parte,nombre) " + "VALUES (?,?)";
					PreparedStatement ps1 = conexion.prepareStatement(consultaP);

					System.out.println(
							"Indica la parte que de la cual es la informacion para poder agregar la: (numero)");
					int agregarPart = input.nextInt();
					input.nextLine();

					System.out.println("Coloca el nombre de la parte que quieres agregar");
					String agregarNombrePart = input.nextLine();

					ps1.setInt(1, agregarPart);
					ps1.setString(2, agregarNombrePart);

					ps1.executeUpdate();
					ps1.clearParameters();

					System.out.println("Ahora puedes agregar la informacion sin problemas. ");
					seguir = true;

				} else if (pregunta.equals("si")) {
					System.out.println("Vamos a agregar los datos entonces. ");
					seguir = true;
				} else {
					System.out.println("La opcion no es valida vuelve a intentar (SI/NO)");
				}

			} while (seguir != true);

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
			consulta = "INSERT INTO amigo(parte,nombre,apellido,edad,habilidad,poder) " + "VALUES (?,?,?,?,?,?)";
			PreparedStatement ps2 = conexion.prepareStatement(consulta);
			System.out.println("¿La información que vas a agregar esta entre la parte (1-7)?  (SI/NO)");

			String pregunta1 = input.nextLine().toLowerCase();
			boolean seguir1 = false;

			do {
				if (pregunta1.equals("no")) {

					String consultaP = "INSERT INTO parte(parte,nombre) " + "VALUES (?,?)";
					PreparedStatement ps1 = conexion.prepareStatement(consultaP);

					System.out.println(
							"Indica la parte que de la cual es la informacion para poder agregar la: (numero)");
					int agregarPart = input.nextInt();
					input.nextLine();

					System.out.println("Coloca el nombre de la parte que quieres agregar");
					String agregarNombrePart = input.nextLine();

					ps1.setInt(1, agregarPart);
					ps1.setString(2, agregarNombrePart);

					ps1.executeUpdate();
					ps1.clearParameters();

					System.out.println("Ahora puedes agregar la informacion sin problemas. ");
					seguir1 = true;

				} else if (pregunta1.equals("si")) {
					System.out.println("Vamos a agregar los datos entonces. ");
					seguir1 = true;
				} else {
					System.out.println("La opcion no es valida vuelve a intentar (SI/NO)");
				}

			} while (seguir1 != true);

			System.out.println(" ");
			System.out.println("Introduce el numero de la parte: ");
			int parte1 = input.nextInt();
			input.nextLine();

			System.out.println("Introduce el nombre: ");
			String nombre1 = input.nextLine();

			System.out.println("Introduce el apellido: ");
			String apellido1 = input.nextLine();

			System.out.println("Introduce el valor de la edad: ");
			int edad1 = input.nextInt();
			input.nextLine();

			System.out.println("Introduce el nombre de la habilidad ");
			String habilidad1 = input.nextLine();

			System.out.println("Introduce el poder de la habilidad: ");
			String poder1 = input.nextLine();

			ps2.setInt(1, parte1);
			ps2.setString(2, nombre1);
			ps2.setString(3, apellido1);
			ps2.setInt(4, edad1);
			ps2.setString(5, habilidad1);
			ps2.setString(6, poder1);

			ps2.executeUpdate();
			ps2.clearParameters();

			break;
		case 3:
			consulta = "INSERT INTO enemigos(parte,nombre,apellido,edad,habilidad,poder) " + "VALUES (?,?,?,?,?,?)";
			PreparedStatement ps3 = conexion.prepareStatement(consulta);
			System.out.println("¿La información que vas a agregar esta entre la parte (1-7)?  (SI/NO)");

			String pregunta2 = input.nextLine().toLowerCase();
			boolean seguir2 = false;

			do {
				if (pregunta2.equals("no")) {

					String consultaP = "INSERT INTO parte(parte,nombre) " + "VALUES (?,?)";
					PreparedStatement ps1 = conexion.prepareStatement(consultaP);

					System.out.println(
							"Indica la parte que de la cual es la informacion para poder agregar la: (numero)");
					int agregarPart = input.nextInt();
					input.nextLine();

					System.out.println("Coloca el nombre de la parte que quieres agregar");
					String agregarNombrePart = input.nextLine();

					ps1.setInt(1, agregarPart);
					ps1.setString(2, agregarNombrePart);

					ps1.executeUpdate();
					ps1.clearParameters();

					System.out.println("Ahora puedes agregar la informacion sin problemas. ");
					seguir2 = true;

				} else if (pregunta2.equals("si")) {
					System.out.println("Vamos a agregar los datos entonces. ");
					seguir2 = true;
				} else {
					System.out.println("La opcion no es valida vuelve a intentar (SI/NO)");
				}

			} while (seguir2 != true);

			System.out.println(" ");
			System.out.println("Introduce el numero de la parte: ");
			int parte2 = input.nextInt();
			input.nextLine();

			System.out.println("Introduce el nombre: ");
			String nombre2 = input.nextLine();

			System.out.println("Introduce el apellido: ");
			String apellido2 = input.nextLine();

			System.out.println("Introduce el valor de la edad: ");
			int edad2 = input.nextInt();
			input.nextLine();

			System.out.println("Introduce el nombre de la habilidad ");
			String habilidad2 = input.nextLine();

			System.out.println("Introduce el poder de la habilidad: ");
			String poder2 = input.nextLine();

			ps3.setInt(1, parte2);
			ps3.setString(2, nombre2);
			ps3.setString(3, apellido2);
			ps3.setInt(4, edad2);
			ps3.setString(5, habilidad2);
			ps3.setString(6, poder2);

			ps3.executeUpdate();
			ps3.clearParameters();


			break;
		case 4:
			consulta = "INSERT INTO parte (parte,nombre) " + "VALUES (?,?)";
			PreparedStatement ps4 = conexion.prepareStatement(consulta);
			
			System.out.println(" ");
			System.out.println("Introduce el numero de la parte: ");
			int parte3 = input.nextInt();
			input.nextLine();

			System.out.println("Introduce el nombre: ");
			String nombre3 = input.nextLine();


			ps4.setInt(1, parte3);
			ps4.setString(2, nombre3);
			
		
			ps4.executeUpdate();
			ps4.clearParameters();

			break;
			
		case 5:
			consulta = "INSERT INTO Stand(parte,nombre,habilidad) " + "VALUES (?,?,?)";
			PreparedStatement ps5 = conexion.prepareStatement(consulta);

			System.out.println("¿La información que vas a agregar esta entre la parte (1-7)?  (SI/NO)");

			String pregunta3 = input.nextLine().toLowerCase();
			boolean seguir3 = false;

			do {
				if (pregunta3.equals("no")) {

					String consultaP = "INSERT INTO parte(parte,nombre) " + "VALUES (?,?)";
					PreparedStatement ps1 = conexion.prepareStatement(consultaP);

					System.out.println(
							"Indica la parte que de la cual es la informacion para poder agregar la: (numero)");
					int agregarPart = input.nextInt();
					input.nextLine();

					System.out.println("Coloca el nombre de la parte que quieres agregar");
					String agregarNombrePart = input.nextLine();

					ps1.setInt(1, agregarPart);
					ps1.setString(2, agregarNombrePart);

					ps1.executeUpdate();
					ps1.clearParameters();

					System.out.println("Ahora puedes agregar la informacion sin problemas. ");
					seguir3 = true;

				} else if (pregunta3.equals("si")) {
					System.out.println("Vamos a agregar los datos entonces. ");
					seguir3 = true;
				} else {
					System.out.println("La opcion no es valida vuelve a intentar (SI/NO)");
				}

			} while (seguir3 != true);

			System.out.println(" ");
			System.out.println("Introduce el numero de la parte: ");
			int parte5 = input.nextInt();
			input.nextLine();

			System.out.println("Introduce el nombre: ");
			String nombre5 = input.nextLine();

			System.out.println("Introduce el nombre de la habilidad ");
			String habilidad5 = input.nextLine();

			
			ps5.setInt(1, parte5);
			ps5.setString(2, nombre5);
			ps5.setString(3, habilidad5);
			
			ps5.executeUpdate();
			ps5.clearParameters();

			break;
			
			default:
				System.out.println("Opcion erronea intenta de nuevo");
				break;
		}

	}
	
	public static void EliminarEnTabla(Connection conexion, Scanner input) throws SQLException {
		String consulta = new String();
		
		final String menuDeleteT =
				 "Para eliminar tenemos las siguientes tablas:"
				 + "\n1. Joestar"
				 + "\n2. amigo"
				 + "\n3. enemigos"
				 + "\n4. parte"
				 + "\n5. Stand";
		
		System.out.println(menuDeleteT);
		System.out.print("Elige una tabla: ");
		int opcion = input.nextInt();
		input.nextLine();
		
		switch(opcion){
		case 1:
			System.out.println("Para eliminar tenemos los siguientes datos por esta tabla "
					+ "\n1.parte. ADVERTENCIA(Si eliminas la parte eliminas todos los datos de dicha parte en esta tabla)"
					+ "\n2.nombre"
					+ "\n3.apellido"
					+ "\n4.edad"
					+ "\n5.habilidad"
					+ "\n6.poder");
			System.out.print("Opción: ");
			int opcionDelete = input.nextInt();
			input.nextLine();
			
			switch(opcionDelete) {
			
			case 1:
				consulta = "DELETE FROM Joestar where parte = ?";
				PreparedStatement ps = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el numero de la parte a eliminar: ");
				int parteDelete = input.nextInt();
				
				ps.setInt(1, parteDelete);
				
				ps.executeUpdate();
				ps.clearParameters();
				
				break;
			case 2:
				consulta = "DELETE FROM Joestar where nombre = ?";
				PreparedStatement ps2 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre a eliminar: ");
				String nombreDelete = input.nextLine();
				
				ps2.setString(1, nombreDelete);
				
				ps2.executeUpdate();
				ps2.clearParameters();
				break;
			case 3:
				consulta = "DELETE FROM Joestar where apellido = ?";
				PreparedStatement ps3 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el apellido a eliminar: ");
				String apellidoDelete = input.nextLine();
				
				ps3.setString(1, apellidoDelete);
				
				ps3.executeUpdate();
				ps3.clearParameters();
				break;
			case 4:
				consulta = "DELETE FROM Joestar where edad = ?";
				PreparedStatement ps4 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca la edad a eliminar eliminar: ");
				int edadDelete = input.nextInt();
				
				ps4.setInt(1, edadDelete);
				
				ps4.executeUpdate();
				ps4.clearParameters();
				break;
			case 5:
				consulta = "DELETE FROM Joestar where habilidad = ?";
				PreparedStatement ps5 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre de la habilidad a eliminar: ");
				String habilidadDelete = input.nextLine();
				
				ps5.setString(1, habilidadDelete);
				
				ps5.executeUpdate();
				ps5.clearParameters();
				break;
			case 6:
				consulta = "DELETE FROM Joestar where poder = ?";
				PreparedStatement ps6 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre del poder a eliminar eliminar: ");
				String poderDelete = input.nextLine();
				
				ps6.setString(1, poderDelete);
				
				ps6.executeUpdate();
				ps6.clearParameters();
				break;
			default:
				System.out.println("Opcion erronea intenta de nuevo");
				break;
			
			}
			
			break;
		case 2:
			System.out.println("Para eliminar tenemos los siguientes datos por esta tabla "
					+ "\n1.parte. ADVERTENCIA(Si eliminas la parte eliminas todos los datos de dicha parte en esta tabla)"
					+ "\n2.nombre"
					+ "\n3.apellido"
					+ "\n4.edad"
					+ "\n5.habilidad"
					+ "\n6.poder");
			System.out.print("Opción: ");
			int opcionDeleteAmigo = input.nextInt();
			input.nextLine();
			
			switch(opcionDeleteAmigo) {
			
			case 1:
				consulta = "DELETE FROM amigo where parte = ?";
				PreparedStatement ps = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el numero de la parte a eliminar: ");
				int parteDelete = input.nextInt();
				
				ps.setInt(1, parteDelete);
				
				ps.executeUpdate();
				ps.clearParameters();
				
				break;
			case 2:
				consulta = "DELETE FROM amigo where nombre = ?";
				PreparedStatement ps2 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre a eliminar: ");
				String nombreDelete = input.nextLine();
				
				ps2.setString(1, nombreDelete);
				
				ps2.executeUpdate();
				ps2.clearParameters();
				break;
			case 3:
				consulta = "DELETE FROM amigo where apellido = ?";
				PreparedStatement ps3 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el apellido a eliminar: ");
				String apellidoDelete = input.nextLine();
				
				ps3.setString(1, apellidoDelete);
				
				ps3.executeUpdate();
				ps3.clearParameters();
				break;
			case 4:
				consulta = "DELETE FROM amigo where edad = ?";
				PreparedStatement ps4 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca la edad a eliminar eliminar: ");
				int edadDelete = input.nextInt();
				
				ps4.setInt(1, edadDelete);
				
				ps4.executeUpdate();
				ps4.clearParameters();
				break;
			case 5:
				consulta = "DELETE FROM amigo where habilidad = ?";
				PreparedStatement ps5 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre de la habilidad a eliminar: ");
				String habilidadDelete = input.nextLine();
				
				ps5.setString(1, habilidadDelete);
				
				ps5.executeUpdate();
				ps5.clearParameters();
				break;
			case 6:
				consulta = "DELETE FROM amigo where poder = ?";
				PreparedStatement ps6 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre del poder a eliminar eliminar: ");
				String poderDelete = input.nextLine();
				
				ps6.setString(1, poderDelete);
				
				ps6.executeUpdate();
				ps6.clearParameters();
				break;
			default:
				System.out.println("Opcion erronea intenta de nuevo");
				break;
			
			}
			break;
		case 3:
			System.out.println("Para eliminar tenemos los siguientes datos por esta tabla "
					+ "\n1.parte. ADVERTENCIA(Si eliminas la parte eliminas todos los datos de dicha parte en esta tabla)"
					+ "\n2.nombre"
					+ "\n3.apellido"
					+ "\n4.edad"
					+ "\n5.habilidad"
					+ "\n6.poder");
			System.out.print("Opción: ");
			int opcionDeleteEnemy = input.nextInt();
			input.nextLine();
			
			switch(opcionDeleteEnemy) {
			
			case 1:
				consulta = "DELETE FROM enemigo where parte = ?";
				PreparedStatement ps = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el numero de la parte a eliminar: ");
				int parteDelete = input.nextInt();
				
				ps.setInt(1, parteDelete);
				
				ps.executeUpdate();
				ps.clearParameters();
				
				break;
			case 2:
				consulta = "DELETE FROM enemigo where nombre = ?";
				PreparedStatement ps2 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre a eliminar: ");
				String nombreDelete = input.nextLine();
				
				ps2.setString(1, nombreDelete);
				
				ps2.executeUpdate();
				ps2.clearParameters();
				break;
			case 3:
				consulta = "DELETE FROM enemigo where apellido = ?";
				PreparedStatement ps3 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el apellido a eliminar: ");
				String apellidoDelete = input.nextLine();
				
				ps3.setString(1, apellidoDelete);
				
				ps3.executeUpdate();
				ps3.clearParameters();
				break;
			case 4:
				consulta = "DELETE FROM enemigo where edad = ?";
				PreparedStatement ps4 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca la edad a eliminar eliminar: ");
				int edadDelete = input.nextInt();
				
				ps4.setInt(1, edadDelete);
				
				ps4.executeUpdate();
				ps4.clearParameters();
				break;
			case 5:
				consulta = "DELETE FROM enemigo where habilidad = ?";
				PreparedStatement ps5 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre de la habilidad a eliminar: ");
				String habilidadDelete = input.nextLine();
				
				ps5.setString(1, habilidadDelete);
				
				ps5.executeUpdate();
				ps5.clearParameters();
				break;
			case 6:
				consulta = "DELETE FROM enemigo where poder = ?";
				PreparedStatement ps6 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre del poder a eliminar eliminar: ");
				String poderDelete = input.nextLine();
				
				ps6.setString(1, poderDelete);
				
				ps6.executeUpdate();
				ps6.clearParameters();
				break;
			default:
				System.out.println("Opcion erronea intenta de nuevo");
				break;
			
			}
			
			break;
		case 4:
			System.out.println("Para eliminar tenemos los siguientes datos por esta tabla "
					+ "\n1.parte. ADVERTENCIA(Si eliminas la parte eliminas todos los datos de dicha parte en esta tabla)"
					+ "\n2.nombre");
			System.out.print("Opción: ");
			int opcionDeletePart = input.nextInt();
			input.nextLine();
			
			switch(opcionDeletePart) {
			
			case 1:
				consulta = "DELETE FROM parte where parte = ?";
				PreparedStatement ps = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el numero de la parte a eliminar: ");
				int parteDelete = input.nextInt();
				
				ps.setInt(1, parteDelete);
				
				ps.executeUpdate();
				ps.clearParameters();
				
				break;
			case 2:
				consulta = "DELETE FROM parte where nombre = ?";
				PreparedStatement ps2 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre a eliminar: ");
				String nombreDelete = input.nextLine();
				
				ps2.setString(1, nombreDelete);
				
				ps2.executeUpdate();
				ps2.clearParameters();
				break;
			
			default:
				System.out.println("Opcion erronea intenta de nuevo");
				break;
			
			}
			break;
		case 5:
			System.out.println("Para eliminar tenemos los siguientes datos por esta tabla "
					+ "\n1.parte. ADVERTENCIA(Si eliminas la parte eliminas todos los datos de dicha parte en esta tabla)"
					+ "\n2.nombre"
					+ "\n3.habilidad");
			System.out.print("Opción: ");
			int opcionDeleteStand = input.nextInt();
			input.nextLine();
			
			switch(opcionDeleteStand) {
			
			case 1:
				consulta = "DELETE FROM stand where parte = ?";
				PreparedStatement ps = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el numero de la parte a eliminar: ");
				int parteDelete = input.nextInt();
				
				ps.setInt(1, parteDelete);
				
				ps.executeUpdate();
				ps.clearParameters();
				
				break;
			case 2:
				consulta = "DELETE FROM stand where nombre = ?";
				PreparedStatement ps2 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre a eliminar: ");
				String nombreDelete = input.nextLine();
				
				ps2.setString(1, nombreDelete);
				
				ps2.executeUpdate();
				ps2.clearParameters();
				break;
			
			case 3:
				consulta = "DELETE FROM stand where habilidad = ?";
				PreparedStatement ps5 = conexion.prepareStatement(consulta);
				
				System.out.println("Coloca el nombre de la habilidad a eliminar: ");
				String habilidadDelete = input.nextLine();
				
				ps5.setString(1, habilidadDelete);
				
				ps5.executeUpdate();
				ps5.clearParameters();
				break;
			}
			break;
		default:
			System.out.println("Opcion erronea intenta de nuevo");
			break;
		}
		
			;
		
	}
}