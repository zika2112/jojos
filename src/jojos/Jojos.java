package jojos;

import java.awt.Menu;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class Jojos {

	public static void main(String[] args) {

		String bbdd = "jojo";
		String user = "root";
		String password = "";
		String servidor = "jdbc:mysql://localhost:3306/";
		Connection conexion = null;

		Scanner input = new Scanner(System.in);

		try {
			conexion = DriverManager.getConnection(servidor + bbdd, user, password);

			int opciones = 0;

			System.out.println("Conexión Establecida");
			do {

				Metodos.menu();
				System.out.print("Opción:");
				opciones = input.nextInt();
				System.out.println();

				if (opciones > 10) {
					System.out.println("Opción inválida");
				} else {

					switch (opciones) {

					case 1:
						Metodos.mostrartabla1(conexion);
						System.out.println();
						break;

					case 2:
						Metodos.mostrartabla2(conexion);
						System.out.println();
						break;

					case 3:
						Metodos.mostrartabla3(conexion);
						System.out.println();
						break;

					case 4:
						Metodos.mostrartabla4(conexion);
						System.out.println();
						break;

					case 5:
						Metodos.mostrartabla5(conexion);
						System.out.println();
						break;
					}
				}

			} while (opciones != 10);

			System.out.println("La conexión ha finalizado");

			conexion.close();
		} catch (SQLException sqle) {
			sqle.printStackTrace();

		}

	}
}
